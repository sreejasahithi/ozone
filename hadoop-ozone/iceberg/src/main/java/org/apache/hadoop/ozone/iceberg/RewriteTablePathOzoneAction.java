/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.ozone.iceberg;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RewriteTablePathUtil;
import org.apache.iceberg.RewriteTablePathUtil.PositionDeleteReaderWriter;
import org.apache.iceberg.RewriteTablePathUtil.RewriteResult;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadata.MetadataLogEntry;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.actions.ImmutableRewriteTablePath;
import org.apache.iceberg.actions.RewriteTablePath;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.data.avro.PlannedDataReader;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.util.Pair;

public class RewriteTablePathOzoneAction implements RewriteTablePath {

  private static final String RESULT_LOCATION = "file-list";

  private String sourcePrefix;
  private String targetPrefix;
  private String startVersionName;
  private String endVersionName;
  private String stagingDir;
  private int parallelism = Runtime.getRuntime().availableProcessors();

  private final Table table;
  private ExecutorService executorService;

  public RewriteTablePathOzoneAction(Table table) {
    this.table = table;
  }

  public RewriteTablePathOzoneAction(Table table, int parallelism) {
    this.table = table;
    this.parallelism = parallelism;
  }

  @Override
  public RewriteTablePath rewriteLocationPrefix(String sPrefix, String tPrefix) {
    Preconditions.checkArgument(
        sPrefix != null && !sPrefix.isEmpty(), "Source prefix('%s') cannot be empty.", sPrefix);
    this.sourcePrefix = sPrefix;
    this.targetPrefix = tPrefix;
    return this;
  }

  @Override
  public RewriteTablePath startVersion(String sVersion) {
    Preconditions.checkArgument(
        sVersion != null && !sVersion.trim().isEmpty(),
        "Start version('%s') cannot be empty.",
        sVersion);
    this.startVersionName = sVersion;
    return this;
  }

  @Override
  public RewriteTablePath endVersion(String eVersion) {
    Preconditions.checkArgument(
        eVersion != null && !eVersion.trim().isEmpty(),
        "End version('%s') cannot be empty.",
        eVersion);
    this.endVersionName = eVersion;
    return this;
  }

  @Override
  public RewriteTablePath stagingLocation(String stagingLocation) {
    Preconditions.checkArgument(
        stagingLocation != null && !stagingLocation.isEmpty(),
        "Staging location('%s') cannot be empty.",
        stagingLocation);
    this.stagingDir = stagingLocation;
    return this;
  }

  @Override
  public Result execute() {
    validateInputs();
    this.executorService = Executors.newFixedThreadPool(parallelism);
    try {
      return doExecute();
    } finally {
      executorService.shutdown();
    }
  }

  private Result doExecute() {
    String resultLocation = rebuildMetadata();
    return ImmutableRewriteTablePath.Result.builder()
        .stagingLocation(stagingDir)
        .fileListLocation(resultLocation)
        .latestVersion(RewriteTablePathUtil.fileName(endVersionName))
        .build();
  }

  private void validateInputs() {
    Preconditions.checkArgument(
        sourcePrefix != null && !sourcePrefix.isEmpty(),
        "Source prefix('%s') cannot be empty.",
        sourcePrefix);
    Preconditions.checkArgument(
        targetPrefix != null && !targetPrefix.isEmpty(),
        "Target prefix('%s') cannot be empty.",
        targetPrefix);
    Preconditions.checkArgument(
        !sourcePrefix.equals(targetPrefix),
        "Source prefix cannot be the same as target prefix (%s)",
        sourcePrefix);

    validateAndSetEndVersion();
    validateAndSetStartVersion();

    if (stagingDir == null) {
      stagingDir =
          getMetadataLocation(table)
              + "copy-table-staging-"
              + UUID.randomUUID()
              + RewriteTablePathUtil.FILE_SEPARATOR;
    } else {
      stagingDir = RewriteTablePathUtil.maybeAppendFileSeparator(stagingDir);
    }
  }

  private void validateAndSetEndVersion() {
    TableMetadata tableMetadata = ((HasTableOperations) table).operations().current();

    if (endVersionName == null) {
      Preconditions.checkNotNull(
          tableMetadata.metadataFileLocation(), "Metadata file location should not be null");
      this.endVersionName = tableMetadata.metadataFileLocation();
    } else {
      this.endVersionName = validateVersion(tableMetadata, endVersionName);
    }
  }

  private void validateAndSetStartVersion() {
    TableMetadata tableMetadata = ((HasTableOperations) table).operations().current();

    if (startVersionName != null) {
      this.startVersionName = validateVersion(tableMetadata, startVersionName);
    }
  }

  private String validateVersion(TableMetadata tableMetadata, String versionFileName) {
    String versionFile = null;
    if (versionInFilePath(tableMetadata.metadataFileLocation(), versionFileName)) {
      versionFile = tableMetadata.metadataFileLocation();
    }

    for (MetadataLogEntry log : tableMetadata.previousFiles()) {
      if (versionInFilePath(log.file(), versionFileName)) {
        versionFile = log.file();
      }
    }

    Preconditions.checkArgument(
        versionFile != null,
        "Cannot find provided version file %s in metadata log.",
        versionFileName);
    Preconditions.checkArgument(
        fileExist(versionFile), "Version file %s does not exist.", versionFile);
    return versionFile;
  }

  private boolean versionInFilePath(String path, String version) {
    return RewriteTablePathUtil.fileName(path).equals(version);
  }

  private String rebuildMetadata() {
    TableMetadata startMetadata =
        startVersionName != null
            ? ((HasTableOperations) newStaticTable(startVersionName, table.io()))
            .operations()
            .current()
            : null;
    TableMetadata endMetadata =
        ((HasTableOperations) newStaticTable(endVersionName, table.io())).operations().current();

    Preconditions.checkArgument(
        endMetadata.partitionStatisticsFiles() == null
            || endMetadata.partitionStatisticsFiles().isEmpty(),
        "Partition statistics files are not supported yet.");

    RewriteResult<Snapshot> rewriteVersionResult = rewriteVersionFiles(endMetadata);
    Set<Snapshot> deltaSnapshots = deltaSnapshots(startMetadata, rewriteVersionResult.toRewrite());

    Set<String> manifestsToRewrite = manifestsToRewrite(deltaSnapshots, startMetadata);

    Set<Snapshot> validSnapshots =
        Sets.difference(snapshotSet(endMetadata), snapshotSet(startMetadata));

    RewriteResult<ManifestFile> rewriteManifestListResult =
        validSnapshots.stream()
            .map(snapshot -> rewriteManifestList(snapshot, endMetadata, manifestsToRewrite))
            .reduce(new RewriteResult<>(), RewriteResult::append);
    System.out.println("All snapshots processed (" + validSnapshots.size() + " snapshot(s)).");

    RewriteContentFileResult rewriteManifestResult =
        rewriteManifests(deltaSnapshots, endMetadata, rewriteManifestListResult.toRewrite());

    Set<DeleteFile> deleteFiles =
        rewriteManifestResult.toRewrite().stream()
            .filter(e -> e instanceof DeleteFile)
            .map(e -> (DeleteFile) e)
            .collect(Collectors.toSet());
    rewritePositionDeletes(deleteFiles);

    Set<Pair<String, String>> copyPlan = Sets.newHashSet();
    copyPlan.addAll(rewriteVersionResult.copyPlan());
    copyPlan.addAll(rewriteManifestListResult.copyPlan());
    copyPlan.addAll(rewriteManifestResult.copyPlan());

    return saveFileList(copyPlan);
  }

  private String saveFileList(Set<Pair<String, String>> filesToMove) {
    String fileListPath = stagingDir + RESULT_LOCATION;
    OutputFile fileList = table.io().newOutputFile(fileListPath);
    writeAsCsv(filesToMove, fileList);
    return fileListPath;
  }

  private void writeAsCsv(Set<Pair<String, String>> rows, OutputFile outputFile) {
    try (BufferedWriter writer =
             new BufferedWriter(
                 new OutputStreamWriter(outputFile.createOrOverwrite(), StandardCharsets.UTF_8))) {
      for (Pair<String, String> pair : rows) {
        writer.write(String.join(",", pair.first(), pair.second()));
        writer.newLine();
      }
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }
  

  private Set<Snapshot> deltaSnapshots(TableMetadata startMetadata, Set<Snapshot> allSnapshots) {
    if (startMetadata == null) {
      return allSnapshots;
    } else {
      Set<Long> startSnapshotIds =
          startMetadata.snapshots().stream().map(Snapshot::snapshotId).collect(Collectors.toSet());
      return allSnapshots.stream()
          .filter(s -> !startSnapshotIds.contains(s.snapshotId()))
          .collect(Collectors.toSet());
    }
  }

  private RewriteResult<Snapshot> rewriteVersionFiles(TableMetadata endMetadata) {
    RewriteResult<Snapshot> result = new RewriteResult<>();
    result.toRewrite().addAll(endMetadata.snapshots());
    result.copyPlan().addAll(rewriteVersionFile(endMetadata, endVersionName));

    List<MetadataLogEntry> versions = endMetadata.previousFiles();
    for (int i = versions.size() - 1; i >= 0; i--) {
      String versionFilePath = versions.get(i).file();
      if (versionFilePath.equals(startVersionName)) {
        break;
      }

      Preconditions.checkArgument(
          fileExist(versionFilePath),
          String.format("Version file %s doesn't exist", versionFilePath));
      TableMetadata tableMetadata =
          new StaticTableOperations(versionFilePath, table.io()).current();

      result.toRewrite().addAll(tableMetadata.snapshots());
      result.copyPlan().addAll(rewriteVersionFile(tableMetadata, versionFilePath));
    }

    return result;
  }

  private Set<Pair<String, String>> rewriteVersionFile(
      TableMetadata metadata, String versionFilePath) {
    Set<Pair<String, String>> result = Sets.newHashSet();
    String stagingPath =
        RewriteTablePathUtil.stagingPath(versionFilePath, sourcePrefix, stagingDir);
    System.out.println("Processing version file " + versionFilePath);
    TableMetadata newTableMetadata =
        RewriteTablePathUtil.replacePaths(metadata, sourcePrefix, targetPrefix);
    TableMetadataParser.overwrite(newTableMetadata, table.io().newOutputFile(stagingPath));
    result.add(
        Pair.of(
            stagingPath,
            RewriteTablePathUtil.newPath(versionFilePath, sourcePrefix, targetPrefix)));

    result.addAll(
        statsFileCopyPlan(metadata.statisticsFiles(), newTableMetadata.statisticsFiles()));

    return result;
  }

  private Set<Pair<String, String>> statsFileCopyPlan(
      List<StatisticsFile> beforeStats, List<StatisticsFile> afterStats) {
    Set<Pair<String, String>> result = Sets.newHashSet();
    if (beforeStats.isEmpty()) {
      return result;
    }

    Preconditions.checkArgument(
        beforeStats.size() == afterStats.size(),
        "Before and after path rewrite, statistic files count should be same");
    for (int i = 0; i < beforeStats.size(); i++) {
      StatisticsFile before = beforeStats.get(i);
      StatisticsFile after = afterStats.get(i);
      Preconditions.checkArgument(
          before.fileSizeInBytes() == after.fileSizeInBytes(),
          "Before and after path rewrite, statistic file size should be same");
      result.add(Pair.of(before.path(), after.path()));
    }
    return result;
  }

  private RewriteResult<ManifestFile> rewriteManifestList(
      Snapshot snapshot, TableMetadata tableMetadata, Set<String> manifestsToRewrite) {
    RewriteResult<ManifestFile> result = new RewriteResult<>();

    String path = snapshot.manifestListLocation();
    String outputPath = RewriteTablePathUtil.stagingPath(path, sourcePrefix, stagingDir);
    RewriteResult<ManifestFile> rewriteResult =
        RewriteTablePathUtil.rewriteManifestList(
            snapshot,
            table.io(),
            tableMetadata,
            manifestsToRewrite,
            sourcePrefix,
            targetPrefix,
            stagingDir,
            outputPath);

    result.append(rewriteResult);
    result
        .copyPlan()
        .add(Pair.of(outputPath, RewriteTablePathUtil.newPath(path, sourcePrefix, targetPrefix)));
    return result;
  }
  

  private Set<String> manifestsToRewrite(Set<Snapshot> deltaSnapshots, TableMetadata startMetadata) {
    Table endStaticTable = newStaticTable(endVersionName, table.io());

    Set<ManifestFile> allManifests = Sets.newHashSet();
    Set<String> manifestsToRewrite = Sets.newHashSet();
    for (Snapshot snapshot : endStaticTable.snapshots()) {
      allManifests.addAll(snapshot.allManifests(table.io()));
    }

    if (startMetadata == null) {
      // Need all manifest paths from all snapshots in the end table
      for (ManifestFile manifest : allManifests) {
        manifestsToRewrite.add(manifest.path());
      }
      return manifestsToRewrite;

    } else {
      Set<Long> deltaSnapshotIds = deltaSnapshots.stream()
              .map(Snapshot::snapshotId)
              .collect(Collectors.toSet());

      for (ManifestFile manifest : allManifests) {
        if (manifest.snapshotId() != null && deltaSnapshotIds.contains(manifest.snapshotId())) {
          manifestsToRewrite.add(manifest.path());
        }
      }

      return manifestsToRewrite;
    }
  }

  public static class RewriteContentFileResult extends RewriteResult<ContentFile<?>> {
    @Override
    public RewriteContentFileResult append(RewriteResult<ContentFile<?>> r1) {
      this.copyPlan().addAll(r1.copyPlan());
      this.toRewrite().addAll(r1.toRewrite());
      return this;
    }

    public RewriteContentFileResult appendDataFile(RewriteResult<DataFile> r1) {
      this.copyPlan().addAll(r1.copyPlan());
      this.toRewrite().addAll(r1.toRewrite());
      return this;
    }

    public RewriteContentFileResult appendDeleteFile(RewriteResult<DeleteFile> r1) {
      this.copyPlan().addAll(r1.copyPlan());
      this.toRewrite().addAll(r1.toRewrite());
      return this;
    }
  }
  
//  private RewriteContentFileResult rewriteManifests(
//      Set<Snapshot> deltaSnapshots, TableMetadata tableMetadata, Set<ManifestFile> toRewrite) {
//    if (toRewrite.isEmpty()) {
//      return new RewriteContentFileResult();
//    }
//
//    Set<Long> deltaSnapshotIds =
//        deltaSnapshots.stream().map(Snapshot::snapshotId).collect(Collectors.toSet());
//
//    // Process manifests in parallel (similar to Spark's repartition + map)
//    List<Future<RewriteContentFileResult>> futures = new ArrayList<>();
//    for (ManifestFile manifestFile : toRewrite) {
//      Future<RewriteContentFileResult> future =
//          executorService.submit(
//              () ->
//                  processManifest(
//                      manifestFile,
//                      table,
//                      deltaSnapshotIds,
//                      stagingDir,
//                      tableMetadata.formatVersion(),
//                      sourcePrefix,
//                      targetPrefix));
//      futures.add(future);
//    }
//
//    // Aggregate results (similar to Spark's reduce)
//    RewriteContentFileResult aggregatedResult = new RewriteContentFileResult();
//    for (Future<RewriteContentFileResult> future : futures) {
//      try {
//        RewriteContentFileResult result = future.get();
//        synchronized (aggregatedResult) {
//          aggregatedResult.append(result);
//        }
//      } catch (Exception e) {
//        throw new RuntimeIOException("Failed to rewrite manifest", e);
//      }
//    }
//
//    return aggregatedResult;
//  }
// OR  
//  private RewriteContentFileResult rewriteManifests(
//      Set<Snapshot> deltaSnapshots, TableMetadata tableMetadata, Set<ManifestFile> toRewrite) {
//
//    if (toRewrite.isEmpty()) {
//      return new RewriteContentFileResult();
//    }
//
//    Set<Long> deltaSnapshotIds =
//        deltaSnapshots.stream().map(Snapshot::snapshotId).collect(Collectors.toSet());
//
//    // Process manifests in parallel
//    List<Future<RewriteContentFileResult>> futures = new ArrayList<>();
//    for (ManifestFile manifestFile : toRewrite) {
//      futures.add(
//          executorService.submit(
//              () ->
//                  processManifest(
//                      manifestFile,
//                      table,
//                      deltaSnapshotIds,
//                      stagingDir,
//                      tableMetadata.formatVersion(),
//                      sourcePrefix,
//                      targetPrefix)));
//    }
//
//    // Aggregate results
//    RewriteContentFileResult aggregatedResult = new RewriteContentFileResult();
//    for (Future<RewriteContentFileResult> future : futures) {
//      try {
//        // future.get() blocks until the specific thread is done.
//        // Since this loop runs on the main thread, no synchronization is needed here.
//        RewriteContentFileResult result = future.get();
//        aggregatedResult.append(result);
//      } catch (Exception e) {
//        throw new RuntimeIOException("Failed to rewrite manifest", e);
//      }
//    }
//
//    return aggregatedResult;
//  }

  private RewriteContentFileResult rewriteManifests(
      Set<Snapshot> deltaSnapshots, TableMetadata tableMetadata, Set<ManifestFile> toRewrite) {
    if (toRewrite.isEmpty()) {
      return new RewriteContentFileResult();
    }

    Set<Long> deltaSnapshotIds =
        deltaSnapshots.stream().map(Snapshot::snapshotId).collect(Collectors.toSet());
    
    RewriteContentFileResult aggregatedResult = new RewriteContentFileResult();
    
    for (ManifestFile manifestFile : toRewrite) {
      System.out.println("Processing manifest " + manifestFile.path());

      RewriteContentFileResult result = processManifest(
          manifestFile,
          table,
          deltaSnapshotIds,
          stagingDir,
          tableMetadata.formatVersion(),
          sourcePrefix,
          targetPrefix);

      aggregatedResult.append(result);
    }

    return aggregatedResult;
  }

  private static RewriteContentFileResult processManifest(
      ManifestFile manifestFile,
      Table table,
      Set<Long> deltaSnapshotIds,
      String stagingLocation,
      int format,
      String sourcePrefix,
      String targetPrefix) {
    RewriteContentFileResult result = new RewriteContentFileResult();
    switch (manifestFile.content()) {
    case DATA:
      result.appendDataFile(
          writeDataManifest(
              manifestFile,
              table,
              deltaSnapshotIds,
              stagingLocation,
              format,
              sourcePrefix,
              targetPrefix));
      break;
    case DELETES:
      result.appendDeleteFile(
          writeDeleteManifest(
              manifestFile,
              table,
              deltaSnapshotIds,
              stagingLocation,
              format,
              sourcePrefix,
              targetPrefix));
      break;
    default:
      throw new UnsupportedOperationException(
          "Unsupported manifest type: " + manifestFile.content());
    }
    return result;
  }

  private static RewriteResult<DataFile> writeDataManifest(
      ManifestFile manifestFile,
      Table table,
      Set<Long> snapshotIds,
      String stagingLocation,
      int format,
      String sourcePrefix,
      String targetPrefix) {
    try {
      String stagingPath =
          RewriteTablePathUtil.stagingPath(manifestFile.path(), sourcePrefix, stagingLocation);
      FileIO io = table.io();
      OutputFile outputFile = io.newOutputFile(stagingPath);
      Map<Integer, PartitionSpec> specsById = table.specs();
      return RewriteTablePathUtil.rewriteDataManifest(
          manifestFile,
          snapshotIds,
          outputFile,
          io,
          format,
          specsById,
          sourcePrefix,
          targetPrefix);
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }

  private static RewriteResult<DeleteFile> writeDeleteManifest(
      ManifestFile manifestFile,
      Table table,
      Set<Long> snapshotIds,
      String stagingLocation,
      int format,
      String sourcePrefix,
      String targetPrefix) {
    try {
      String stagingPath =
          RewriteTablePathUtil.stagingPath(manifestFile.path(), sourcePrefix, stagingLocation);
      FileIO io = table.io();
      OutputFile outputFile = io.newOutputFile(stagingPath);
      Map<Integer, PartitionSpec> specsById = table.specs();
      return RewriteTablePathUtil.rewriteDeleteManifest(
          manifestFile,
          snapshotIds,
          outputFile,
          io,
          format,
          specsById,
          sourcePrefix,
          targetPrefix,
          stagingLocation);
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }
  
//  private void rewritePositionDeletes(Set<DeleteFile> toRewrite) {
//    if (toRewrite.isEmpty()) {
//      return;
//    }
//
//    PositionDeleteReaderWriter posDeleteReaderWriter = new OzonePositionDeleteReaderWriter();
//
//    List<Future<?>> futures = new ArrayList<>();
//    for (DeleteFile deleteFile : toRewrite) {
//      Future<?> future =
//          executorService.submit(
//              () -> {
//                try {
//                  rewritePositionDelete(
//                      deleteFile, table, sourcePrefix, targetPrefix, stagingDir, posDeleteReaderWriter);
//                } catch (Exception e) {
//                  throw new RuntimeIOException("Failed to rewrite position delete file: " + deleteFile.location(), e);
//                }
//              });
//      futures.add(future);
//    }
//
//    // Wait for all delete file rewrites to complete
//    for (Future<?> future : futures) {
//      try {
//        future.get();
//      } catch (Exception e) {
//        throw new RuntimeIOException("Failed to rewrite position delete files", e);
//      }
//    }
//  }

  private void rewritePositionDeletes(Set<DeleteFile> toRewrite) {
    if (toRewrite.isEmpty()) {
      return;
    }

    PositionDeleteReaderWriter posDeleteReaderWriter = new OzonePositionDeleteReaderWriter();
    
    for (DeleteFile deleteFile : toRewrite) {
      try {
        rewritePositionDelete(
                deleteFile, table, sourcePrefix, targetPrefix, stagingDir, posDeleteReaderWriter);
      } catch (Exception e) {
        throw new RuntimeIOException("Failed to rewrite position delete file: " + deleteFile.location(), e);
      }
    }
  }

  private static class OzonePositionDeleteReaderWriter implements PositionDeleteReaderWriter {
    @Override
    public CloseableIterable<Record> reader(
        InputFile inputFile, FileFormat format, PartitionSpec spec) {
      return positionDeletesReader(inputFile, format, spec);
    }

    @Override
    public PositionDeleteWriter<Record> writer(
        OutputFile outputFile,
        FileFormat format,
        PartitionSpec spec,
        StructLike partition,
        Schema rowSchema)
        throws IOException {
      return positionDeletesWriter(outputFile, format, spec, partition, rowSchema);
    }
  }

//  private static void rewritePositionDelete(
//      DeleteFile deleteFile,
//      Table table,
//      String sourcePrefixArg,
//      String targetPrefixArg,
//      String stagingLocationArg,
//      PositionDeleteReaderWriter posDeleteReaderWriter) {
//    try {
//      FileIO io = table.io();
//      String newPath =
//          RewriteTablePathUtil.stagingPath(
//              deleteFile.location(), sourcePrefixArg, stagingLocationArg);
//      OutputFile outputFile = io.newOutputFile(newPath);
//      PartitionSpec spec = table.specs().get(deleteFile.specId());
//      RewriteTablePathUtil.rewritePositionDeleteFile(
//          deleteFile,
//          outputFile,
//          io,
//          spec,
//          sourcePrefixArg,
//          targetPrefixArg,
//          posDeleteReaderWriter);
//    } catch (IOException e) {
//      throw new RuntimeIOException(e);
//    }
//  }
private static void rewritePositionDelete(
        DeleteFile deleteFile,
        Table table,
        String sourcePrefixArg,
        String targetPrefixArg,
        String stagingLocationArg,
        PositionDeleteReaderWriter posDeleteReaderWriter) {
    
  System.out.println("REWRITING POSITION DELETE FILE");
  System.out.println("Source: " + deleteFile.location());
  System.out.println("Format: " + deleteFile.format());
  System.out.println("Content: " + deleteFile.content());
  System.out.println("Size: " + deleteFile.fileSizeInBytes() + " bytes");
  System.out.println("Spec ID: " + deleteFile.specId());
  System.out.println("Partition: " + deleteFile.partition());

  try {
    FileIO io = table.io();

    // STEP 1: Verify source exists
    System.out.println("\n[STEP 1] Checking source file...");
    InputFile inputFile = io.newInputFile(deleteFile.location());
    System.out.println("  Checking existence...");
    boolean exists = inputFile.exists();
    System.out.println("  Exists: " + exists);

    if (!exists) {
      throw new RuntimeIOException("Source file does not exist");
    }

    System.out.println("  Getting file size...");
    long size = inputFile.getLength();
    System.out.println("  Size: " + size + " bytes");

    // STEP 2: Prepare output
    System.out.println("\n[STEP 2] Preparing output file...");
    String newPath = RewriteTablePathUtil.stagingPath(
            deleteFile.location(), sourcePrefixArg, stagingLocationArg);
    System.out.println("  Target: " + newPath);

    OutputFile outputFile = io.newOutputFile(newPath);
    System.out.println("  Output file created");

    // STEP 3: Get spec and schema
    System.out.println("\n[STEP 3] Getting spec and schema...");
    PartitionSpec spec = table.specs().get(deleteFile.specId());
    if (spec == null) {
      throw new RuntimeIOException("Cannot find spec with ID: " + deleteFile.specId());
    }
//    Preconditions.checkState(spec != null,
//        "Table metadata is corrupted: specId %s referenced by file %s is missing",
//        deleteFile.specId(), deleteFile.location());
    System.out.println("  Spec ID: " + spec.specId());
    System.out.println("  Table schema: " + spec.schema());
    System.out.println("  Schema fields: " + spec.schema().columns().size());

    // STEP 4: Test if file is readable
    System.out.println("\n[STEP 4] Testing file read...");
    try {
      CloseableIterable<Record> testReader =
              posDeleteReaderWriter.reader(inputFile, deleteFile.format(), spec);
      System.out.println("  Reader created successfully");

      try {
        for (Record record : testReader) {
          System.out.println("  First record read successfully");
          System.out.println("    file_path: " + record.get(0));
          System.out.println("    pos: " + record.get(1));
          System.out.println("    row: " + (record.get(2) != null ? "present" : "null"));
          break; // Just read first record
        }
      } finally {
        testReader.close();
      }
    } catch (Exception e) {
      System.err.println("  ✗ FAILED TO READ FILE");
      System.err.println("  Exception: " + e.getClass().getName());
      System.err.println("  Message: " + e.getMessage());
      e.printStackTrace();
      throw e;
    }

    // STEP 5: Call the actual rewrite
    System.out.println("\n[STEP 5] Calling RewriteTablePathUtil.rewritePositionDeleteFile...");
    RewriteTablePathUtil.rewritePositionDeleteFile(
            deleteFile,
            outputFile,
            io,
            spec,
            sourcePrefixArg,
            targetPrefixArg,
            posDeleteReaderWriter);


    System.out.println("✓ SUCCESS - Position delete file rewritten");


  } catch (IOException e) {

    System.err.println("✗ IOException CAUGHT");

    System.err.println("Exception type: " + e.getClass().getName());
    System.err.println("Message: " + e.getMessage());
    System.err.println("\nFull stack trace:");
    e.printStackTrace(System.err);

    if (e.getCause() != null) {
      System.err.println("\nCaused by: " + e.getCause().getClass().getName());
      System.err.println("Cause message: " + e.getCause().getMessage());
      e.getCause().printStackTrace(System.err);
    }


    throw new RuntimeIOException("Failed to rewrite position delete file: " + deleteFile.location(), e);

  } catch (Exception e) {

    System.err.println("✗ UNEXPECTED EXCEPTION CAUGHT");

    System.err.println("Exception type: " + e.getClass().getName());
    System.err.println("Message: " + e.getMessage());
    System.err.println("\nFull stack trace:");
    e.printStackTrace(System.err);

    if (e.getCause() != null) {
      System.err.println("\nCaused by: " + e.getCause().getClass().getName());
      System.err.println("Cause message: " + e.getCause().getMessage());
      e.getCause().printStackTrace(System.err);
    }
    
    throw new RuntimeIOException("Failed to rewrite position delete file: " + deleteFile.location(), e);
  }
}

  private static CloseableIterable<Record> positionDeletesReader(
      InputFile inputFile, FileFormat format, PartitionSpec spec) {
    Schema deleteSchema = DeleteSchemaUtil.posDeleteReadSchema(spec.schema());
    switch (format) {
    case AVRO:
      return Avro.read(inputFile)
          .project(deleteSchema)
          .reuseContainers()
          .createReaderFunc(DataReader::create)
          .build();

    case PARQUET:
      return Parquet.read(inputFile)
          .project(deleteSchema)
          .reuseContainers()
          .createReaderFunc(
              fileSchema -> GenericParquetReaders.buildReader(deleteSchema, fileSchema))
          .build();

    case ORC:
      return ORC.read(inputFile)
          .project(deleteSchema)
          .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(deleteSchema, fileSchema))
          .build();

    default:
      throw new UnsupportedOperationException("Unsupported file format: " + format);
    }
  }

  private static PositionDeleteWriter<Record> positionDeletesWriter(
      OutputFile outputFile,
      FileFormat format,
      PartitionSpec spec,
      StructLike partition,
      Schema rowSchema)
      throws IOException {
    switch (format) {
    case AVRO:
      return Avro.writeDeletes(outputFile)
          .createWriterFunc(DataWriter::create)
          .withPartition(partition)
          .rowSchema(rowSchema)
          .withSpec(spec)
          .buildPositionWriter();
    case PARQUET:
      return Parquet.writeDeletes(outputFile)
          .createWriterFunc(GenericParquetWriter::create)
          .withPartition(partition)
          .rowSchema(rowSchema)
          .withSpec(spec)
          .buildPositionWriter();
    case ORC:
      return ORC.writeDeletes(outputFile)
          .createWriterFunc(GenericOrcWriter::buildWriter)
          .withPartition(partition)
          .rowSchema(rowSchema)
          .withSpec(spec)
          .buildPositionWriter();
    default:
      throw new UnsupportedOperationException("Unsupported file format: " + format);
    }
  }

  private Set<Snapshot> snapshotSet(TableMetadata metadata) {
    if (metadata == null) {
      return Sets.newHashSet();
    } else {
      return Sets.newHashSet(metadata.snapshots());
    }
  }

  private boolean fileExist(String path) {
    if (path == null || path.trim().isEmpty()) {
      return false;
    }
    return table.io().newInputFile(path).exists();
  }

  private String getMetadataLocation(Table tbl) {
    String currentMetadataPath =
        ((HasTableOperations) tbl).operations().current().metadataFileLocation();
    int lastIndex = currentMetadataPath.lastIndexOf(RewriteTablePathUtil.FILE_SEPARATOR);
    String metadataDir = "";
    if (lastIndex != -1) {
      metadataDir = currentMetadataPath.substring(0, lastIndex + 1);
    }

    Preconditions.checkArgument(
        !metadataDir.isEmpty(), "Failed to get the metadata file root directory");
    return metadataDir;
  }

  private Table newStaticTable(String metadataFileLocation, FileIO io) {
    StaticTableOperations ops = new StaticTableOperations(metadataFileLocation, io);
    return new BaseTable(ops, metadataFileLocation);
  }

  @VisibleForTesting
  void setExecutorService(ExecutorService executorService) {
    this.executorService = executorService;
  }

  @VisibleForTesting
  ExecutorService getExecutorService() {
    return executorService;
  }
}

