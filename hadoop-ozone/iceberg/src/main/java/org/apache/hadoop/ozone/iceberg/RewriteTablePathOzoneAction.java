/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.iceberg;

import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.RewriteTablePathUtil;
import org.apache.iceberg.RewriteTablePathUtil.PositionDeleteReaderWriter;
import org.apache.iceberg.RewriteTablePathUtil.RewriteResult;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StaticTableOperations;
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

/**
 * An implementation of {@link RewriteTablePath} for Apache Ozone backed Iceberg tables.
 *
 * <p>This action rewrites table's metadata and position delete file paths by replacing a source
 * prefix with a target prefix. It processes table versions, snapshots, manifests and position delete files.</p>
 *
 * <p>The rewrite can be scoped between optional start and end metadata versions,
 * and all rewritten files are staged in a temporary directory.</p>
 */
public class RewriteTablePathOzoneAction implements RewriteTablePath {

  private String sourcePrefix;
  private String targetPrefix;
  private String startVersionName;
  private String endVersionName;
  private String stagingDir;
  private int parallelism;

  private final Table table;

  public RewriteTablePathOzoneAction(Table table) {
    this.table = table;
    this.parallelism = Runtime.getRuntime().availableProcessors();
  }

  public RewriteTablePathOzoneAction(Table table, int parallelism) {
    this.table = table;
    this.parallelism = parallelism;
  }

  @Override
  public RewriteTablePath rewriteLocationPrefix(String sPrefix, String tPrefix) {
    RewriteTablePathOzoneUtils.checkNonNullNonEmpty(sPrefix, "Source prefix");
    RewriteTablePathOzoneUtils.checkNonNullNonEmpty(tPrefix, "Target prefix");
    this.sourcePrefix = sPrefix;
    this.targetPrefix = tPrefix;
    return this;
  }

  @Override
  public RewriteTablePath startVersion(String sVersion) {
    RewriteTablePathOzoneUtils.checkNonNullNonEmpty(sVersion, "Start version");
    this.startVersionName = sVersion;
    return this;
  }

  @Override
  public RewriteTablePath endVersion(String eVersion) {
    RewriteTablePathOzoneUtils.checkNonNullNonEmpty(eVersion, "End version");
    this.endVersionName = eVersion;
    return this;
  }

  @Override
  public RewriteTablePath stagingLocation(String stagingLocation) {
    RewriteTablePathOzoneUtils.checkNonNullNonEmpty(stagingLocation, "Staging location");
    this.stagingDir = stagingLocation;
    return this;
  }

  @Override
  public Result execute() {
    validateInputs();
    // TODO: should use for parallel manifest and position delete file rewriting.
    ExecutorService executorService = Executors.newFixedThreadPool(parallelism);
    try {
      return doExecute();
    } finally {
      executorService.shutdown();
      try {
        if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
          executorService.shutdownNow();
        }
      } catch (InterruptedException e) {
        executorService.shutdownNow();
        Thread.currentThread().interrupt();
      }
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
    if (sourcePrefix.equals(targetPrefix)) {
      throw new IllegalArgumentException(
          String.format(
              "Source prefix cannot be the same as target prefix (%s)", sourcePrefix));
    }

    TableMetadata tableMetadata = ((HasTableOperations) table).operations().current();
    validateAndSetEndVersion(tableMetadata);
    validateAndSetStartVersion(tableMetadata);

    if (stagingDir == null) {
      stagingDir =
              RewriteTablePathOzoneUtils.getMetadataLocation(table)
              + "copy-table-staging-"
              + UUID.randomUUID()
              + RewriteTablePathUtil.FILE_SEPARATOR;
    } else {
      stagingDir = RewriteTablePathUtil.maybeAppendFileSeparator(stagingDir);
    }
  }

  private void validateAndSetEndVersion(TableMetadata tableMetadata) {
    if (endVersionName == null) {
      Objects.requireNonNull(
          tableMetadata.metadataFileLocation(), "Metadata file location should not be null");
      this.endVersionName = tableMetadata.metadataFileLocation();
    } else {
      this.endVersionName = validateVersion(tableMetadata, endVersionName);
    }
  }

  private void validateAndSetStartVersion(TableMetadata tableMetadata) {
    if (startVersionName != null) {
      this.startVersionName = validateVersion(tableMetadata, startVersionName);
    }
  }

  private String validateVersion(TableMetadata tableMetadata, String versionFileName) {
    String versionFile = null;
    if (versionInFilePath(tableMetadata.metadataFileLocation(), versionFileName)) {
      versionFile = tableMetadata.metadataFileLocation();
    } else {
      for (MetadataLogEntry log : tableMetadata.previousFiles()) {
        if (versionInFilePath(log.file(), versionFileName)) {
          versionFile = log.file();
          break;
        }
      }
    }

    if (versionFile == null) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot find provided version file %s in metadata log.", versionFileName));
    }
    if (!RewriteTablePathOzoneUtils.fileExist(versionFile, table.io())) {
      throw new IllegalArgumentException(
          String.format("Version file %s does not exist.", versionFile));
    }
    return versionFile;
  }

  private boolean versionInFilePath(String path, String version) {
    return RewriteTablePathUtil.fileName(path).equals(version);
  }

  private String rebuildMetadata() {
    TableMetadata startMetadata = startVersionName != null
        ? new StaticTableOperations(startVersionName, table.io()).current()
        : null;
    TableMetadata endMetadata = new StaticTableOperations(endVersionName, table.io()).current();

    List<PartitionStatisticsFile> partitionStats = endMetadata.partitionStatisticsFiles();
    if (partitionStats != null && !partitionStats.isEmpty()) {
      throw new IllegalArgumentException("Partition statistics files are not supported yet.");
    }

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

    Set<Pair<String, String>> copyPlan = new HashSet<>();
    copyPlan.addAll(rewriteVersionResult.copyPlan());
    copyPlan.addAll(rewriteManifestListResult.copyPlan());
    copyPlan.addAll(rewriteManifestResult.copyPlan());

    return RewriteTablePathOzoneUtils.saveFileList(copyPlan, stagingDir, table.io());
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

  private Set<String> manifestsToRewrite(Set<Snapshot> deltaSnapshots, TableMetadata startMetadata) {
    Table endStaticTable = newStaticTable(endVersionName, table.io());

    Set<ManifestFile> allManifests = new HashSet<>();
    Set<String> manifestsToRewrite = new HashSet<>();
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

  /**
   * Aggregated result of rewriting content files (data files and delete files),
   * tracking both the copy plan and the set of files to rewrite.
   */
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

  private static class OzonePositionDeleteReaderWriter implements RewriteTablePathUtil.PositionDeleteReaderWriter {
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

  private void rewritePositionDeletes(Set<DeleteFile> toRewrite) {
    if (toRewrite.isEmpty()) {
      return;
    }

    RewriteTablePathUtil.PositionDeleteReaderWriter posDeleteReaderWriter = new OzonePositionDeleteReaderWriter();

    for (DeleteFile deleteFile : toRewrite) {
      try {
        System.out.println("Rewriting position delete file: " + deleteFile.location());
        rewritePositionDelete(
            deleteFile, table, sourcePrefix, targetPrefix, stagingDir, posDeleteReaderWriter);
      } catch (Exception e) {
        throw new RuntimeIOException("Failed to rewrite position delete file: " + deleteFile.location(), e);
      }
    }
  }

  private static void rewritePositionDelete(
      DeleteFile deleteFile,
      Table table,
      String sourcePrefixArg,
      String targetPrefixArg,
      String stagingLocationArg,
      PositionDeleteReaderWriter posDeleteReaderWriter) {
    try {
      FileIO io = table.io();
      String newPath =
          RewriteTablePathUtil.stagingPath(
              deleteFile.location(), sourcePrefixArg, stagingLocationArg);
      OutputFile outputFile = io.newOutputFile(newPath);
      PartitionSpec spec = table.specs().get(deleteFile.specId());
      RewriteTablePathUtil.rewritePositionDeleteFile(
          deleteFile,
          outputFile,
          io,
          spec,
          sourcePrefixArg,
          targetPrefixArg,
          posDeleteReaderWriter);
    } catch (IOException e) {
      throw new RuntimeIOException(e);
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
      return new HashSet<>();
    } else {
      return new HashSet<>(metadata.snapshots());
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

      if (!RewriteTablePathOzoneUtils.fileExist(versionFilePath, table.io())) {
        throw new IllegalArgumentException(String.format("Version file %s doesn't exist", versionFilePath));
      }

      TableMetadata tableMetadata = new StaticTableOperations(versionFilePath, table.io()).current();

      result.toRewrite().addAll(tableMetadata.snapshots());
      result.copyPlan().addAll(rewriteVersionFile(tableMetadata, versionFilePath));
    }

    return result;
  }

  private Set<Pair<String, String>> rewriteVersionFile(TableMetadata metadata, String versionFilePath) {
    Set<Pair<String, String>> result = new HashSet<>();
    String stagingPath = RewriteTablePathUtil.stagingPath(versionFilePath, sourcePrefix, stagingDir);
    
    System.out.println("Processing version file " + versionFilePath);
    TableMetadata newTableMetadata = RewriteTablePathUtil.replacePaths(metadata, sourcePrefix, targetPrefix);
    TableMetadataParser.overwrite(newTableMetadata, table.io().newOutputFile(stagingPath));
    
    result.add(Pair.of(stagingPath, RewriteTablePathUtil.newPath(versionFilePath, sourcePrefix, targetPrefix)));
    result.addAll(RewriteTablePathOzoneUtils.statsFileCopyPlan(
            metadata.statisticsFiles(), newTableMetadata.statisticsFiles()));

    return result;
  }

  private Table newStaticTable(String metadataFileLocation, FileIO io) {
    StaticTableOperations ops = new StaticTableOperations(metadataFileLocation, io);
    return new BaseTable(ops, metadataFileLocation);
  }
}
