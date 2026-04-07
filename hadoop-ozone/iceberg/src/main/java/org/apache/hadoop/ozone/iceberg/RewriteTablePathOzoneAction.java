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

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.HashSet;
import java.util.Set;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.RewriteTablePathUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadata.MetadataLogEntry;
import org.apache.iceberg.actions.ImmutableRewriteTablePath;
import org.apache.iceberg.actions.RewriteTablePath;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.iceberg.RewriteTablePathUtil.RewriteResult;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
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
    checkNonNullNonEmpty(sPrefix, "Source prefix('%s') cannot be empty.");
    checkNonNullNonEmpty(tPrefix, "Target prefix('%s') cannot be empty.");
    this.sourcePrefix = sPrefix;
    this.targetPrefix = tPrefix;
    return this;
  }

  @Override
  public RewriteTablePath startVersion(String sVersion) {
    checkNonNullNonEmpty(sVersion, "Start version('%s') cannot be empty.");
    this.startVersionName = sVersion;
    return this;
  }

  @Override
  public RewriteTablePath endVersion(String eVersion) {
    checkNonNullNonEmpty(eVersion, "End version('%s') cannot be empty.");
    this.endVersionName = eVersion;
    return this;
  }

  @Override
  public RewriteTablePath stagingLocation(String stagingLocation) {
    checkNonNullNonEmpty(stagingLocation, "Staging location('%s') cannot be empty.");
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
    if (sourcePrefix.equals(targetPrefix)) {
      throw new IllegalArgumentException(
          String.format(
              "Source prefix cannot be the same as target prefix (%s)", sourcePrefix));
    }

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
      Objects.requireNonNull(
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

    if (versionFile == null) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot find provided version file %s in metadata log.", versionFileName));
    }
    if (!fileExist(versionFile)) {
      throw new IllegalArgumentException(
          String.format("Version file %s does not exist.", versionFile));
    }
    return versionFile;
  }

  private boolean versionInFilePath(String path, String version) {
    return RewriteTablePathUtil.fileName(path).equals(version);
  }

  private String rebuildMetadata() {
    //TODO need to implement rewrite of manifest list , manifest files and position delete files.
    TableMetadata startMetadata =
        startVersionName != null
            ? ((HasTableOperations) newStaticTable(startVersionName, table.io()))
            .operations()
            .current()
            : null;
    TableMetadata endMetadata =
        ((HasTableOperations) newStaticTable(endVersionName, table.io())).operations().current();

    if (endMetadata.partitionStatisticsFiles() != null
        && !endMetadata.partitionStatisticsFiles().isEmpty()) {
      throw new IllegalArgumentException("Partition statistics files are not supported yet.");
    }

    RewriteResult<Snapshot> rewriteVersionResult = rewriteVersionFiles(endMetadata);

    Set<Pair<String, String>> copyPlan = new HashSet<>();
    copyPlan.addAll(rewriteVersionResult.copyPlan());

    return saveFileList(copyPlan);
    //return null;
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

      if (!fileExist(versionFilePath)){
        throw new IllegalArgumentException(String.format("Version file %s doesn't exist", versionFilePath));
      }

      TableMetadata tableMetadata =
          new StaticTableOperations(versionFilePath, table.io()).current();

      result.toRewrite().addAll(tableMetadata.snapshots());
      result.copyPlan().addAll(rewriteVersionFile(tableMetadata, versionFilePath));
    }

    return result;
  }

  private Set<Pair<String, String>> rewriteVersionFile(
      TableMetadata metadata, String versionFilePath) {
    Set<Pair<String, String>> result = new HashSet<>();
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
    Set<Pair<String, String>> result = new HashSet<>();
    if (beforeStats.isEmpty()) {
      return result;
    }

    if (beforeStats.size() != afterStats.size()){
      throw new IllegalArgumentException("Before and after path rewrite, statistic files count should be same");
    }

    for (int i = 0; i < beforeStats.size(); i++) {
      StatisticsFile before = beforeStats.get(i);
      StatisticsFile after = afterStats.get(i);
      if (before.fileSizeInBytes() != after.fileSizeInBytes()){
        throw new IllegalArgumentException("Before and after path rewrite, statistic files count should be same");
      }
      result.add(Pair.of(before.path(), after.path()));
    }
    return result;
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

    if (metadataDir.isEmpty()) {
      throw new IllegalArgumentException("Failed to get the metadata file root directory");
    }
    return metadataDir;
  }

  private static void checkNonNullNonEmpty(String value, String messageFormat) {
    String message = String.format(messageFormat, value);
    Objects.requireNonNull(value, message);
    if (value.trim().isEmpty()) {
      throw new IllegalArgumentException(message);
    }
  }

  private Table newStaticTable(String metadataFileLocation, FileIO io) {
    StaticTableOperations ops = new StaticTableOperations(metadataFileLocation, io);
    return new BaseTable(ops, metadataFileLocation);
  }
}
