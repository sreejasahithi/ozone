/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.iceberg;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteTablePath;
import org.apache.iceberg.hadoop.HadoopTables;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * CLI to rewrite Iceberg table paths. The {@code ozone-iceberg} shell script strips the
 * {@code rewrite-path} token before invoking this class (same pattern as {@code ozone debug}).
 */
@Command(
    name = "rewrite-path",
    description = "Rewrite Iceberg table paths for table migration",
    mixinStandardHelpOptions = true
)
public class RewriteTablePathCommand implements Runnable {

  @Option(
      names = {"-l", "--table-location"},
      required = true,
      description = "The latest metadata.json file path of the table"
  )
  private String tableLocation;

  @Option(
      names = {"-s", "--source-prefix"},
      required = true,
      description = "Source path prefix to replace"
  )
  private String sourcePrefix;

  @Option(
      names = {"-t", "--target-prefix"},
      required = true,
      description = "Target path prefix"
  )
  private String targetPrefix;

  @Option(
      names = {"--staging"},
      description = "Staging location where all the rewritten files will be placed "
          + "(Default is a new directory under the table's current metadata directory.)"
  )
  private String stagingLocation;

  @Option(
      names = {"--start-version"},
      description = "Start version metadata file name (optional, e.g., v1.metadata.json)"
  )
  private String startVersion;

  @Option(
      names = {"--end-version"},
      description = "End version metadata file name (optional, defaults to current)"
  )
  private String endVersion;

  @Option(
      names = {"--om-service-id"},
      description = "Ozone Manager service ID for HA setup"
  )
  private String omServiceId;

  @Override
  public void run() {
    try {
      System.out.println("Starting Iceberg table path rewrite...");
      System.out.println("Table location: " + tableLocation);
      System.out.println("Source prefix: " + sourcePrefix);
      System.out.println("Target prefix: " + targetPrefix);

      OzoneConfiguration conf = new OzoneConfiguration();
      conf.set("fs.ofs.impl", "org.apache.hadoop.fs.ozone.RootedOzoneFileSystem");

      if (omServiceId != null) {
        conf.set("ozone.om.service.ids", omServiceId);
        System.out.println("Ozone OM Service ID: " + omServiceId);
      }

      Table table = null;
      if (tableLocation != null && !tableLocation.trim().isEmpty()) {
        System.out.println("\nLoading table...");
        HadoopTables tables = new HadoopTables(conf);
        table = tables.load(tableLocation.trim());
        System.out.println("Table loaded: " + table.location());
      }

      RewriteTablePathOzoneAction action = new RewriteTablePathOzoneAction(table);

      System.out.println("\nConfiguring rewrite action...");
      RewriteTablePath rewriteAction = action.rewriteLocationPrefix(sourcePrefix, targetPrefix);

      if (stagingLocation != null && !stagingLocation.trim().isEmpty()) {
        System.out.println("Staging location: " + stagingLocation);
        rewriteAction.stagingLocation(stagingLocation);
      }

      if (startVersion != null && !startVersion.trim().isEmpty()) {
        System.out.println("Start version: " + startVersion);
        rewriteAction.startVersion(startVersion);
      }

      if (endVersion != null && !endVersion.trim().isEmpty()) {
        System.out.println("End version: " + endVersion);
        rewriteAction.endVersion(endVersion);
      }

      System.out.println("\n=== Starting rewrite operation ===");
      RewriteTablePath.Result result = rewriteAction.execute();

      System.out.println("\n=== Rewrite completed successfully ===");
      System.out.println("Latest version: " + result.latestVersion());
      System.out.println("Staging location: " + result.stagingLocation());
      System.out.println("\n Next step: Copy files from source to target using the file list");
      System.out.println("  File list location: " + result.fileListLocation());

    } catch (Exception e) {
      System.err.println("\n✗ Error: " + e.getMessage());
      if (System.getProperty("debug") != null) {
        e.printStackTrace();
      }
      System.exit(1);
    }
  }

  public static void main(String[] args) {
    int exitCode = new CommandLine(new RewriteTablePathCommand()).execute(args);
    System.exit(exitCode);
  }
}
