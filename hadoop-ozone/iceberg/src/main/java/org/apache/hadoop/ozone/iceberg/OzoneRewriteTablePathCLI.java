package org.apache.hadoop.ozone.iceberg;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteTablePath;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopTables;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import java.util.Map;

/**
 * CLI command for rewriting Iceberg table paths in Ozone.
 */
@Command(
    name = "rewrite-table-path",
    description = "Rewrite Iceberg table paths for table migration",
    mixinStandardHelpOptions = true
)
public class OzoneRewriteTablePathCLI implements Runnable {

//  @Option(
//      names = {"-w", "--warehouse"},
//      required = true,
//      description = "Iceberg warehouse location (e.g., ofs://buck1.vol1/warehouse)"
//  )
//  private String warehouse;

  @Option(
      names = {"-l", "--table-location"},
      required = true,
      description = "The latest metadata.json file path of the table"
  )
  private String tableLocation;

//  @Option(
//      names = {"-t", "--table"},
//      required = true,
//      description = "Table identifier (database.table_name)"
//  )
//  private String tableIdentifier;

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
      description = "Staging location where all the rewritten files will be placed " +
              "(Default is a new directory under the table's current metadata directory.)"
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

//  @Option(
//      names = {"-p", "--parallelism"},
//      defaultValue = "4",
//      description = "Number of parallel threads (default: ${DEFAULT-VALUE})"
//  )
//  private int parallelism;

  @Option(
      names = {"--om-service-id"},
      description = "Ozone Manager service ID for HA setup"
  )
  private String omServiceId;

  @Override
  public void run() {
    try {
      System.out.println("Starting Iceberg table path rewrite...");
//      System.out.println("Warehouse: " + warehouse);
      System.out.println("Table location: " + tableLocation);
//      System.out.println("Table: " + tableIdentifier);
      System.out.println("Source prefix: " + sourcePrefix);
      System.out.println("Target prefix: " + targetPrefix);

      // Configure Ozone
      OzoneConfiguration conf = new OzoneConfiguration();
      conf.set("fs.ofs.impl", "org.apache.hadoop.fs.ozone.RootedOzoneFileSystem");

      if (omServiceId != null) {
        conf.set("ozone.om.service.ids", omServiceId);
        System.out.println("Ozone OM Service ID: " + omServiceId);
      }

      // Create Catalog
//      HadoopCatalog catalog = new HadoopCatalog();
//      catalog.setConf(conf);
//      catalog.initialize("ozone_catalog", Map.of("warehouse", warehouse));
//
//      // Load table
//      System.out.println("\nLoading table...");
//      TableIdentifier tableId = TableIdentifier.parse(tableIdentifier);
//      Table table = catalog.loadTable(tableId);
//      System.out.println("Table loaded: " + table.location());
       //Load table (path-based works for any catalog; catalog-based for HadoopCatalog only)
      
      Table table = null;
      if (tableLocation != null && !tableLocation.trim().isEmpty()) {
        System.out.println("\nLoading table...");
        HadoopTables tables = new HadoopTables(conf);
        table = tables.load(tableLocation.trim());
        System.out.println("Table loaded: " + table.location());
      } //else {
//        if (tableIdentifier == null || tableIdentifier.trim().isEmpty()) {
//          throw new IllegalArgumentException("Either --table-location or --table is required");
//        }
//        HadoopCatalog catalog = new HadoopCatalog();
//        catalog.setConf(conf);
//        catalog.initialize("ozone_catalog", Map.of("warehouse", warehouse));
//        System.out.println("\nLoading table...");
//        TableIdentifier tableId = TableIdentifier.parse(tableIdentifier);
//        table = catalog.loadTable(tableId);
//        System.out.println("Table loaded: " + table.location());
//      }

      // Create action
      RewriteTablePathOzoneAction action = new RewriteTablePathOzoneAction(table);

      // Build the rewrite action with required parameters
      System.out.println("\nConfiguring rewrite action...");
      RewriteTablePath rewriteAction = action.rewriteLocationPrefix(sourcePrefix, targetPrefix);

      // Add optional parameters only if provided
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

      //System.out.println("Parallelism: " + parallelism);

      // Execute rewrite
      System.out.println("\n=== Starting rewrite operation ===");
      RewriteTablePath.Result result = rewriteAction.execute();

      // Print results
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
    int exitCode = new CommandLine(new OzoneRewriteTablePathCLI()).execute(args);
    System.exit(exitCode);
  }
}
