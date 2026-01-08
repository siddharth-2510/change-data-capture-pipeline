package com.salescode.iceberg;

import com.salescode.config.IcebergConfig;
import com.salescode.config.S3Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.aws.glue.GlueCatalog;

import java.util.HashMap;
import java.util.Map;

/**
 * Manages Iceberg table partition evolution.
 * Adds partition specification to existing unpartitioned tables.
 */
@Slf4j
public class PartitionManager {

    /**
     * Adds partition specification to the table if it's currently unpartitioned.
     * Partition strategy: bucket(16, entity_id) / year(version_ts) /
     * month(version_ts)
     * 
     * @param icebergConfig Iceberg configuration
     * @param s3Config      S3 configuration
     */
    public static void addPartitionSpecIfNeeded(IcebergConfig icebergConfig, S3Config s3Config) {
        GlueCatalog catalog = null;
        try {
            log.info("Checking partition spec for table: {}.{}",
                    icebergConfig.getDatabase(), icebergConfig.getTable());

            catalog = new GlueCatalog();

            Map<String, String> catalogProperties = new HashMap<>();
            catalogProperties.put("warehouse", icebergConfig.getWarehouse());
            catalogProperties.put("catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog");
            catalogProperties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");

            catalogProperties.put("s3.endpoint", s3Config.getEndpoint());
            catalogProperties.put("s3.access-key-id", s3Config.getAccessKey());
            catalogProperties.put("s3.secret-access-key", s3Config.getSecretKey());
            if (s3Config.getSessionToken() != null && !s3Config.getSessionToken().isEmpty()) {
                catalogProperties.put("s3.session-token", s3Config.getSessionToken());
            }

            catalog.initialize(icebergConfig.getCatalogName(), catalogProperties);

            // Load table
            TableIdentifier tableId = TableIdentifier.of(
                    icebergConfig.getDatabase(),
                    icebergConfig.getTable());

            if (!catalog.tableExists(tableId)) {
                log.error("Table does not exist: {}", tableId);
                return;
            }

            Table table = catalog.loadTable(tableId);
            PartitionSpec currentSpec = table.spec();

            if (currentSpec.isUnpartitioned()) {
                log.info("Table is unpartitioned. Adding partition spec...");

                // Add partition spec using partition evolution with proper Expressions
                table.updateSpec()
                        .addField(org.apache.iceberg.expressions.Expressions.bucket("entity_id", 16))
                        .addField(org.apache.iceberg.expressions.Expressions.year("version_ts"))
                        .addField(org.apache.iceberg.expressions.Expressions.month("version_ts"))
                        .commit();

                log.info("✅ Partition spec added successfully!");
                log.info("New partition spec: {}", table.spec());
            } else {
                log.info("Table already has partition spec: {}", currentSpec);
            }

        } catch (Exception e) {
            log.error("Error managing partition spec: {}", e.getMessage(), e);
            log.warn("Continuing without partition evolution - data will still be written");
        } finally {
            if (catalog != null) {
                try {
                    catalog.close();
                } catch (Exception e) {
                    log.warn("Error closing catalog: {}", e.getMessage());
                }
            }
        }
    }
}
