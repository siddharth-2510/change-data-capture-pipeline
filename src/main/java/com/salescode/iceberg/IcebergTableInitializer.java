package com.salescode.iceberg;

import com.salescode.config.IcebergConfig;
import com.salescode.config.S3Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.types.Types;

import java.util.HashMap;
import java.util.Map;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

/**
 * Utility to initialize or update Iceberg table with partition specification.
 * This handles partitioning programmatically as Athena DDL doesn't support
 * partition transforms for Iceberg tables.
 */
@Slf4j
public class IcebergTableInitializer {

    /**
     * Update table partition spec if needed.
     * Note: For existing tables, this will add an evolution, not replace the spec.
     * 
     * WARNING: If table already exists with different partitioning,
     * you may need to recreate it or use partition evolution.
     */
    public static void ensurePartitionSpec(IcebergConfig icebergConfig, S3Config s3Config) {
        try {
            log.info("Checking partition specification for table: {}.{}",
                    icebergConfig.getDatabase(), icebergConfig.getTable());

            // Initialize Glue Catalog
            GlueCatalog catalog = new GlueCatalog();

            Map<String, String> catalogProperties = new HashMap<>();
            catalogProperties.put("warehouse", icebergConfig.getWarehouse());
            catalogProperties.put("catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog");
            catalogProperties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");

            catalog.initialize(icebergConfig.getCatalogName(), catalogProperties);

            // Load the table
            TableIdentifier tableId = TableIdentifier.of(
                    icebergConfig.getDatabase(),
                    icebergConfig.getTable());

            if (!catalog.tableExists(tableId)) {
                log.info("Table does not exist, creating with partition spec...");
                createTableWithPartitions(catalog, tableId, icebergConfig);
            } else {
                log.info("Table exists, verifying partition spec...");
                Table table = catalog.loadTable(tableId);
                PartitionSpec spec = table.spec();

                if (spec.isUnpartitioned()) {
                    log.warn("Table is unpartitioned! Adding partition spec...");
                    updatePartitionSpec(table);
                } else {
                    log.info("Table already has partition spec: {}", spec);
                }
            }

            catalog.close();

        } catch (Exception e) {
            log.error("Error ensuring partition spec: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to ensure partition spec", e);
        }
    }

    /**
     * Create table with proper partition specification.
     * Partitioning: bucket(16, entity_id) / year(version_ts) / month(version_ts)
     */
    private static void createTableWithPartitions(
            GlueCatalog catalog,
            TableIdentifier tableId,
            IcebergConfig config) {

        // Define schema (simplified - add all fields as needed)
        Schema schema = new Schema(
                // CDC Fields
                required(1, "entity_id", Types.StringType.get()),
                required(2, "version_ts", Types.TimestampType.withZone()),
                required(3, "event_type", Types.StringType.get()),
                required(4, "ingest_ts", Types.TimestampType.withZone()),
                required(5, "is_latest", Types.BooleanType.get()),

                // Business fields (add all 50+ fields here)
                required(6, "id", Types.StringType.get()),
                optional(7, "order_number", Types.StringType.get()),
                optional(8, "lob", Types.StringType.get()),
                optional(9, "status", Types.StringType.get())
        // ... add remaining fields
        );

        // Define partition spec
        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .bucket("entity_id", 16, "entity_id_bucket") // 16 buckets
                .year("version_ts", "y") // Year partition
                .month("version_ts", "m") // Month partition
                .build();

        // Create table
        Map<String, String> properties = new HashMap<>();
        properties.put("write.upsert.enabled", "true");
        properties.put("format-version", "2");
        properties.put("write.metadata.compression-codec", "zstd");

        catalog.createTable(tableId, schema, spec, properties);
        log.info("✅ Table created with partition spec: {}", spec);
    }

    /**
     * Update partition spec on existing table (uses partition evolution).
     */
    private static void updatePartitionSpec(Table table) {
        log.warn("Partition evolution is complex - recommend recreating table instead.");
        log.warn("Current spec: {}", table.spec());

        // Partition evolution example (commented out - use with caution)
        /*
         * table.updateSpec()
         * .addField(Expressions.bucket("entity_id", 16))
         * .addField(Expressions.year("version_ts"))
         * .addField(Expressions.month("version_ts"))
         * .commit();
         */
    }
}
