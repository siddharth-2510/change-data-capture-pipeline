package com.salescode.iceberg;

import com.salescode.config.AppConfig;
import com.salescode.config.ConfigLoader;
import com.salescode.config.IcebergConfig;
import com.salescode.config.S3Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;

import java.util.HashMap;
import java.util.Map;

/**
 * Creates Iceberg tables with CDC versioning support.
 * 
 * Features:
 * - Versioning fields (entity_id, version_ts, event_type, ingest_ts, is_latest)
 * - Partition strategy: bucket(16, entity_id) + year(version_ts) +
 * month(version_ts)
 * - Iceberg v2 table format with ZSTD compression
 */
@Slf4j
public class CreateIcebergTables {

    // ============================================================
    // Iceberg v2 Table Properties
    // ============================================================
    private static final Map<String, String> TABLE_PROPERTIES = new HashMap<>() {
        {
            put("format-version", "2");
            put("write.format.default", "parquet");
            put("write.parquet.compression-codec", "zstd");
            put("write.metadata.delete-after-commit.enabled", "true");
            put("write.metadata.previous-versions-max", "10");
            put("write.target-file-size-bytes", "134217728"); // 128 MB
        }
    };

    public static void main(String[] args) {

        // Load config from application.yaml
        AppConfig appConfig = ConfigLoader.loadConfig("application.yaml");
        IcebergConfig icebergConfig = appConfig.getIceberg();
        S3Config s3Config = appConfig.getS3();

        log.info("Using warehouse: {}", icebergConfig.getWarehouse());
        log.info("Using S3 endpoint: {}", s3Config.getEndpoint());

        Configuration conf = IcebergUtil.hadoopConf(s3Config);

        Catalog catalog = new HadoopCatalog(conf, icebergConfig.getWarehouse());

        createOrdersTable(catalog);
        createOrderDetailsTable(catalog);

        log.info("✔ All Iceberg tables are ready.");
    }

    // ------------------------------------------------------------
    // CREATE db.orders TABLE (with CDC versioning)
    // ------------------------------------------------------------
    public static void createOrdersTable(Catalog catalog) {

        TableIdentifier tableId = TableIdentifier.of("db", "orders");

        if (catalog.tableExists(tableId)) {
            log.info("✔ Table db.orders already exists. Skipping.");
            return;
        }

        Schema schema = new Schema(
                // ============================================================
                // CDC Versioning Fields (required for tracking multiple versions)
                // ============================================================
                Types.NestedField.required(1, "entity_id", Types.StringType.get()),
                Types.NestedField.required(2, "version_ts", Types.TimestampType.withZone()),
                Types.NestedField.required(3, "event_type", Types.StringType.get()),
                Types.NestedField.required(4, "ingest_ts", Types.TimestampType.withZone()),
                Types.NestedField.required(5, "is_latest", Types.BooleanType.get()),

                // ============================================================
                // Original Business Fields (preserved from existing schema)
                // ============================================================
                Types.NestedField.required(6, "id", Types.StringType.get()),
                Types.NestedField.optional(7, "active_status", Types.StringType.get()),
                Types.NestedField.optional(8, "active_status_reason", Types.StringType.get()),
                Types.NestedField.optional(9, "created_by", Types.StringType.get()),
                Types.NestedField.optional(10, "creation_time", Types.TimestampType.withZone()),
                Types.NestedField.optional(11, "extended_attributes", Types.StringType.get()),
                Types.NestedField.optional(12, "hash", Types.StringType.get()),
                Types.NestedField.optional(13, "last_modified_time", Types.TimestampType.withZone()),
                Types.NestedField.optional(14, "lob", Types.StringType.get()),
                Types.NestedField.optional(15, "modified_by", Types.StringType.get()),
                Types.NestedField.optional(16, "source", Types.StringType.get()),
                Types.NestedField.optional(17, "version", Types.IntegerType.get()),
                Types.NestedField.optional(18, "system_time", Types.TimestampType.withZone()),
                Types.NestedField.optional(19, "bill_amount", Types.DoubleType.get()),
                Types.NestedField.optional(20, "channel", Types.StringType.get()),
                Types.NestedField.optional(21, "discount_info", Types.StringType.get()),
                Types.NestedField.optional(22, "gps_latitude", Types.StringType.get()),
                Types.NestedField.optional(23, "gps_longitude", Types.StringType.get()),
                Types.NestedField.optional(24, "user_hierarchy", Types.StringType.get()),
                Types.NestedField.optional(25, "line_count", Types.IntegerType.get()),
                Types.NestedField.optional(26, "loginid", Types.StringType.get()),
                Types.NestedField.optional(27, "net_amount", Types.DoubleType.get()),
                Types.NestedField.optional(28, "normalized_quantity", Types.FloatType.get()),
                Types.NestedField.optional(29, "order_number", Types.StringType.get()),
                Types.NestedField.optional(30, "reference_number", Types.StringType.get()),
                Types.NestedField.optional(31, "remarks", Types.StringType.get()),
                Types.NestedField.optional(32, "ship_id", Types.StringType.get()),
                Types.NestedField.optional(33, "total_amount", Types.DoubleType.get()),
                Types.NestedField.optional(34, "total_initial_amt", Types.DoubleType.get()),
                Types.NestedField.optional(35, "total_initial_quantity", Types.FloatType.get()),
                Types.NestedField.optional(36, "total_mrp", Types.DoubleType.get()),
                Types.NestedField.optional(37, "total_quantity", Types.FloatType.get()),
                Types.NestedField.optional(38, "type", Types.StringType.get()),
                Types.NestedField.optional(39, "delivery_date", Types.TimestampType.withZone()),
                Types.NestedField.optional(40, "status", Types.StringType.get()),
                Types.NestedField.optional(41, "location_hierarchy", Types.StringType.get()),
                Types.NestedField.optional(42, "outletcode", Types.StringType.get()),
                Types.NestedField.optional(43, "supplierid", Types.StringType.get()),
                Types.NestedField.optional(44, "hierarchy", Types.StringType.get()),
                Types.NestedField.optional(45, "status_reason", Types.StringType.get()),
                Types.NestedField.optional(46, "changed", Types.BooleanType.get()),
                Types.NestedField.optional(47, "group_id", Types.StringType.get()),
                Types.NestedField.optional(48, "beat", Types.StringType.get()),
                Types.NestedField.optional(49, "beat_name", Types.StringType.get()),
                Types.NestedField.optional(50, "initial_normalized_quantity", Types.FloatType.get()),
                Types.NestedField.optional(51, "normalized_volume", Types.FloatType.get()),
                Types.NestedField.optional(52, "processing_status", Types.StringType.get()),
                Types.NestedField.optional(53, "sales_date", Types.TimestampType.withZone()),
                Types.NestedField.optional(54, "sales_value", Types.DoubleType.get()),
                Types.NestedField.optional(55, "sub_type", Types.StringType.get()),
                Types.NestedField.optional(56, "in_beat", Types.BooleanType.get()),
                Types.NestedField.optional(57, "in_range", Types.BooleanType.get()),
                Types.NestedField.optional(58, "nw", Types.DoubleType.get()),
                Types.NestedField.optional(59, "reference_order_number", Types.StringType.get()));

        // ============================================================
        // Partition Strategy: bucket + time-based
        // ============================================================
        PartitionSpec partitionSpec = PartitionSpec.builderFor(schema)
                .bucket("entity_id", 16)
                .year("version_ts")
                .build();

        Table table = catalog.createTable(tableId, schema, partitionSpec, TABLE_PROPERTIES);
        log.info("✔ Created Iceberg v2 table: db.orders (partitioned by entity_id + version_ts)");
    }

    // ------------------------------------------------------------
    // CREATE db.order_details TABLE (with CDC versioning)
    // ------------------------------------------------------------
    public static void createOrderDetailsTable(Catalog catalog) {

        TableIdentifier tableId = TableIdentifier.of("db", "order_details");

        if (catalog.tableExists(tableId)) {
            log.info("✔ Table db.order_details already exists. Skipping.");
            return;
        }

        Schema schema = new Schema(
                // ============================================================
                // CDC Versioning Fields (required for tracking multiple versions)
                // ============================================================
                Types.NestedField.required(1, "entity_id", Types.StringType.get()),
                Types.NestedField.required(2, "version_ts", Types.TimestampType.withZone()),
                Types.NestedField.required(3, "event_type", Types.StringType.get()),
                Types.NestedField.required(4, "ingest_ts", Types.TimestampType.withZone()),
                Types.NestedField.required(5, "is_latest", Types.BooleanType.get()),

                // ============================================================
                // Original Business Fields (preserved from existing schema)
                // ============================================================
                Types.NestedField.required(6, "id", Types.StringType.get()),
                Types.NestedField.optional(7, "active_status", Types.StringType.get()),
                Types.NestedField.optional(8, "active_status_reason", Types.StringType.get()),
                Types.NestedField.optional(9, "created_by", Types.StringType.get()),
                Types.NestedField.optional(10, "creation_time", Types.TimestampType.withZone()),
                Types.NestedField.optional(11, "extended_attributes", Types.StringType.get()),
                Types.NestedField.optional(12, "hash", Types.StringType.get()),
                Types.NestedField.optional(13, "last_modified_time", Types.TimestampType.withZone()),
                Types.NestedField.optional(14, "lob", Types.StringType.get()),
                Types.NestedField.optional(15, "modified_by", Types.StringType.get()),
                Types.NestedField.optional(16, "source", Types.StringType.get()),
                Types.NestedField.optional(17, "version", Types.IntegerType.get()),
                Types.NestedField.optional(18, "system_time", Types.TimestampType.withZone()),
                Types.NestedField.optional(19, "batch_code", Types.StringType.get()),
                Types.NestedField.optional(20, "bill_amount", Types.DoubleType.get()),
                Types.NestedField.optional(21, "case_quantity", Types.FloatType.get()),
                Types.NestedField.optional(22, "color", Types.StringType.get()),
                Types.NestedField.optional(23, "discount_number", Types.StringType.get()),
                Types.NestedField.optional(24, "discount_info", Types.StringType.get()),
                Types.NestedField.optional(25, "initial_amount", Types.DoubleType.get()),
                Types.NestedField.optional(26, "initial_case_quantity", Types.FloatType.get()),
                Types.NestedField.optional(27, "initial_other_unit_quantity", Types.FloatType.get()),
                Types.NestedField.optional(28, "initial_piece_quantity", Types.FloatType.get()),
                Types.NestedField.optional(29, "initial_quantity", Types.FloatType.get()),
                Types.NestedField.optional(30, "mrp", Types.DoubleType.get()),
                Types.NestedField.optional(31, "name", Types.StringType.get()),
                Types.NestedField.optional(32, "net_amount", Types.DoubleType.get()),
                Types.NestedField.optional(33, "normalized_quantity", Types.FloatType.get()),
                Types.NestedField.optional(34, "other_unit_quantity", Types.FloatType.get()),
                Types.NestedField.optional(35, "piece_quantity", Types.FloatType.get()),
                Types.NestedField.optional(36, "product_info", Types.StringType.get()),
                Types.NestedField.optional(37, "product_key", Types.StringType.get()),
                Types.NestedField.optional(38, "quantity_unit", Types.StringType.get()),
                Types.NestedField.optional(39, "size", Types.StringType.get()),
                Types.NestedField.optional(40, "skucode", Types.StringType.get()),
                Types.NestedField.optional(41, "status", Types.StringType.get()),
                Types.NestedField.optional(42, "type", Types.StringType.get()),
                Types.NestedField.optional(43, "unit_of_measurement", Types.StringType.get()),
                Types.NestedField.optional(44, "price", Types.FloatType.get()),
                Types.NestedField.optional(45, "location_hierarchy", Types.StringType.get()),
                Types.NestedField.optional(46, "hierarchy", Types.StringType.get()),
                Types.NestedField.optional(47, "order_id", Types.StringType.get()),
                Types.NestedField.optional(48, "status_reason", Types.StringType.get()),
                Types.NestedField.optional(49, "changed", Types.BooleanType.get()),
                Types.NestedField.optional(50, "gps_latitude", Types.StringType.get()),
                Types.NestedField.optional(51, "gps_longitude", Types.StringType.get()),
                Types.NestedField.optional(52, "initial_normalized_quantity", Types.FloatType.get()),
                Types.NestedField.optional(53, "line_count", Types.IntegerType.get()),
                Types.NestedField.optional(54, "normalized_volume", Types.FloatType.get()),
                Types.NestedField.optional(55, "case_price", Types.FloatType.get()),
                Types.NestedField.optional(56, "batch_ids", Types.StringType.get()),
                Types.NestedField.optional(57, "other_unit_price", Types.FloatType.get()),
                Types.NestedField.optional(58, "sales_quantity", Types.FloatType.get()),
                Types.NestedField.optional(59, "sales_value", Types.DoubleType.get()),
                Types.NestedField.optional(60, "nw", Types.DoubleType.get()),
                Types.NestedField.optional(61, "site_id", Types.StringType.get()));

        // ============================================================
        // Partition Strategy: bucket + time-based
        // ============================================================
        PartitionSpec partitionSpec = PartitionSpec.builderFor(schema)
                .bucket("entity_id", 16)
                .month("version_ts")
                .build();

        Table table = catalog.createTable(tableId, schema, partitionSpec, TABLE_PROPERTIES);
        log.info("✔ Created Iceberg v2 table: db.order_details (partitioned by entity_id + version_ts)");
    }
}
