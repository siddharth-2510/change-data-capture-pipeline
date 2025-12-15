package com.salescode.iceberg;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;

public class CreateIcebergTables {

    public static void main(String[] args) {

        // MinIO (S3A) Hadoop Configuration
        Configuration conf = new Configuration();
        conf.set("fs.s3a.endpoint", "http://localhost:9000");
        conf.set("fs.s3a.access.key", "minio");
        conf.set("fs.s3a.secret.key", "minio123");
        conf.set("fs.s3a.path.style.access", "true");
        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

        // Iceberg Warehouse (bucket root)
        String warehouse = "s3a://warehouse";

        Catalog catalog = new HadoopCatalog(conf, warehouse);

        createOrdersTable(catalog);
        createOrderDetailsTable(catalog);

        System.out.println("✔ All Iceberg tables are ready.");
    }

    // ------------------------------------------------------------
    // CREATE ck_orders TABLE
    // ------------------------------------------------------------
    public static void createOrdersTable(Catalog catalog) {

        TableIdentifier tableId = TableIdentifier.of("db", "orders");

        if (catalog.tableExists(tableId)) {
            System.out.println("✔ Table db.orders already exists. Skipping.");
            return;
        }

        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.StringType.get()),
                Types.NestedField.optional(2, "active_status", Types.StringType.get()),
                Types.NestedField.optional(3, "active_status_reason", Types.StringType.get()),
                Types.NestedField.optional(4, "created_by", Types.StringType.get()),
                Types.NestedField.optional(5, "creation_time", Types.TimestampType.withZone()),
                Types.NestedField.optional(6, "extended_attributes", Types.StringType.get()),
                Types.NestedField.optional(7, "hash", Types.StringType.get()),
                Types.NestedField.optional(8, "last_modified_time", Types.TimestampType.withZone()),
                Types.NestedField.optional(9, "lob", Types.StringType.get()),
                Types.NestedField.optional(10, "modified_by", Types.StringType.get()),
                Types.NestedField.optional(11, "source", Types.StringType.get()),
                Types.NestedField.optional(12, "version", Types.IntegerType.get()),
                Types.NestedField.optional(13, "system_time", Types.TimestampType.withZone()),
                Types.NestedField.optional(14, "bill_amount", Types.DoubleType.get()),
                Types.NestedField.optional(15, "channel", Types.StringType.get()),
                Types.NestedField.optional(16, "discount_info", Types.StringType.get()),
                Types.NestedField.optional(17, "gps_latitude", Types.StringType.get()),
                Types.NestedField.optional(18, "gps_longitude", Types.StringType.get()),
                Types.NestedField.optional(19, "user_hierarchy", Types.StringType.get()),
                Types.NestedField.optional(20, "line_count", Types.IntegerType.get()),
                Types.NestedField.optional(21, "loginid", Types.StringType.get()),
                Types.NestedField.optional(22, "net_amount", Types.DoubleType.get()),
                Types.NestedField.optional(23, "normalized_quantity", Types.FloatType.get()),
                Types.NestedField.optional(24, "order_number", Types.StringType.get()),
                Types.NestedField.optional(25, "reference_number", Types.StringType.get()),
                Types.NestedField.optional(26, "remarks", Types.StringType.get()),
                Types.NestedField.optional(27, "ship_id", Types.StringType.get()),
                Types.NestedField.optional(28, "total_amount", Types.DoubleType.get()),
                Types.NestedField.optional(29, "total_initial_amt", Types.DoubleType.get()),
                Types.NestedField.optional(30, "total_initial_quantity", Types.FloatType.get()),
                Types.NestedField.optional(31, "total_mrp", Types.DoubleType.get()),
                Types.NestedField.optional(32, "total_quantity", Types.FloatType.get()),
                Types.NestedField.optional(33, "type", Types.StringType.get()),
                Types.NestedField.optional(34, "delivery_date", Types.TimestampType.withZone()),
                Types.NestedField.optional(35, "status", Types.StringType.get()),
                Types.NestedField.optional(36, "location_hierarchy", Types.StringType.get()),
                Types.NestedField.optional(37, "outletcode", Types.StringType.get()),
                Types.NestedField.optional(38, "supplierid", Types.StringType.get()),
                Types.NestedField.optional(39, "hierarchy", Types.StringType.get()),
                Types.NestedField.optional(40, "status_reason", Types.StringType.get()),
                Types.NestedField.optional(41, "changed", Types.BooleanType.get()),
                Types.NestedField.optional(42, "group_id", Types.StringType.get()),
                Types.NestedField.optional(43, "beat", Types.StringType.get()),
                Types.NestedField.optional(44, "beat_name", Types.StringType.get()),
                Types.NestedField.optional(45, "initial_normalized_quantity", Types.FloatType.get()),
                Types.NestedField.optional(46, "normalized_volume", Types.FloatType.get()),
                Types.NestedField.optional(47, "processing_status", Types.StringType.get()),
                Types.NestedField.optional(48, "sales_date", Types.TimestampType.withZone()),
                Types.NestedField.optional(49, "sales_value", Types.DoubleType.get()),
                Types.NestedField.optional(50, "sub_type", Types.StringType.get()),
                Types.NestedField.optional(51, "in_beat", Types.BooleanType.get()),
                Types.NestedField.optional(52, "in_range", Types.BooleanType.get()),
                Types.NestedField.optional(53, "nw", Types.DoubleType.get()),
                Types.NestedField.optional(54, "reference_order_number", Types.StringType.get())
        );

        Table table = catalog.createTable(tableId, schema);
        System.out.println("✔ Created Iceberg table: db.orders");
    }

    // ------------------------------------------------------------
    // CREATE ck_order_details TABLE
    // ------------------------------------------------------------
    public static void createOrderDetailsTable(Catalog catalog) {

        TableIdentifier tableId = TableIdentifier.of("db", "order_details");

        if (catalog.tableExists(tableId)) {
            System.out.println("✔ Table db.order_details already exists. Skipping.");
            return;
        }

        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.StringType.get()),
                Types.NestedField.optional(2, "active_status", Types.StringType.get()),
                Types.NestedField.optional(3, "active_status_reason", Types.StringType.get()),
                Types.NestedField.optional(4, "created_by", Types.StringType.get()),
                Types.NestedField.optional(5, "creation_time", Types.TimestampType.withZone()),
                Types.NestedField.optional(6, "extended_attributes", Types.StringType.get()),
                Types.NestedField.optional(7, "hash", Types.StringType.get()),
                Types.NestedField.optional(8, "last_modified_time", Types.TimestampType.withZone()),
                Types.NestedField.optional(9, "lob", Types.StringType.get()),
                Types.NestedField.optional(10, "modified_by", Types.StringType.get()),
                Types.NestedField.optional(11, "source", Types.StringType.get()),
                Types.NestedField.optional(12, "version", Types.IntegerType.get()),
                Types.NestedField.optional(13, "system_time", Types.TimestampType.withZone()),
                Types.NestedField.optional(14, "batch_code", Types.StringType.get()),
                Types.NestedField.optional(15, "bill_amount", Types.DoubleType.get()),
                Types.NestedField.optional(16, "case_quantity", Types.FloatType.get()),
                Types.NestedField.optional(17, "color", Types.StringType.get()),
                Types.NestedField.optional(18, "discount_number", Types.StringType.get()),
                Types.NestedField.optional(19, "discount_info", Types.StringType.get()),
                Types.NestedField.optional(20, "initial_amount", Types.DoubleType.get()),
                Types.NestedField.optional(21, "initial_case_quantity", Types.FloatType.get()),
                Types.NestedField.optional(22, "initial_other_unit_quantity", Types.FloatType.get()),
                Types.NestedField.optional(23, "initial_piece_quantity", Types.FloatType.get()),
                Types.NestedField.optional(24, "initial_quantity", Types.FloatType.get()),
                Types.NestedField.optional(25, "mrp", Types.DoubleType.get()),
                Types.NestedField.optional(26, "name", Types.StringType.get()),
                Types.NestedField.optional(27, "net_amount", Types.DoubleType.get()),
                Types.NestedField.optional(28, "normalized_quantity", Types.FloatType.get()),
                Types.NestedField.optional(29, "other_unit_quantity", Types.FloatType.get()),
                Types.NestedField.optional(30, "piece_quantity", Types.FloatType.get()),
                Types.NestedField.optional(31, "product_info", Types.StringType.get()),
                Types.NestedField.optional(32, "product_key", Types.StringType.get()),
                Types.NestedField.optional(33, "quantity_unit", Types.StringType.get()),
                Types.NestedField.optional(34, "size", Types.StringType.get()),
                Types.NestedField.optional(35, "skucode", Types.StringType.get()),
                Types.NestedField.optional(36, "status", Types.StringType.get()),
                Types.NestedField.optional(37, "type", Types.StringType.get()),
                Types.NestedField.optional(38, "unit_of_measurement", Types.StringType.get()),
                Types.NestedField.optional(39, "price", Types.FloatType.get()),
                Types.NestedField.optional(40, "location_hierarchy", Types.StringType.get()),
                Types.NestedField.optional(41, "hierarchy", Types.StringType.get()),
                Types.NestedField.optional(42, "order_id", Types.StringType.get()),
                Types.NestedField.optional(43, "status_reason", Types.StringType.get()),
                Types.NestedField.optional(44, "changed", Types.BooleanType.get()),
                Types.NestedField.optional(45, "gps_latitude", Types.StringType.get()),
                Types.NestedField.optional(46, "gps_longitude", Types.StringType.get()),
                Types.NestedField.optional(47, "initial_normalized_quantity", Types.FloatType.get()),
                Types.NestedField.optional(48, "line_count", Types.IntegerType.get()),
                Types.NestedField.optional(49, "normalized_volume", Types.FloatType.get()),
                Types.NestedField.optional(50, "case_price", Types.FloatType.get()),
                Types.NestedField.optional(51, "batch_ids", Types.StringType.get()),
                Types.NestedField.optional(52, "other_unit_price", Types.FloatType.get()),
                Types.NestedField.optional(53, "sales_quantity", Types.FloatType.get()),
                Types.NestedField.optional(54, "sales_value", Types.DoubleType.get()),
                Types.NestedField.optional(55, "nw", Types.DoubleType.get()),
                Types.NestedField.optional(56, "site_id", Types.StringType.get())
        );

        Table table = catalog.createTable(tableId, schema);
        System.out.println("✔ Created Iceberg table: db.order_details");
    }
}
