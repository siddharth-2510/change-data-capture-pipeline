package com.salescode.iceberg;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hadoop.HadoopCatalog;

public class IcebergTableInitializer {

    private static final String WAREHOUSE = "s3a://warehouse";

    public static void ensureTablesExist() {

        Configuration conf = new Configuration();
        conf.set("fs.s3a.endpoint", "http://localhost:9000");
        conf.set("fs.s3a.access.key", "minio");
        conf.set("fs.s3a.secret.key", "minio123");
        conf.set("fs.s3a.path.style.access", "true");
        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

        Catalog catalog = new HadoopCatalog(conf, WAREHOUSE);

        CreateIcebergTables.createOrdersTable(catalog);
        CreateIcebergTables.createOrderDetailsTable(catalog);
    }
}
