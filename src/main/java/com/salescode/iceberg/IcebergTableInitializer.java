package com.salescode.iceberg;

import com.salescode.config.AppConfig;
import com.salescode.config.IcebergConfig;
import com.salescode.config.S3Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hadoop.HadoopCatalog;

/**
 * Initializes Iceberg tables at application startup.
 * Ensures tables exist with proper CDC versioning schema.
 */
@Slf4j
public class IcebergTableInitializer {

    // ============================================================
    // Config-aware method (preferred)
    // ============================================================

    public static void ensureTablesExist(AppConfig config) {
        log.info("Initializing Iceberg tables...");

        IcebergConfig icebergConfig = config.getIceberg();
        S3Config s3Config = config.getS3();

        log.info("Using warehouse: {}", icebergConfig.getWarehouse());
        log.info("Using S3 endpoint: {}", s3Config.getEndpoint());

        try {
            Configuration hadoopConf = IcebergUtil.hadoopConf(s3Config);
            Catalog catalog = new HadoopCatalog(hadoopConf, icebergConfig.getWarehouse());

            CreateIcebergTables.createOrdersTable(catalog, icebergConfig.getDatabase());

            log.info("✔ Iceberg tables initialization complete.");
        } catch (Exception e) {
            log.error("✖ Failed to initialize Iceberg tables: {}", e.getMessage(), e);
            throw new RuntimeException("Iceberg table initialization failed", e);
        }
    }

    // ============================================================
    // Legacy method (deprecated - for backward compatibility)
    // ============================================================

    private static final String WAREHOUSE = "s3a://warehouse-v1";

    @Deprecated
    public static void ensureTablesExist() {
        log.info("Initializing Iceberg tables (using hardcoded config)...");

        try {
            Configuration conf = new Configuration();
            conf.set("fs.s3a.endpoint", "http://localhost:9000");
            conf.set("fs.s3a.access.key", "minio");
            conf.set("fs.s3a.secret.key", "minio123");
            conf.set("fs.s3a.path.style.access", "true");
            conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

            Catalog catalog = new HadoopCatalog(conf, WAREHOUSE);

            CreateIcebergTables.createOrdersTable(catalog, "db");

            log.info("✔ Iceberg tables initialization complete.");
        } catch (Exception e) {
            log.error("✖ Failed to initialize Iceberg tables: {}", e.getMessage(), e);
            throw new RuntimeException("Iceberg table initialization failed", e);
        }
    }
}
