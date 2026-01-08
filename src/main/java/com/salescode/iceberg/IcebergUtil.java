package com.salescode.iceberg;

import com.salescode.config.IcebergConfig;
import com.salescode.config.S3Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class IcebergUtil {

    // ============================================================
    // Glue Catalog Loader
    // ============================================================

    public static CatalogLoader glueCatalogLoader(IcebergConfig icebergConfig) {
        Map<String, String> properties = new HashMap<>();
        properties.put("warehouse", icebergConfig.getWarehouse());
        properties.put("io-impl", icebergConfig.getIoImpl());

        log.info("Creating Glue CatalogLoader with warehouse: {}", icebergConfig.getWarehouse());

        return CatalogLoader.custom(
                "glue",
                properties,
                new Configuration(),
                "org.apache.iceberg.aws.glue.GlueCatalog"
        );
    }

    // ============================================================
    // Glue-based Table Loader (preferred for Athena integration)
    // ============================================================

    public static TableLoader ordersTableLoader(IcebergConfig icebergConfig) {
        CatalogLoader catalogLoader = glueCatalogLoader(icebergConfig);
        TableIdentifier tableId = TableIdentifier.of(icebergConfig.getDatabase(), "orders");
        log.info("Loading orders table from Glue Catalog: {}.{}", icebergConfig.getDatabase(), "orders");
        return TableLoader.fromCatalog(catalogLoader, tableId);
    }

    // ============================================================
    // Legacy Hadoop-based methods (for backward compatibility)
    // ============================================================

    @Deprecated
    public static TableLoader ordersTableLoader(IcebergConfig icebergConfig, S3Config s3Config) {
        String tablePath = icebergConfig.getWarehouse() + "/" + icebergConfig.getDatabase() + "/orders";
        log.info("Loading orders table from Hadoop path: {}", tablePath);
        return TableLoader.fromHadoopTable(tablePath, hadoopConf(s3Config));
    }

    public static Configuration hadoopConf(S3Config s3Config) {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.endpoint", s3Config.getEndpoint());
        conf.set("fs.s3a.access.key", s3Config.getAccessKey());
        conf.set("fs.s3a.secret.key", s3Config.getSecretKey());

        // Set session token if present (for AWS temporary credentials)
        if (s3Config.getSessionToken() != null && !s3Config.getSessionToken().isEmpty()) {
            conf.set("fs.s3a.session.token", s3Config.getSessionToken());
            log.info("AWS session token configured for temporary credentials");
        }

        conf.set("fs.s3a.path.style.access", String.valueOf(s3Config.isPathStyleAccess()));
        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        return conf;
    }
}