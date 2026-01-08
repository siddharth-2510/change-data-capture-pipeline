package com.salescode.iceberg;

import com.salescode.config.IcebergConfig;
import com.salescode.config.S3Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class IcebergUtil {

    // ============================================================
    // Glue Catalog Configuration
    // ============================================================

    public static TableLoader ordersTableLoader(IcebergConfig icebergConfig, S3Config s3Config) {
        log.info("Loading orders table from Glue catalog");

        // Create Glue catalog configuration
        Map<String, String> catalogProperties = new HashMap<>();
        catalogProperties.put("warehouse", icebergConfig.getWarehouse());
        catalogProperties.put("catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog");
        catalogProperties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");

        // Configure S3 access
        Configuration hadoopConf = hadoopConf(s3Config);

        // Table identifier for Glue catalog
        String database = icebergConfig.getDatabase();
        String table = icebergConfig.getTable();

        log.info("Creating Glue catalog loader for table: {}.{}", database, table);

        // Create a CatalogLoader using the catalog properties and Hadoop configuration
        CatalogLoader catalogLoader = CatalogLoader.custom(
                icebergConfig.getCatalogName(),
                catalogProperties,
                hadoopConf,
                "org.apache.iceberg.aws.glue.GlueCatalog");

        // Use the CatalogLoader to create the TableLoader
        return TableLoader.fromCatalog(
                catalogLoader,
                TableIdentifier.of(database, table));
    }

    public static Configuration hadoopConf(S3Config s3Config) {
        Configuration conf = new Configuration();

        // S3 endpoint configuration
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

        // AWS Glue specific configurations
        conf.set("fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain");

        return conf;
    }

}