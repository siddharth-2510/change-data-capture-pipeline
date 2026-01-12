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

    public static CatalogLoader glueCatalogLoader(IcebergConfig icebergConfig) {
        Map<String, String> properties = new HashMap<>();
        properties.put("warehouse", icebergConfig.getWarehouse());
        properties.put("io-impl", icebergConfig.getIoImpl());

        // Use the SSO profile for AWS credentials
        properties.put("client.credentials-provider",
                "software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider");
        properties.put("client.credentials-provider.profile-name",
                "DataLakeDeveloperAccess-240754906059");

        log.info("Creating Glue CatalogLoader with warehouse: {} using SSO profile", icebergConfig.getWarehouse());

        return CatalogLoader.custom(
                "glue",
                properties,
                new Configuration(),
                "org.apache.iceberg.aws.glue.GlueCatalog");
    }

    public static TableLoader ordersTableLoader(IcebergConfig icebergConfig) {
        CatalogLoader catalogLoader = glueCatalogLoader(icebergConfig);
        String tableName = icebergConfig.getTable();
        TableIdentifier tableId = TableIdentifier.of(icebergConfig.getDatabase(), tableName);
        log.info("Loading orders table from Glue Catalog: {}.{}", icebergConfig.getDatabase(), tableName);
        return TableLoader.fromCatalog(catalogLoader, tableId);
    }

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