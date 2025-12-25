package com.salescode.iceberg;

import com.salescode.config.IcebergConfig;
import com.salescode.config.S3Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.flink.TableLoader;

@Slf4j
public class IcebergUtil {

    // ============================================================
    // Config-aware methods (preferred)
    // ============================================================

    public static TableLoader ordersTableLoader(IcebergConfig icebergConfig, S3Config s3Config) {
        String tablePath = icebergConfig.getWarehouse() + "/" + icebergConfig.getDatabase() + "/orders";
        log.info("Loading orders table from: {}", tablePath);
        return TableLoader.fromHadoopTable(tablePath, hadoopConf(s3Config));
    }

    public static TableLoader orderDetailsTableLoader(IcebergConfig icebergConfig, S3Config s3Config) {
        String tablePath = icebergConfig.getWarehouse() + "/" + icebergConfig.getDatabase() + "/order_details";
        log.info("Loading order_details table from: {}", tablePath);
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