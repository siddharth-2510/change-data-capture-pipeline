package com.salescode.iceberg;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.TableLoader;

public class IcebergUtil {

    public static TableLoader ordersTableLoader() {
        return TableLoader.fromHadoopTable(
                "s3a://warehouse-v1/db/orders",
                hadoopConf());
    }

    public static TableLoader orderDetailsTableLoader() {
        return TableLoader.fromHadoopTable(
                "s3a://warehouse-v1/db/order_details",
                hadoopConf());
    }

    private static Configuration hadoopConf() {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.endpoint", "http://localhost:9000");
        conf.set("fs.s3a.access.key", "minio");
        conf.set("fs.s3a.secret.key", "minio123");
        conf.set("fs.s3a.path.style.access", "true");
        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        return conf;
    }
}
