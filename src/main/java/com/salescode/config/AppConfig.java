package com.salescode.config;

import lombok.Data;

@Data
public class AppConfig {
    private KafkaConfig kafka;
    private IcebergConfig iceberg;
    private S3Config s3;
}
