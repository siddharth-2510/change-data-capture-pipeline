package com.salescode;

import com.salescode.config.AppConfig;
import com.salescode.config.ConfigLoader;
import com.salescode.kafka.KafkaSourceBuilder;
import com.salescode.sink.IcebergSinkBuilder;
import com.salescode.transformer.OrderHeaderTransformer;
import com.salescode.iceberg.IcebergUtil;

import lombok.extern.slf4j.Slf4j;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.flink.TableLoader;

/**
 * Main Flink Application:
 * Kafka → Transform Orders → Iceberg (AWS Glue/S3)
 */
@Slf4j
public class Main {

        public static void main(String[] args) throws Exception {

                log.info("-------------- Starting Flink Application --------------");

                // ------------------------------------------------------------------
                // 1️⃣ Load Configuration
                // ------------------------------------------------------------------
                AppConfig config = ConfigLoader.loadConfig("application.yaml");
                log.info("Configuration loaded successfully.");

                // ------------------------------------------------------------------
                // 2️⃣ Initialize Flink Environment
                // ------------------------------------------------------------------
                StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(1);

                // Enable checkpointing (REQUIRED for Iceberg to commit data)
                env.enableCheckpointing(300_000); // Checkpoint every 5 minutes
                log.info("Checkpointing enabled (5min interval)");

                // ------------------------------------------------------------------
                // 3️⃣ Build Kafka Source using Config
                // ------------------------------------------------------------------
                KafkaSource<ObjectNode> kafkaSource = KafkaSourceBuilder.build(config.getKafka());

                DataStream<ObjectNode> kafkaStream = env.fromSource(
                                kafkaSource,
                                WatermarkStrategy.noWatermarks(),
                                "KafkaSource");

                log.info("Kafka source initialized.");

                // ------------------------------------------------------------------
                // 4️⃣ RAW Kafka Print (optional - for debugging)
                // ------------------------------------------------------------------
                kafkaStream.print("RAW FROM KAFKA");

                // ------------------------------------------------------------------
                // 5️⃣ Transform → Orders
                // ------------------------------------------------------------------
                DataStream<ObjectNode> orderStream = kafkaStream.flatMap(new OrderHeaderTransformer());

                orderStream.print("ORDERS");

                // ------------------------------------------------------------------
                // 6️⃣ Configure Partition Spec (if needed)
                // ------------------------------------------------------------------
                log.info("Configuring partition specification...");
                try {
                        com.salescode.iceberg.PartitionManager.addPartitionSpecIfNeeded(
                                        config.getIceberg(),
                                        config.getS3());
                } catch (Exception e) {
                        log.warn("Could not configure partitions (table may already be partitioned): {}",
                                        e.getMessage());
                }

                // ------------------------------------------------------------------
                // 7️⃣ Write to Iceberg Table in S3 via Glue Catalog
                // ------------------------------------------------------------------
                log.info("Setting up Iceberg sink...");

                // Load table loader for orders table from Glue catalog
                TableLoader ordersTableLoader = IcebergUtil.ordersTableLoader(config.getIceberg(), config.getS3());

                // Create and attach Iceberg sink
                var orderSink = IcebergSinkBuilder.createOrderSink(orderStream, ordersTableLoader);
                log.info("✔ Orders sink configured → iceberg_db_test.ck_orders");

                // ------------------------------------------------------------------
                // 8️⃣ Execute Flink Job
                // ------------------------------------------------------------------
                env.execute("Flink Kafka → Iceberg Pipeline (AWS Glue)");
        }
}
