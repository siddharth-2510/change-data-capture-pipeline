package com.salescode;

import com.salescode.config.AppConfig;
import com.salescode.config.ConfigLoader;
import com.salescode.kafka.KafkaSourceBuilder;
import com.salescode.iceberg.*;
import com.salescode.sink.IcebergSinkBuilder;

import com.salescode.transformer.OrderHeaderTransformer;

import lombok.extern.slf4j.Slf4j;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.flink.TableLoader;

/**
 * Main Flink Application:
 * Kafka → Transform (Order Headers with embedded Order Details) → Iceberg
 * (MinIO)
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

                log.info("Ensuring Iceberg tables exist....");
                IcebergTableInitializer.ensureTablesExist(config);
                log.info("Iceberg tables are ready.");

                // Enable checkpointing (REQUIRED for Iceberg to commit data)
                env.enableCheckpointing(30); // Checkpoint every 30 seconds
                log.info("Checkpointing enabled (30s interval)");

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
                // 5️⃣ Transform → Order Headers (ck_orders)
                // ------------------------------------------------------------------
                DataStream<ObjectNode> orderHeaderStream = kafkaStream.flatMap(new OrderHeaderTransformer());

                orderHeaderStream.print("ORDER_HEADER");

                // ------------------------------------------------------------------
                // 7️⃣ Write to Iceberg Tables in MinIO
                // ------------------------------------------------------------------
                log.info("Setting up Iceberg sinks...");

                // Load table loader (using Glue Catalog)
                TableLoader ordersTableLoader = IcebergUtil.ordersTableLoader(config.getIceberg());

                // Create and attach Iceberg sink
                var orderHeaderSink = IcebergSinkBuilder.createOrderHeaderSink(orderHeaderStream, ordersTableLoader);
                log.info("✔ Order sink configured → db1.orders (with embedded order_details)");

                // ------------------------------------------------------------------
                // 8️⃣ Execute Flink Job
                // ------------------------------------------------------------------
                env.execute("Flink Kafka → Iceberg Pipeline (Orders)");
        }
}
