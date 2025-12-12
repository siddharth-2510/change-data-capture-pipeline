package com.salescode;

import com.salescode.config.AppConfig;
import com.salescode.config.ConfigLoader;
import com.salescode.kafka.KafkaSourceBuilder;

import com.salescode.transformer.OrderHeaderTransformer;
import com.salescode.transformer.OrderDetailsTransformer;

import lombok.extern.slf4j.Slf4j;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Main Flink Application:
 * Kafka → Transform (Order Header + Order Details)
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

        // ------------------------------------------------------------------
        // 3️⃣ Build Kafka Source using Config
        // ------------------------------------------------------------------
        KafkaSource<ObjectNode> kafkaSource =
                KafkaSourceBuilder.build(config.getKafka());

        DataStream<ObjectNode> kafkaStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "KafkaSource"
        );

        log.info("Kafka source initialized.");

        // ------------------------------------------------------------------
        // 4️⃣ RAW Kafka Print
        // ------------------------------------------------------------------
        kafkaStream.print("RAW FROM KAFKA");

        // ------------------------------------------------------------------
        // 5️⃣ Transform → Order Headers (ck_orders)
        // ------------------------------------------------------------------
        DataStream<ObjectNode> orderHeaderStream =
                kafkaStream.flatMap(new OrderHeaderTransformer());

        orderHeaderStream.print("ORDER_HEADER");

        // ------------------------------------------------------------------
        // 6️⃣ Transform → Order Details (ck_order_details)
        // ------------------------------------------------------------------
        DataStream<ObjectNode> orderDetailsStream =
                kafkaStream.flatMap(new OrderDetailsTransformer());

        orderDetailsStream.print("ORDER_DETAILS");

        // ------------------------------------------------------------------
        // 7️⃣ Execute Flink Job
        // ------------------------------------------------------------------
        env.execute("Flink Kafka → OrderHeader + OrderDetails Transformation Job");
    }
}
