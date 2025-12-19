package com.salescode.cdc.kafka;

public class CDCKafkaConstants {

    // Kafka Broker Configuration
    public static final String KAFKA_BROKERS = System.getenv("KAFKA_BROKERS") != null
            ? System.getenv("KAFKA_BROKERS")
            : "localhost:9092";

    // CDC Producer Configuration
    public static final String CDC_CLIENT_ID = "cdc-producer-client";
    public static final String CDC_TOPIC = System.getenv("CDC_TOPIC") != null
            ? System.getenv("CDC_TOPIC")
            : "cdc-events";

    // Optional: Topic naming patterns
    public static final String CDC_TOPIC_PREFIX = "cdc.";

    // Private constructor to prevent instantiation
    private CDCKafkaConstants() {
        throw new UnsupportedOperationException("This is a constants class and cannot be instantiated");
    }
}