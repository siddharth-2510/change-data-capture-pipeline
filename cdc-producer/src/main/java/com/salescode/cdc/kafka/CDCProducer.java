package com.salescode.cdc.kafka;

import com.salescode.cdc.models.CDCKafkaObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class CDCProducer {

    private static Properties getCommonProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CDCKafkaConstants.KAFKA_BROKERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1); // Maintains order
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true); // Prevents duplicates
        return props;
    }

    public static Producer<String, CDCKafkaObject> createCDCProducer() {
        Properties props = getCommonProperties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, CDCKafkaConstants.CDC_CLIENT_ID);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CDCEventSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    public static Producer<String, CDCKafkaObject> createCDCProducer(String brokers) {
        Properties props = getCommonProperties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, CDCKafkaConstants.CDC_CLIENT_ID);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CDCEventSerializer.class.getName());
        return new KafkaProducer<>(props);
    }
}