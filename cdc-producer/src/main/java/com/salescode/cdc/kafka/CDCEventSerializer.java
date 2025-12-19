package com.salescode.cdc.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.salescode.cdc.models.CDCKafkaObject;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class CDCEventSerializer implements Serializer<CDCKafkaObject> {

    private static final Logger logger = LoggerFactory.getLogger(CDCEventSerializer.class);
    private final ObjectMapper objectMapper;

    public CDCEventSerializer() {
        this.objectMapper = new ObjectMapper();
        // Register JavaTimeModule to handle LocalDateTime serialization
        this.objectMapper.registerModule(new JavaTimeModule());
        // Write dates as ISO-8601 strings instead of timestamps
        this.objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        // Don't fail on empty beans
        this.objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // No additional configuration needed
    }

    @Override
    public byte[] serialize(String topic, CDCKafkaObject data) {
        if (data == null) {
            logger.warn("Received null CDCKafkaObject for serialization");
            return null;
        }

        try {
            String json = objectMapper.writeValueAsString(data);
            logger.debug("Serialized CDCKafkaObject: entity={}, operation={}, id={}",
                    data.getEntityName(), data.getOperationType(), data.getId());
            return json.getBytes();
        } catch (Exception e) {
            logger.error("Error serializing CDCKafkaObject: entity={}, operation={}, id={}",
                    data.getEntityName(), data.getOperationType(), data.getId(), e);
            throw new SerializationException("Error serializing CDCKafkaObject to JSON", e);
        }
    }

    @Override
    public void close() {
        // No resources to close
    }
}