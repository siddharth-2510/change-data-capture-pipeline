package com.salescode.cdc.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.salescode.cdc.models.CDCKafkaObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;


import java.util.Map;

@Slf4j
public class CDCEventSerializer implements Serializer<CDCKafkaObject> {

    private final ObjectMapper objectMapper;

    public CDCEventSerializer() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
        this.objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        this.objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, CDCKafkaObject data) {
        if (data == null) {

//            log.error("Received null CDCKafkaObject for serialization");
            return null;
        }

        try {
            String json = objectMapper.writeValueAsString(data);
//            log.debug("Serialized CDCKafkaObject: entity={}, operation={}, id={}",
//                    data.getEntityName(), data.getOperationType(), data.getId());
            return json.getBytes();
        } catch (Exception e) {
//            log.error("Error serializing CDCKafkaObject: entity={}, operation={}, id={}",
//                    data.getEntityName(), data.getOperationType(), data.getId(), e);
            throw new SerializationException("Error serializing CDCKafkaObject to JSON", e);
        }
    }

    @Override
    public void close() {
    }
}