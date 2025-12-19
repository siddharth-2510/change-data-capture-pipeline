package com.salescode.cdc.listener;

import com.salescode.cdc.kafka.CDCEventSerializer;
import com.salescode.cdc.kafka.CDCKafkaConstants;
import com.salescode.cdc.kafka.CDCProducer;
import com.salescode.cdc.models.CDCKafkaObject;
import com.salescode.cdc.models.enums.OperationType;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.hibernate.event.spi.*;
import org.hibernate.persister.entity.EntityPersister;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import javax.persistence.Table;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GlobalEntityListener implements PostInsertEventListener,
        PostUpdateEventListener,
        PostDeleteEventListener {

    private static final Logger log = LoggerFactory.getLogger(GlobalEntityListener.class);

    private static final Producer<String, CDCKafkaObject> PRODUCER =
            CDCProducer.createCDCProducer();
    @Override
    public void onPostDelete(PostDeleteEvent postDeleteEvent) {

    }

    @Override
    public void onPostInsert(PostInsertEvent event) {
        try {

            Object entity = event.getEntity();
            EntityPersister persister = event.getPersister();
            Object[] state = event.getState();
            Serializable id = event.getId();

            String entityName = entity.getClass().getSimpleName();

            Map<String, Object> afterState = mapStateToFields(persister, state);

            CDCKafkaObject cdcEvent = CDCKafkaObject.builder()
                    .id(id != null ? id.toString() : null)
                    .entityName(entityName)
                    .operationType(OperationType.insert)
                    .body(List.of(afterState))  // INSERT only has "after" state
                    .modifiedBy(extractModifiedBy(entity))
                    .timestamp(LocalDateTime.now())
                    .build();

            String key = String.format("%s_%s", entityName, id);

            ProducerRecord<String, CDCKafkaObject> record =
                    new ProducerRecord<>(CDCKafkaConstants.CDC_TOPIC, key, cdcEvent);

            PRODUCER.send(record, (metadata, exception) -> {
                if (exception == null) {
                    log.error("✅ CDC INSERT sent: {} - {} - partition: {}, offset: {}",
                            entityName, id, metadata.partition(), metadata.offset());
                } else {
                    log.error("❌ Failed to send CDC INSERT: {} - {}", entityName, id, exception);
                }
            });

        } catch (Exception e) {
            log.error("❌ Error in onPostInsert for entity: {}",
                    event.getEntity().getClass().getSimpleName(), e);
        }
    }

    // Helper method to map Hibernate state array to field names
    private Map<String, Object> mapStateToFields(EntityPersister persister, Object[] state) {
        Map<String, Object> fields = new HashMap<>();
        String[] propertyNames = persister.getPropertyNames();

        for (int i = 0; i < propertyNames.length && i < state.length; i++) {
            fields.put(propertyNames[i], state[i]);
        }

        return fields;
    }



    private String extractModifiedBy(Object entity) {
        try {
            // Try common field names via reflection
            for (String fieldName : Arrays.asList("modifiedBy", "updatedBy")) {
                Field field = entity.getClass().getDeclaredField(fieldName);
                field.setAccessible(true);
                Object value = field.get(entity);
                if (value != null) {
                    return value.toString();
                }
            }
        } catch (Exception e) {
            log.error("Field either inaccessible or not present",e.getStackTrace());
        }
        return "system"; // Default fallback
    }

    @Override
    public void onPostUpdate(PostUpdateEvent postUpdateEvent) {

    }

    @Override
    public boolean requiresPostCommitHanding(EntityPersister entityPersister) {
        return false;
    }
    @PreDestroy
    public void cleanup() {
        if (PRODUCER != null) {
            PRODUCER.flush();
            PRODUCER.close();
            log.info("✅ CDC Producer closed");
        }
    }
}
