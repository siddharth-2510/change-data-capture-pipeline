package com.salescode.cdc.models;

import com.salescode.cdc.models.enums.OperationType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@Data
@NoArgsConstructor
public class CDCKafkaObject {
    private String id;
    private String entityName;
    private OperationType operationType;
    private List<Map<String,Object>> body;
    private String modifiedBy;
    private LocalDateTime timestamp;

    /**
     * Constructor with all fields (Lombok @AllArgsConstructor not processing, adding manually)
     */
    public CDCKafkaObject(String id, String entityName, OperationType operationType, 
                          List<Map<String,Object>> body, String modifiedBy, LocalDateTime timestamp) {
        this.id = id;
        this.entityName = entityName;
        this.operationType = operationType;
        this.body = body;
        this.modifiedBy = modifiedBy;
        this.timestamp = timestamp;
    }

    // Builder pattern (Lombok @Builder not processing, adding manually)
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String id;
        private String entityName;
        private OperationType operationType;
        private List<Map<String,Object>> body;
        private String modifiedBy;
        private LocalDateTime timestamp;

        public Builder id(String id) {
            this.id = id;
            return this;
        }

        public Builder entityName(String entityName) {
            this.entityName = entityName;
            return this;
        }

        public Builder operationType(OperationType operationType) {
            this.operationType = operationType;
            return this;
        }

        public Builder body(List<Map<String,Object>> body) {
            this.body = body;
            return this;
        }

        public Builder modifiedBy(String modifiedBy) {
            this.modifiedBy = modifiedBy;
            return this;
        }

        public Builder timestamp(LocalDateTime timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public CDCKafkaObject build() {
            return new CDCKafkaObject(id, entityName, operationType, body, modifiedBy, timestamp);
        }
    }
}
