package com.salescode.cdc.models;

import com.salescode.cdc.models.enums.OperationType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CDCKafkaObject {
    private String id;
    private String entityName;
    private OperationType operationType;
    private List<Map<String,Object>> body;
    private String modifiedBy;
    private LocalDateTime timestamp;
}
