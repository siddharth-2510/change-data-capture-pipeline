package com.salescode.cdc.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@NoArgsConstructor
@Data
public  class BatchSummary implements Serializable {
    private static final long serialVersionUID = 1L;

    private long totalRecords;
    private long passedRecords;
    private long failedRecords;
    private List<Map<String, Object>> failedRecordsList;
    private long timestamp;

    /**
     * Constructor with all fields
     */
    public BatchSummary(long totalRecords, long passedRecords, long failedRecords, 
                       List<Map<String, Object>> failedRecordsList, long timestamp) {
        this.totalRecords = totalRecords;
        this.passedRecords = passedRecords;
        this.failedRecords = failedRecords;
        this.failedRecordsList = failedRecordsList;
        this.timestamp = timestamp;
    }
}