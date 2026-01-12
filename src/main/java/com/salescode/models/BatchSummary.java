package com.salescode.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@AllArgsConstructor
@Data
public  class BatchSummary implements Serializable {
    private static final long serialVersionUID = 1L;

    final long totalRecords;
    final long passedRecords;
    final long failedRecords;
    final List<Map<String, Object>> failedRecordsList;
    final long timestamp;


}