package com.salescode.models;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public  class BatchAccumulator implements Serializable {
    private static final long serialVersionUID = 1L;

    public long totalRecords = 0;
    public long passedRecords = 0;
    public long failedRecords = 0;
    public List<Map<String, Object>> failedRecordsList = new ArrayList<>();
}