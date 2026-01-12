package com.salescode.config;

import com.salescode.models.BatchAccumulator;
import com.salescode.models.BatchSummary;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.io.Serializable;
import java.util.Map;

public class CountAggregateFunction implements AggregateFunction<Map<String, Object>, BatchAccumulator, BatchSummary>, Serializable {

    private static final long serialVersionUID = 1L;

    @Override
    public BatchAccumulator createAccumulator() {
        return new BatchAccumulator();
    }

    @Override
    public BatchAccumulator add(Map<String, Object> record, BatchAccumulator accumulator) {
        accumulator.totalRecords++;

        // Check if record transformation was successful
        // You can add your own logic here to determine if a record "failed"
        // For now, assume all records pass unless they're null or empty
        if (record == null || record.isEmpty()) {
            accumulator.failedRecords++;
            accumulator.failedRecordsList.add(record);
        } else {
            accumulator.passedRecords++;
        }

        return accumulator;
    }

    @Override
    public BatchSummary getResult(BatchAccumulator accumulator) {
        return new BatchSummary(
                accumulator.totalRecords,
                accumulator.passedRecords,
                accumulator.failedRecords,
                accumulator.failedRecordsList,
                System.currentTimeMillis()
        );
    }

    @Override
    public BatchAccumulator merge(BatchAccumulator a, BatchAccumulator b) {
        a.totalRecords += b.totalRecords;
        a.passedRecords += b.passedRecords;
        a.failedRecords += b.failedRecords;
        a.failedRecordsList.addAll(b.failedRecordsList);
        return a;
    }
}
