package com.salescode.mapper;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.iceberg.flink.FlinkSchemaUtil;

import javax.xml.validation.Schema;
import java.util.Map;

public class ObjectNodeToRowData implements MapFunction<ObjectNode, RowData> {

    private final Schema schema;
    private final RowType rowType;

    public ObjectNodeToRowData(Schema schema) {
        this.schema = schema;
        this.rowType = FlinkSchemaUtil.convert(schema);
    }

    @Override
    public RowData map(ObjectNode json) throws Exception {

        Map<String, Object> map =
                new ObjectMapper().convertValue(json, new TypeReference<Map<String, Object>>() {});

        return FlinkRowDataConverter.convertMapToRowData(map, rowType);
    }
}
