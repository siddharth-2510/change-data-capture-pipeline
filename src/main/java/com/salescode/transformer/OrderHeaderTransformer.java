package com.salescode.transformer;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.util.Collector;

@Slf4j
public class OrderHeaderTransformer implements FlatMapFunction<ObjectNode, ObjectNode> {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void flatMap(ObjectNode event, Collector<ObjectNode> out) throws Exception {

        // features list contains all orders inside this message
        ArrayNode features = (ArrayNode) event.get("features");

        if (features == null) {
            log.warn("No features[] list found in message!");
            return;
        }

        for (int i = 0; i < features.size(); i++) {

            ObjectNode feature = (ObjectNode) features.get(i);
            ObjectNode row = mapper.createObjectNode();

            // --- REQUIRED CK_ORDERS FIELDS ---
            row.put("id", text(feature, "id"));
            row.put("lob", text(feature, "lob"));
            row.put("active_status", text(feature, "activeStatus"));
            row.put("active_status_reason", text(feature, "activeStatusReason"));

            row.put("created_by", text(feature, "createdBy"));
            row.put("modified_by", text(feature, "modifiedBy"));
            row.put("creation_time", text(feature, "creationTime"));
            row.put("last_modified_time", text(feature, "lastModifiedTime"));
            row.put("system_time", text(feature, "systemTime"));

            row.put("order_number", text(feature, "orderNumber"));
            row.put("reference_number", text(feature, "referenceNumber"));
            row.put("ship_id", text(feature, "shipId"));
            row.put("remarks", text(feature, "remarks"));

            row.put("bill_amount", dbl(feature, "billAmount"));
            row.put("net_amount", dbl(feature, "netAmount"));
            row.put("total_amount", dbl(feature, "totalAmount"));
            row.put("total_initial_amt", dbl(feature, "totalInitialAmt"));
            row.put("total_initial_quantity", dbl(feature, "totalInitialQuantity"));
            row.put("total_mrp", dbl(feature, "totalMrp"));
            row.put("total_quantity", dbl(feature, "totalQuantity"));
            row.put("normalized_quantity", dbl(feature, "normalizedQuantity"));
            row.put("initial_normalized_quantity", dbl(feature, "initialNormalizedQuantity"));
            row.put("normalized_volume", dbl(feature, "normalizedVolume"));

            row.put("line_count", intVal(feature, "lineCount"));

            row.put("outletcode", text(feature, "outletCode"));
            row.put("supplierid", text(feature, "supplier"));
            row.put("hierarchy", text(feature, "hierarchy"));
            row.put("location_hierarchy", text(feature, "locationHierarchy"));

            row.put("channel", text(feature, "channel"));
            row.put("status", text(feature, "status"));
            row.put("status_reason", text(feature, "statusReason"));

            row.put("beat", text(feature, "beat"));
            row.put("beat_name", text(feature, "beatName"));

            row.put("in_beat", bool(feature, "inBeat"));
            row.put("in_range", bool(feature, "inRange"));

            row.put("nw", dbl(feature, "nw"));
            row.put("sales_value", dbl(feature, "salesValue"));
            row.put("sub_type", text(feature, "subType"));
            row.put("processing_status", text(feature, "processingStatus"));
            row.put("group_id", text(feature, "groupId"));

            // extendedAttributes → store as JSON string
            row.put("extended_attributes", feature.get("extendedAttributes") != null ?
                    feature.get("extendedAttributes").toString() : null);

            // discountInfo[] exists → store full JSON
            row.put("discount_info", feature.get("discountInfo") != null ?
                    feature.get("discountInfo").toString() : null);

            out.collect(row);
        }
    }

    private String text(ObjectNode n, String f) {
        return n.has(f) && !n.get(f).isNull() ? n.get(f).asText() : null;
    }

    private double dbl(ObjectNode n, String f) {
        return n.has(f) && n.get(f).isNumber() ? n.get(f).asDouble() : 0.0;
    }

    private int intVal(ObjectNode n, String f) {
        return n.has(f) && n.get(f).isInt() ? n.get(f).asInt() : 0;
    }

    private boolean bool(ObjectNode n, String f) {
        return n.has(f) && n.get(f).isBoolean() && n.get(f).asBoolean();
    }
}
