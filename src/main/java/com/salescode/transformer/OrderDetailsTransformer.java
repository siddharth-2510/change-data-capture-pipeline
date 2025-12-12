package com.salescode.transformer;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.util.Collector;

@Slf4j
public class OrderDetailsTransformer implements FlatMapFunction<ObjectNode, ObjectNode> {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void flatMap(ObjectNode event, Collector<ObjectNode> out) throws Exception {

        ArrayNode features = (ArrayNode) event.get("features");
        if (features == null) return;

        for (int i = 0; i < features.size(); i++) {

            ObjectNode feature = (ObjectNode) features.get(i);

            ArrayNode items = (ArrayNode) feature.get("orderDetails");
            if (items == null) continue;

            for (int j = 0; j < items.size(); j++) {

                ObjectNode item = (ObjectNode) items.get(j);
                ObjectNode row = mapper.createObjectNode();

                row.put("id", text(item, "id"));
                row.put("order_id", text(feature, "id")); // FK → order header

                row.put("skucode", text(item, "skuCode"));
                row.put("batch_code", text(item, "batchCode"));
                row.put("case_quantity", dbl(item, "caseQuantity"));

                row.put("initial_amount", dbl(item, "initialAmount"));
                row.put("initial_quantity", dbl(item, "initialQuantity"));
                row.put("initial_piece_quantity", dbl(item, "initialPieceQuantity"));
                row.put("initial_case_quantity", dbl(item, "initialCaseQuantity"));
                row.put("initial_other_unit_quantity", dbl(item, "initialOtherUnitQuantity"));

                row.put("mrp", dbl(item, "mrp"));
                row.put("net_amount", dbl(item, "netAmount"));
                row.put("bill_amount", dbl(item, "billAmount"));

                row.put("normalized_quantity", dbl(item, "normalizedQuantity"));
                row.put("initial_normalized_quantity", dbl(item, "initialNormalizedQuantity"));
                row.put("normalized_volume", dbl(item, "normalizedVolume"));

                row.put("price", dbl(item, "price"));
                row.put("case_price", dbl(item, "casePrice"));
                row.put("other_unit_price", dbl(item, "otherUnitPrice"));

                row.put("sales_value", dbl(item, "salesValue"));
                row.put("sales_quantity", dbl(item, "salesQuantity"));
                row.put("nw", dbl(item, "nw"));

                row.put("status", text(item, "status"));
                row.put("status_reason", text(item, "statusReason"));

                row.put("location_hierarchy", text(feature, "locationHierarchy"));
                row.put("hierarchy", text(feature, "hierarchy"));

                row.put("discount_info", item.get("discountInfo") != null ?
                        item.get("discountInfo").toString() : null);

                row.put("product_info", item.get("productInfo") != null ?
                        item.get("productInfo").toString() : null);

                out.collect(row);
            }
        }
    }

    private String text(ObjectNode n, String f) {
        return n.has(f) && !n.get(f).isNull() ? n.get(f).asText() : null;
    }

    private double dbl(ObjectNode n, String f) {
        return n.has(f) && n.get(f).isNumber() ? n.get(f).asDouble() : 0.0;
    }
}
