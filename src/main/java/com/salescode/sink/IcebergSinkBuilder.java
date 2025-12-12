package com.salescode.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.*;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;

import java.util.HashMap;
import java.util.Map;

/**
 * Builder utility for creating Iceberg sinks for Order Headers and Order
 * Details
 */
public class IcebergSinkBuilder {

    /**
     * Create an Iceberg sink for Order Headers (ck_orders table)
     */
    public static DataStreamSink<Void> createOrderHeaderSink(
            DataStream<ObjectNode> stream,
            TableLoader tableLoader) {

        // Convert ObjectNode to RowData
        DataStream<RowData> rowStream = stream.map(new OrderHeaderMapper());

        // Build Iceberg sink
        return FlinkSink.forRowData(rowStream)
                .tableLoader(tableLoader)
                .writeParallelism(1)
                .append();
    }

    /**
     * Create an Iceberg sink for Order Details (ck_order_details table)
     */
    public static DataStreamSink<Void> createOrderDetailsSink(
            DataStream<ObjectNode> stream,
            TableLoader tableLoader) {

        // Convert ObjectNode to RowData
        DataStream<RowData> rowStream = stream.map(new OrderDetailsMapper());

        // Build Iceberg sink
        return FlinkSink.forRowData(rowStream)
                .tableLoader(tableLoader)
                .writeParallelism(1)
                .append();
    }

    /**
     * Mapper for Order Headers: ObjectNode → RowData
     */
    private static class OrderHeaderMapper implements MapFunction<ObjectNode, RowData> {

        @Override
        public RowData map(ObjectNode node) throws Exception {
            GenericRowData row = new GenericRowData(54); // 54 fields in orders table

            // Map all fields according to CreateIcebergTables schema
            row.setField(0, toStringData(node, "id")); // required
            row.setField(1, toStringData(node, "active_status"));
            row.setField(2, toStringData(node, "active_status_reason"));
            row.setField(3, toStringData(node, "created_by"));
            row.setField(4, null); // creation_time - timestamp (set to null for now)
            row.setField(5, toStringData(node, "extended_attributes"));
            row.setField(6, null); // hash
            row.setField(7, null); // last_modified_time - timestamp
            row.setField(8, toStringData(node, "lob"));
            row.setField(9, toStringData(node, "modified_by"));
            row.setField(10, null); // source
            row.setField(11, null); // version
            row.setField(12, null); // system_time - timestamp
            row.setField(13, toDouble(node, "bill_amount"));
            row.setField(14, toStringData(node, "channel"));
            row.setField(15, toStringData(node, "discount_info"));
            row.setField(16, null); // gps_latitude
            row.setField(17, null); // gps_longitude
            row.setField(18, null); // user_hierarchy
            row.setField(19, toInt(node, "line_count"));
            row.setField(20, null); // loginid
            row.setField(21, toDouble(node, "net_amount"));
            row.setField(22, toFloat(node, "normalized_quantity"));
            row.setField(23, toStringData(node, "order_number"));
            row.setField(24, toStringData(node, "reference_number"));
            row.setField(25, toStringData(node, "remarks"));
            row.setField(26, toStringData(node, "ship_id"));
            row.setField(27, toDouble(node, "total_amount"));
            row.setField(28, toDouble(node, "total_initial_amt"));
            row.setField(29, toFloat(node, "total_initial_quantity"));
            row.setField(30, toDouble(node, "total_mrp"));
            row.setField(31, toFloat(node, "total_quantity"));
            row.setField(32, null); // type
            row.setField(33, null); // delivery_date - timestamp
            row.setField(34, toStringData(node, "status"));
            row.setField(35, toStringData(node, "location_hierarchy"));
            row.setField(36, toStringData(node, "outletcode"));
            row.setField(37, toStringData(node, "supplierid"));
            row.setField(38, toStringData(node, "hierarchy"));
            row.setField(39, toStringData(node, "status_reason"));
            row.setField(40, null); // changed
            row.setField(41, toStringData(node, "group_id"));
            row.setField(42, toStringData(node, "beat"));
            row.setField(43, toStringData(node, "beat_name"));
            row.setField(44, toFloat(node, "initial_normalized_quantity"));
            row.setField(45, toFloat(node, "normalized_volume"));
            row.setField(46, toStringData(node, "processing_status"));
            row.setField(47, null); // sales_date - timestamp
            row.setField(48, toDouble(node, "sales_value"));
            row.setField(49, toStringData(node, "sub_type"));
            row.setField(50, toBoolean(node, "in_beat"));
            row.setField(51, toBoolean(node, "in_range"));
            row.setField(52, toDouble(node, "nw"));
            row.setField(53, null); // reference_order_number

            return row;
        }
    }

    /**
     * Mapper for Order Details: ObjectNode → RowData
     */
    private static class OrderDetailsMapper implements MapFunction<ObjectNode, RowData> {

        @Override
        public RowData map(ObjectNode node) throws Exception {
            GenericRowData row = new GenericRowData(56); // 56 fields in order_details table

            // Map all fields according to CreateIcebergTables schema
            row.setField(0, toStringData(node, "id")); // required
            row.setField(1, null); // active_status
            row.setField(2, null); // active_status_reason
            row.setField(3, null); // created_by
            row.setField(4, null); // creation_time - timestamp
            row.setField(5, null); // extended_attributes
            row.setField(6, null); // hash
            row.setField(7, null); // last_modified_time - timestamp
            row.setField(8, null); // lob
            row.setField(9, null); // modified_by
            row.setField(10, null); // source
            row.setField(11, null); // version
            row.setField(12, null); // system_time - timestamp
            row.setField(13, toStringData(node, "batch_code"));
            row.setField(14, toDouble(node, "bill_amount"));
            row.setField(15, toFloat(node, "case_quantity"));
            row.setField(16, null); // color
            row.setField(17, null); // discount_number
            row.setField(18, toStringData(node, "discount_info"));
            row.setField(19, toDouble(node, "initial_amount"));
            row.setField(20, toFloat(node, "initial_case_quantity"));
            row.setField(21, toFloat(node, "initial_other_unit_quantity"));
            row.setField(22, toFloat(node, "initial_piece_quantity"));
            row.setField(23, toFloat(node, "initial_quantity"));
            row.setField(24, toDouble(node, "mrp"));
            row.setField(25, null); // name
            row.setField(26, toDouble(node, "net_amount"));
            row.setField(27, toFloat(node, "normalized_quantity"));
            row.setField(28, null); // other_unit_quantity
            row.setField(29, null); // piece_quantity
            row.setField(30, toStringData(node, "product_info"));
            row.setField(31, null); // product_key
            row.setField(32, null); // quantity_unit
            row.setField(33, null); // size
            row.setField(34, toStringData(node, "skucode"));
            row.setField(35, toStringData(node, "status"));
            row.setField(36, null); // type
            row.setField(37, null); // unit_of_measurement
            row.setField(38, toFloat(node, "price"));
            row.setField(39, toStringData(node, "location_hierarchy"));
            row.setField(40, toStringData(node, "hierarchy"));
            row.setField(41, toStringData(node, "order_id")); // FK to orders
            row.setField(42, toStringData(node, "status_reason"));
            row.setField(43, null); // changed
            row.setField(44, null); // gps_latitude
            row.setField(45, null); // gps_longitude
            row.setField(46, toFloat(node, "initial_normalized_quantity"));
            row.setField(47, null); // line_count
            row.setField(48, toFloat(node, "normalized_volume"));
            row.setField(49, toFloat(node, "case_price"));
            row.setField(50, null); // batch_ids
            row.setField(51, toFloat(node, "other_unit_price"));
            row.setField(52, toFloat(node, "sales_quantity"));
            row.setField(53, toDouble(node, "sales_value"));
            row.setField(54, toDouble(node, "nw"));
            row.setField(55, null); // site_id

            return row;
        }
    }

    // Helper methods for type conversion
    private static StringData toStringData(ObjectNode node, String field) {
        if (node.has(field) && !node.get(field).isNull()) {
            return StringData.fromString(node.get(field).asText());
        }
        return null;
    }

    private static Double toDouble(ObjectNode node, String field) {
        if (node.has(field) && node.get(field).isNumber()) {
            return node.get(field).asDouble();
        }
        return null;
    }

    private static Float toFloat(ObjectNode node, String field) {
        if (node.has(field) && node.get(field).isNumber()) {
            return (float) node.get(field).asDouble();
        }
        return null;
    }

    private static Integer toInt(ObjectNode node, String field) {
        if (node.has(field) && node.get(field).isInt()) {
            return node.get(field).asInt();
        }
        return null;
    }

    private static Boolean toBoolean(ObjectNode node, String field) {
        if (node.has(field) && node.get(field).isBoolean()) {
            return node.get(field).asBoolean();
        }
        return null;
    }
}
