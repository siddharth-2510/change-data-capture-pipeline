package com.salescode.sink;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * Builder utility for creating Iceberg sinks for Order Headers and Order
 * Details.
 * Supports CDC versioning fields (entity_id, version_ts, event_type, ingest_ts,
 * is_latest).
 */
@Slf4j
public class IcebergSinkBuilder {

    /**
     * Create an Iceberg sink for Order Headers (db.orders table)
     */
    public static DataStreamSink<Void> createOrderHeaderSink(
            DataStream<ObjectNode> stream,
            TableLoader tableLoader) {

        DataStream<RowData> rowStream = stream.map(new OrderHeaderMapper());

        return FlinkSink.forRowData(rowStream)
                .tableLoader(tableLoader)
                .writeParallelism(1)
                .append();
    }

    /**
     * Create an Iceberg sink for Order Details (db.order_details table)
     */
    public static DataStreamSink<Void> createOrderDetailsSink(
            DataStream<ObjectNode> stream,
            TableLoader tableLoader) {

        DataStream<RowData> rowStream = stream.map(new OrderDetailsMapper());

        return FlinkSink.forRowData(rowStream)
                .tableLoader(tableLoader)
                .writeParallelism(1)
                .append();
    }

    // ============================================================
    // Order Headers Mapper: ObjectNode → RowData
    // Schema: 59 fields (5 versioning + 54 business)
    // ============================================================
    private static class OrderHeaderMapper implements MapFunction<ObjectNode, RowData> {

        @Override
        public RowData map(ObjectNode node) throws Exception {
            GenericRowData row = new GenericRowData(59);

            // CDC Versioning Fields (indices 0-4)
            row.setField(0, toStringData(node, "entity_id")); // required
            row.setField(1, toTimestamp(node, "version_ts")); // required
            row.setField(2, toStringData(node, "event_type")); // required
            row.setField(3, toTimestamp(node, "ingest_ts")); // required
            row.setField(4, toBoolean(node, "is_latest")); // required

            // Original Business Fields (indices 5-58)
            row.setField(5, toStringData(node, "id")); // required
            row.setField(6, toStringData(node, "active_status"));
            row.setField(7, toStringData(node, "active_status_reason"));
            row.setField(8, toStringData(node, "created_by"));
            row.setField(9, toTimestamp(node, "creation_time"));
            row.setField(10, toStringData(node, "extended_attributes"));
            row.setField(11, null); // hash
            row.setField(12, toTimestamp(node, "last_modified_time"));
            row.setField(13, toStringData(node, "lob"));
            row.setField(14, toStringData(node, "modified_by"));
            row.setField(15, null); // source
            row.setField(16, null); // version
            row.setField(17, toTimestamp(node, "system_time"));
            row.setField(18, toDouble(node, "bill_amount"));
            row.setField(19, toStringData(node, "channel"));
            row.setField(20, toStringData(node, "discount_info"));
            row.setField(21, null); // gps_latitude
            row.setField(22, null); // gps_longitude
            row.setField(23, null); // user_hierarchy
            row.setField(24, toInt(node, "line_count"));
            row.setField(25, null); // loginid
            row.setField(26, toDouble(node, "net_amount"));
            row.setField(27, toFloat(node, "normalized_quantity"));
            row.setField(28, toStringData(node, "order_number"));
            row.setField(29, toStringData(node, "reference_number"));
            row.setField(30, toStringData(node, "remarks"));
            row.setField(31, toStringData(node, "ship_id"));
            row.setField(32, toDouble(node, "total_amount"));
            row.setField(33, toDouble(node, "total_initial_amt"));
            row.setField(34, toFloat(node, "total_initial_quantity"));
            row.setField(35, toDouble(node, "total_mrp"));
            row.setField(36, toFloat(node, "total_quantity"));
            row.setField(37, null); // type
            row.setField(38, null); // delivery_date
            row.setField(39, toStringData(node, "status"));
            row.setField(40, toStringData(node, "location_hierarchy"));
            row.setField(41, toStringData(node, "outletcode"));
            row.setField(42, toStringData(node, "supplierid"));
            row.setField(43, toStringData(node, "hierarchy"));
            row.setField(44, toStringData(node, "status_reason"));
            row.setField(45, null); // changed
            row.setField(46, toStringData(node, "group_id"));
            row.setField(47, toStringData(node, "beat"));
            row.setField(48, toStringData(node, "beat_name"));
            row.setField(49, toFloat(node, "initial_normalized_quantity"));
            row.setField(50, toFloat(node, "normalized_volume"));
            row.setField(51, toStringData(node, "processing_status"));
            row.setField(52, null); // sales_date
            row.setField(53, toDouble(node, "sales_value"));
            row.setField(54, toStringData(node, "sub_type"));
            row.setField(55, toBoolean(node, "in_beat"));
            row.setField(56, toBoolean(node, "in_range"));
            row.setField(57, toDouble(node, "nw"));
            row.setField(58, null); // reference_order_number

            return row;
        }
    }

    // ============================================================
    // Order Details Mapper: ObjectNode → RowData
    // Schema: 61 fields (5 versioning + 56 business)
    // ============================================================
    private static class OrderDetailsMapper implements MapFunction<ObjectNode, RowData> {

        @Override
        public RowData map(ObjectNode node) throws Exception {
            GenericRowData row = new GenericRowData(61);

            // CDC Versioning Fields (indices 0-4)
            row.setField(0, toStringData(node, "entity_id")); // required
            row.setField(1, toTimestamp(node, "version_ts")); // required
            row.setField(2, toStringData(node, "event_type")); // required
            row.setField(3, toTimestamp(node, "ingest_ts")); // required
            row.setField(4, toBoolean(node, "is_latest")); // required

            // Original Business Fields (indices 5-60)
            row.setField(5, toStringData(node, "id")); // required
            row.setField(6, null); // active_status
            row.setField(7, null); // active_status_reason
            row.setField(8, null); // created_by
            row.setField(9, null); // creation_time
            row.setField(10, null); // extended_attributes
            row.setField(11, null); // hash
            row.setField(12, null); // last_modified_time
            row.setField(13, null); // lob
            row.setField(14, null); // modified_by
            row.setField(15, null); // source
            row.setField(16, null); // version
            row.setField(17, null); // system_time
            row.setField(18, toStringData(node, "batch_code"));
            row.setField(19, toDouble(node, "bill_amount"));
            row.setField(20, toFloat(node, "case_quantity"));
            row.setField(21, null); // color
            row.setField(22, null); // discount_number
            row.setField(23, toStringData(node, "discount_info"));
            row.setField(24, toDouble(node, "initial_amount"));
            row.setField(25, toFloat(node, "initial_case_quantity"));
            row.setField(26, toFloat(node, "initial_other_unit_quantity"));
            row.setField(27, toFloat(node, "initial_piece_quantity"));
            row.setField(28, toFloat(node, "initial_quantity"));
            row.setField(29, toDouble(node, "mrp"));
            row.setField(30, null); // name
            row.setField(31, toDouble(node, "net_amount"));
            row.setField(32, toFloat(node, "normalized_quantity"));
            row.setField(33, null); // other_unit_quantity
            row.setField(34, null); // piece_quantity
            row.setField(35, toStringData(node, "product_info"));
            row.setField(36, null); // product_key
            row.setField(37, null); // quantity_unit
            row.setField(38, null); // size
            row.setField(39, toStringData(node, "skucode"));
            row.setField(40, toStringData(node, "status"));
            row.setField(41, null); // type
            row.setField(42, null); // unit_of_measurement
            row.setField(43, toFloat(node, "price"));
            row.setField(44, toStringData(node, "location_hierarchy"));
            row.setField(45, toStringData(node, "hierarchy"));
            row.setField(46, toStringData(node, "order_id"));
            row.setField(47, toStringData(node, "status_reason"));
            row.setField(48, null); // changed
            row.setField(49, null); // gps_latitude
            row.setField(50, null); // gps_longitude
            row.setField(51, toFloat(node, "initial_normalized_quantity"));
            row.setField(52, null); // line_count
            row.setField(53, toFloat(node, "normalized_volume"));
            row.setField(54, toFloat(node, "case_price"));
            row.setField(55, null); // batch_ids
            row.setField(56, toFloat(node, "other_unit_price"));
            row.setField(57, toFloat(node, "sales_quantity"));
            row.setField(58, toDouble(node, "sales_value"));
            row.setField(59, toDouble(node, "nw"));
            row.setField(60, null); // site_id

            return row;
        }
    }

    // ============================================================
    // Type Conversion Helpers
    // ============================================================

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

    /**
     * Convert timestamp to TimestampData.
     * Supports:
     * - Long epoch millis (from transformers)
     * - ISO-8601 string format
     * - Custom 'yyyy-MM-dd HH:mm:ss' format
     */
    private static TimestampData toTimestamp(ObjectNode node, String field) {
        if (node.has(field) && !node.get(field).isNull()) {
            // Handle Long epoch millis (from transformers)
            if (node.get(field).isNumber()) {
                long epochMillis = node.get(field).asLong();
                return TimestampData.fromEpochMillis(epochMillis);
            }

            // Handle String timestamp formats
            String value = node.get(field).asText();
            try {
                // Try parsing as ISO-8601 with timezone
                Instant instant = Instant.parse(value);
                return TimestampData.fromInstant(instant);
            } catch (DateTimeParseException e) {
                try {
                    // Try custom format 'yyyy-MM-dd HH:mm:ss'
                    java.time.LocalDateTime ldt = java.time.LocalDateTime.parse(value,
                            java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                    return TimestampData.fromInstant(ldt.toInstant(java.time.ZoneOffset.UTC));
                } catch (DateTimeParseException e2) {
                    try {
                        // Try parsing as OffsetDateTime
                        OffsetDateTime odt = OffsetDateTime.parse(value);
                        return TimestampData.fromInstant(odt.toInstant());
                    } catch (DateTimeParseException e3) {
                        log.warn("Failed to parse timestamp field '{}' with value '{}': {}",
                                field, value, e3.getMessage());
                        // Return current time as fallback for required fields
                        return TimestampData.fromEpochMillis(System.currentTimeMillis());
                    }
                }
            }
        }
        // Return current time as fallback for required fields
        return TimestampData.fromEpochMillis(System.currentTimeMillis());
    }
}
