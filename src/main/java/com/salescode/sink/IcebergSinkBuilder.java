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
 * Builder utility for creating Iceberg sink for Orders.
 * Supports CDC versioning fields and UPSERT mode for deduplication.
 */
@Slf4j
public class IcebergSinkBuilder {

    /**
     * Create an Iceberg sink for Orders (iceberg_db_test.ck_orders table in AWS
     * Glue)
     * UPSERT MODE: Uses (entity_id, version_ts) as composite primary key
     * - Same entity_id + same version_ts = duplicate (last write wins)
     * - Same entity_id + different version_ts = new version (both kept)
     */
    public static DataStreamSink<Void> createOrderSink(
            DataStream<ObjectNode> stream,
            TableLoader tableLoader) {

        DataStream<RowData> rowStream = stream.map(new OrderMapper());

        return FlinkSink.forRowData(rowStream)
                .tableLoader(tableLoader)
                .writeParallelism(1)
                // Primary key for deduplication: entity_id + version_ts
                // Same entity_id with different version_ts = new version (kept)
                // Same entity_id with same version_ts = duplicate (replaced)
                .equalityFieldColumns(java.util.Arrays.asList("entity_id", "version_ts"))
                // Enable upsert mode: dedup based on primary key
                .upsert(true)
                .append();
    }

    // ============================================================
    // Order Mapper: ObjectNode → RowData
    // Schema: 60 fields total (5 CDC versioning + 55 business fields)
    // ============================================================
    private static class OrderMapper implements MapFunction<ObjectNode, RowData> {

        @Override
        public RowData map(ObjectNode node) throws Exception {
            GenericRowData row = new GenericRowData(60);

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
            row.setField(58, null);

            row.setField(59, toStringData(node, "order_details"));

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
