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

/**
 * Builder utility for creating Iceberg sinks for Order Headers.
 * Maps JSON data to RowData matching the Athena-created Iceberg table schema.
 */
@Slf4j
public class IcebergSinkBuilder {

    /**
     * Create an Iceberg sink for Order Headers (iceberg_db_test.ck_orders table)
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
     * Order Headers Mapper: ObjectNode → RowData
     * Schema matches the Athena-created table with 52 fields:
     * entity_id, version_ts, event_type, ingest_ts, is_latest, id, order_number,
     * reference_number, lob, source, channel, created_by, modified_by,
     * creation_time,
     * last_modified_time, system_time, outletcode, supplierid, loginid, hierarchy,
     * location_hierarchy, supplier_hierarchy, status, status_reason,
     * processing_status,
     * type, sub_type, line_count, version, bill_amount, net_amount, total_amount,
     * total_initial_amt, total_mrp, total_quantity, total_initial_quantity,
     * normalized_quantity, initial_normalized_quantity, nw, normalized_volume,
     * sales_value, sales_date, gps_latitude, gps_longitude, beat, beat_name,
     * group_id, remarks, extended_attributes, discount_info, order_details,
     * ingestion_time
     */
    private static class OrderHeaderMapper implements MapFunction<ObjectNode, RowData> {

        @Override
        public RowData map(ObjectNode node) throws Exception {
            GenericRowData row = new GenericRowData(52);

            // Fields 0-5: CDC versioning + id
            row.setField(0, toStringData(node, "entity_id")); // entity_id
            row.setField(1, toTimestamp(node, "version_ts")); // version_ts
            row.setField(2, toStringData(node, "event_type")); // event_type
            row.setField(3, toTimestamp(node, "ingest_ts")); // ingest_ts
            row.setField(4, toBoolean(node, "is_latest")); // is_latest
            row.setField(5, toStringData(node, "id")); // id

            // Fields 6-12: Order identifiers and metadata
            row.setField(6, toStringData(node, "order_number")); // order_number
            row.setField(7, toStringData(node, "reference_number")); // reference_number
            row.setField(8, toStringData(node, "lob")); // lob
            row.setField(9, null); // source
            row.setField(10, toStringData(node, "channel")); // channel
            row.setField(11, toStringData(node, "created_by")); // created_by
            row.setField(12, toStringData(node, "modified_by")); // modified_by

            // Fields 13-15: Timestamps
            row.setField(13, toTimestamp(node, "creation_time")); // creation_time
            row.setField(14, toTimestamp(node, "last_modified_time")); // last_modified_time
            row.setField(15, toTimestamp(node, "system_time")); // system_time

            // Fields 16-21: Location and hierarchy
            row.setField(16, toStringData(node, "outletcode")); // outletcode
            row.setField(17, toStringData(node, "supplierid")); // supplierid
            row.setField(18, null); // loginid
            row.setField(19, toStringData(node, "hierarchy")); // hierarchy
            row.setField(20, toStringData(node, "location_hierarchy")); // location_hierarchy
            row.setField(21, null); // supplier_hierarchy

            // Fields 22-26: Status and type
            row.setField(22, toStringData(node, "status")); // status
            row.setField(23, toStringData(node, "status_reason")); // status_reason
            row.setField(24, toStringData(node, "processing_status")); // processing_status
            row.setField(25, null); // type
            row.setField(26, toStringData(node, "sub_type")); // sub_type

            // Fields 27-28: Counts
            row.setField(27, toInt(node, "line_count")); // line_count
            row.setField(28, null); // version

            // Fields 29-40: Amounts and quantities
            row.setField(29, toDouble(node, "bill_amount")); // bill_amount
            row.setField(30, toDouble(node, "net_amount")); // net_amount
            row.setField(31, toDouble(node, "total_amount")); // total_amount
            row.setField(32, toDouble(node, "total_initial_amt")); // total_initial_amt
            row.setField(33, toDouble(node, "total_mrp")); // total_mrp
            row.setField(34, toDouble(node, "total_quantity")); // total_quantity
            row.setField(35, toDouble(node, "total_initial_quantity")); // total_initial_quantity
            row.setField(36, toDouble(node, "normalized_quantity")); // normalized_quantity
            row.setField(37, toDouble(node, "initial_normalized_quantity")); // initial_normalized_quantity
            row.setField(38, toDouble(node, "nw")); // nw
            row.setField(39, toDouble(node, "normalized_volume")); // normalized_volume
            row.setField(40, toDouble(node, "sales_value")); // sales_value

            // Field 41: sales_date (timestamp)
            row.setField(41, null); // sales_date

            // Fields 42-47: GPS and beat info
            row.setField(42, null); // gps_latitude
            row.setField(43, null); // gps_longitude
            row.setField(44, toStringData(node, "beat")); // beat
            row.setField(45, toStringData(node, "beat_name")); // beat_name
            row.setField(46, toStringData(node, "group_id")); // group_id
            row.setField(47, toStringData(node, "remarks")); // remarks

            // Fields 48-50: JSON fields
            row.setField(48, toStringData(node, "extended_attributes")); // extended_attributes
            row.setField(49, toStringData(node, "discount_info")); // discount_info
            row.setField(50, toStringData(node, "order_details")); // order_details

            // Field 51: ingestion_time
            row.setField(51, TimestampData.fromEpochMillis(System.currentTimeMillis())); // ingestion_time

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
     * Supports Long epoch millis and various string formats.
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
                Instant instant = Instant.parse(value);
                return TimestampData.fromInstant(instant);
            } catch (Exception e) {
                try {
                    java.time.LocalDateTime ldt = java.time.LocalDateTime.parse(value,
                            java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                    return TimestampData.fromInstant(ldt.toInstant(java.time.ZoneOffset.UTC));
                } catch (Exception e2) {
                    try {
                        OffsetDateTime odt = OffsetDateTime.parse(value);
                        return TimestampData.fromInstant(odt.toInstant());
                    } catch (Exception e3) {
                        log.warn("Failed to parse timestamp field '{}': {}", field, value);
                        return null;
                    }
                }
            }
        }
        return null;
    }
}
