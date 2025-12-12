package com.salescode.kafka;

import com.salescode.config.CountAggregateFunction;
import com.salescode.models.BatchSummary;
import com.salescode.models.EntityConfig;
import com.salescode.models.FieldConfig;
import com.salescode.sink.IcebergSink;
import com.salescode.transformer.MainTransformer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class KafkaConsumer {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final IcebergSink sink;
    private final List<FieldConfig> fieldConfigs;

    public KafkaConsumer(String entityName, List<FieldConfig> fieldConfig) {
        this.sink = new IcebergSink(100);
        this.fieldConfigs = fieldConfig;
    }

    public StreamExecutionEnvironment read(EntityConfig config) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(300000);

        DataStream<ObjectNode> kafkaStream = env.fromSource(
                KafkaSourceProvider.createKafkaSource(
                        config.getKafkaBroker(),
                        config.getKafkaTopic(),
                        config.getGroupId()
                ),
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );

        DataStream<Map<String, Object>> processedStream = kafkaStream
                .map(objectNode -> {
                    // full Kafka message as map
                    Map<String, Object> root = OBJECT_MAPPER.convertValue(objectNode, Map.class);

                    // Extract features[0]
                    Object featuresObj = root.get("features");

                    if (featuresObj instanceof List) {
                        List<?> featuresList = (List<?>) featuresObj;

                        if (!featuresList.isEmpty() && featuresList.get(0) instanceof Map) {
                            return (Map<String, Object>) featuresList.get(0);  // <-- ACTUAL ORDER
                        }
                    }

                    // fallback (no features or invalid)
                    return root;
                })
                .returns(TypeInformation.of(new TypeHint<Map<String, Object>>() {}))
                .name("Extract features[0]")
                .map(new TransformFunction(fieldConfigs))
                .name("Transform Fields");

        // WINDOWING
        AllWindowedStream<Map<String, Object>, TimeWindow> windowedStream = processedStream
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        DataStream<BatchSummary> batchResults = windowedStream.aggregate(new CountAggregateFunction());

        batchResults.print();

        // WRITE TO ICEBERG
        processedStream.addSink(sink)
                .name("Iceberg Sink");

        return env;
    }


    /**
     * Static inner class for field transformation
     */
    private static class TransformFunction implements MapFunction<Map<String, Object>, Map<String, Object>>, Serializable {

        private static final long serialVersionUID = 1L;

        private final List<FieldConfig> fieldConfigs;
        private transient MainTransformer transformer;

        public TransformFunction(List<FieldConfig> fieldConfigs) {
            this.fieldConfigs = fieldConfigs;
        }

        @Override
        public Map<String, Object> map(Map<String, Object> input) throws Exception {
            if (transformer == null) {
                transformer = new MainTransformer();
            }
            return transformer.transform(fieldConfigs, input);
        }
    }
}