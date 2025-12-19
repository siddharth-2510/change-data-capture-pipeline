package com.salescode;

import com.salescode.cdc.config.ConfigReader;
import com.salescode.cdc.kafka.KafkaConsumer;
import com.salescode.cdc.models.RootConfig;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.concurrent.TimeUnit;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) {
        ConfigReader configReader  = ConfigReader.getInstance();
        RootConfig rootConfig = configReader.loadConfig();
        List<com.salescode.cdc.models.EntityConfig> entityConfigList = rootConfig.getEntities();
        for(com.salescode.cdc.models.EntityConfig config:entityConfigList) {
            KafkaConsumer consumer = new KafkaConsumer(config.getEntityName(),config.getFields());
            StreamExecutionEnvironment environment = consumer.read(config);
            try {
                JobClient client = environment.executeAsync();
                long millis = config.getRuntimeMinutes() * 60 * 1000;
                try {
                    var accumulators = client.getAccumulators().get(5, TimeUnit.SECONDS);
                    System.out.println("   Accumulators: " + accumulators);
                } catch (Exception e) {
                    System.out.println("   Accumulators: Not available");
                }
                Thread.sleep(millis);
                try {
                    var accumulators = client.getAccumulators().get(5, TimeUnit.SECONDS);
                    System.out.println("   Accumulators: " + accumulators);
                } catch (Exception e) {
                    System.out.println("   Accumulators: Not available");
                }
                client.cancel().get();  // Cancel and wait

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }


}