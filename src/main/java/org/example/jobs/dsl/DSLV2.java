package org.example.jobs.dsl;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.example.config.ComputeConf;
import org.example.config.FeatureConf;
import org.example.config.OutputConf;
import org.example.model.PrePostTxData;
import org.example.util.BoundedOutOfOrdernessStrategy;
import org.example.util.FlinkUtil;
import org.example.util.PrePostTxDataParserRichFlatMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DSLV2 {
    private static final Logger logger = LoggerFactory.getLogger(DSLV2.class);

    // define function logic
    public static class toUpperCase extends ScalarFunction {
        public String eval(String ipAddress, String xxx) {
            if (ipAddress == null || ipAddress.isEmpty()) return "";
            return ipAddress.toUpperCase();
        }
    }

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration.setString("table.exec.state.ttl", "0" );

        FlinkUtil.initEnvironment(env);
        env.setParallelism(2);
        tableEnv.createTemporarySystemFunction("toUpperCase", toUpperCase.class);

        KafkaSource<String> preSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setGroupId("test")
                .setTopics("test3")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        KafkaSource<String> postSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setGroupId("test")
                .setTopics("test4")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> preStream = env
                .fromSource(preSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

        DataStream<String> postStream = env
                .fromSource(postSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

        WatermarkStrategy<PrePostTxData> wm1 = new BoundedOutOfOrdernessStrategy<>("preDataStreamWM",0L);
        WatermarkStrategy<PrePostTxData> wm2 = new BoundedOutOfOrdernessStrategy<>("postDataStreamWM",0L);

        DataStream<PrePostTxData> preDataStream = preStream
                .flatMap(new PrePostTxDataParserRichFlatMap())
                .assignTimestampsAndWatermarks(
                        wm1.withTimestampAssigner((event, timestamp) -> event.createAt)
                                .withIdleness(Duration.ofMillis(300))
                );

        DataStream<PrePostTxData> postDataStream = postStream
                .flatMap(new PrePostTxDataParserRichFlatMap())
                .assignTimestampsAndWatermarks(
                        wm2.withTimestampAssigner((event, timestamp) -> event.createAt)
                                .withIdleness(Duration.ofMillis(300))
                );

        tableEnv.createTemporaryView(
                "preFraudTxSource",
                preDataStream,
                Schema.newBuilder()
                        .columnByMetadata("rowtime", "TIMESTAMP_LTZ(3)")
                        .watermark("rowtime", "SOURCE_WATERMARK()")
                        .build());

        tableEnv.from("preFraudTxSource").printSchema();

        tableEnv.createTemporaryView(
                "postFraudTxSource",
                postDataStream,
                Schema.newBuilder()
                        .columnByMetadata("rowtime", "TIMESTAMP_LTZ(3)")
                        .watermark("rowtime", "SOURCE_WATERMARK()")
                        .build());

        tableEnv.from("postFraudTxSource").printSchema();

        /**
         *  The previous codes is designed to provide user with two original tables, preFraudTxSource and postFraudTxSource
         *  user can use these two tables as source tables and generate their own tables
         */

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        InputStream in = DSLV2.class.getClassLoader().getResourceAsStream("feature_example.yaml");
        FeatureConf conf;
        if(in != null) {
            conf = mapper.readValue(in, FeatureConf.class);
        }else {
            conf = mapper.readValue(new File("src/main/resources/feature_example.yaml"), FeatureConf.class);
        }

        FlinkUtil.logInfo(conf.toString());

        List<ComputeConf> computes = conf.compute;
        List<OutputConf> outputs = conf.output;
        for(ComputeConf compute : computes) {
            if(compute.type.equals("createTable")) {
                Table tmp = tableEnv.sqlQuery(compute.sql);
                tableEnv.createTemporaryView(compute.name, tmp);
                tableEnv.from(compute.name).printSchema();
                //tableEnv.toDataStream(tmp).print();
            }
            if(compute.type.equals("union")) {
                Table tmp = tableEnv.sqlQuery(
                        "(SELECT rowtime as eventime, " + compute.columns + " FROM mapLeft ) UNION ALL " +
                        "(SELECT rowtime as eventime, " + compute.columns + " FROM mapRight);");
                WatermarkStrategy<Row> wm = new BoundedOutOfOrdernessStrategy<>(compute.name,0L);
                DataStream<Row> result = tableEnv
                        .toDataStream(tmp)
                        .assignTimestampsAndWatermarks(
                                wm.withTimestampAssigner((row, timestamp) -> {
                                    Instant instant = (Instant)row.getField("eventime");
                                    return instant.toEpochMilli();
                                }).withIdleness(Duration.ofMillis(200))
                        );
                tableEnv.createTemporaryView(
                        compute.name,
                        result,
                        Schema.newBuilder()
                                .columnByMetadata("rowtime", "TIMESTAMP_LTZ(3)")
                                .watermark("rowtime", "SOURCE_WATERMARK()")
                                .build());
                tableEnv.from(compute.name).printSchema();
//                tableEnv.toDataStream(tmp).print();
            }
        }
        for(OutputConf output : outputs) {
            Table tmp = tableEnv.sqlQuery(output.sql);
            tableEnv.createTemporaryView("output", tmp);
            tableEnv.toDataStream(tmp).print();
            tableEnv.dropTemporaryView("output");
        }


        env.execute("DSLV2");
    }
}

