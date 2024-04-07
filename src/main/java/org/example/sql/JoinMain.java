package org.example.sql;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.model.PreTxData;
import org.example.util.BoundedOutOfOrdernessStrategy;
import org.example.util.FlinkUtil;
import org.example.util.PreTxDataParserRichFlatMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import com.google.gson.Gson;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.*;

public class JoinMain {
    private static final Logger logger = LoggerFactory.getLogger(JoinMain.class);

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration.setString("table.exec.state.ttl", "0" );

        FlinkUtil.initEnvironment(env);

        KafkaSource<String> preSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setGroupId("test")
                .setTopics("test")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        KafkaSource<String> postSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setGroupId("test")
                .setTopics("test2")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();


        DataStream<String> preStream = env
                .fromSource(preSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

        DataStream<String> postStream = env
                .fromSource(postSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

        WatermarkStrategy<PreTxData> wm1 = new BoundedOutOfOrdernessStrategy<>(0L);
        WatermarkStrategy<PreTxData> wm2 = new BoundedOutOfOrdernessStrategy<>(0L);

        DataStream<PreTxData> preDataStream = preStream
                .flatMap(new PreTxDataParserRichFlatMap())
                .assignTimestampsAndWatermarks(
                        wm1.withTimestampAssigner((event, timestamp) -> event.createAt)
                                .withIdleness(Duration.ofSeconds(1))
        );

        DataStream<PreTxData> postDataStream = postStream
                .flatMap(new PreTxDataParserRichFlatMap())
                .assignTimestampsAndWatermarks(
                        wm2.withTimestampAssigner((event, timestamp) -> event.createAt)
                                .withIdleness(Duration.ofSeconds(1))
        );

        tableEnv.createTemporaryView("pre", preDataStream,
                $("paymentIntentId"),
                $("paymentAttemptId"),
                $("deviceIp"),
                $("createAt"),
                $("createAt").rowtime().as("event_time"),
                $("num"));

        tableEnv.createTemporaryView("post", postDataStream,
                $("paymentIntentId"),
                $("paymentAttemptId"),
                $("deviceIp"),
                $("createAt"),
                $("createAt").rowtime().as("event_time"),
                $("num"));

        String regularJoin =
                " WITH t1 AS (SELECT " +
                        " pre.paymentIntentId AS id, pre.createAt AS create_time, pre.event_time AS event_time, post.num AS num" +
                        " FROM pre LEFT JOIN post ON pre.deviceIp = post.deviceIp" +
                        " WHERE post.createAt < pre.createAt AND post.createAt >= pre.createAt - 1000*60) " +
                        " SELECT id, SUM(num) FROM t1 GROUP BY id";

        String intervalJoin =
                " WITH t1 AS (SELECT " +
                        " pre.paymentIntentId AS id, pre.createAt AS create_time, pre.event_time AS event_time, post.num AS num" +
                        " FROM pre LEFT JOIN post ON pre.deviceIp = post.deviceIp WHERE " +
                        " post.event_time BETWEEN pre.event_time - INTERVAL '4' HOUR AND pre.event_time) " +
                        " SELECT id, SUM(num) FROM t1 GROUP BY id";

        Table regularTable = tableEnv.sqlQuery(regularJoin);
        Table intervalTable = tableEnv.sqlQuery(intervalJoin);

        DataStream<Row> regularDS = tableEnv.toChangelogStream(regularTable);
        DataStream<Row> intervalDS = tableEnv.toChangelogStream(intervalTable);

        regularDS.print();
        intervalDS.print();
        env.execute("JoinFlink");
    }
}
