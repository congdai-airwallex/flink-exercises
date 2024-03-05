package org.example.sql;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.example.model.PreTxData;
import org.example.util.BoundedOutOfOrdernessStrategy;
import org.example.util.FlinkUtil;
import org.example.util.PreTxDataParserRichFlatMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import com.google.gson.Gson;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.*;

public class OverPartitionMain {
    private static final Logger logger = LoggerFactory.getLogger(OverPartitionMain.class);

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration.setString("table.exec.state.ttl", "0" );

        FlinkUtil.initEnvironment(env);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setGroupId("test")
                .setTopics("test")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> sourceStream = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        WatermarkStrategy<PreTxData> wm = new BoundedOutOfOrdernessStrategy<>(0L);

        DataStream<PreTxData> eventDataStream = sourceStream
                .flatMap(new PreTxDataParserRichFlatMap())
                .assignTimestampsAndWatermarks(
            wm.withTimestampAssigner((event, timestamp) -> event.createAt).withIdleness(Duration.ofSeconds(1))
        );

        tableEnv.createTemporaryView("events", eventDataStream,
                $("paymentIntentId"),
                $("paymentAttemptId"),
                $("deviceIp"),
                $("createAt"),
                $("createAt").rowtime().as("event_time"),
                $("num"));

        String sql =
                " SELECT" +
                "   paymentIntentId, deviceIp, num, createAt, event_time " +
                "   ,sum(num) OVER (" +
                "       PARTITION BY paymentIntentId, deviceIp " +
                "       ORDER BY event_time " +
                "       RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW " +
                "     ) AS sum_num " +
                "   FROM events ";

        Table table = tableEnv.sqlQuery(sql);

        DataStream<Row> dataStream = tableEnv.toDataStream(table);

        dataStream.print();
        env.execute("OverPartition");
    }
}