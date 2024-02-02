package org.example.sql;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
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
import org.apache.flink.util.Collector;
import org.example.model.PreTxData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import com.google.gson.Gson;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.*;

public class OverPartitionMain {
    private static final Logger logger = LoggerFactory.getLogger(OverPartitionMain.class);

    public static void initEnvironment(StreamExecutionEnvironment env) {
        env.setParallelism(1);

        env.setStateBackend(new HashMapStateBackend());
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointInterval(6000);
        checkpointConfig.setMinPauseBetweenCheckpoints(1000);
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setCheckpointStorage("file:///Users/cong.dai/flink-checkpoint/");

        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3,
                org.apache.flink.api.common.time.Time.of(5, TimeUnit.SECONDS),
                org.apache.flink.api.common.time.Time.of(10, TimeUnit.SECONDS)
        ));
    }

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration.setString("table.exec.state.ttl", "0" );

        initEnvironment(env);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setGroupId("test")
                .setTopics("test")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> sourceStream = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        DataStream<PreTxData> eventDataStream = sourceStream.flatMap(new RichFlatMapFunction<String, PreTxData>() {
            private Gson gson;
            @Override
            public void open(Configuration parameters) throws Exception {
                gson = new Gson();
            }
            @Override
            public void flatMap(String s, Collector<PreTxData> collector) throws Exception {
                PreTxData data = null;
                try {
                    data = gson.fromJson(s, PreTxData.class);
                } catch (Exception e) {

                }
                if(data != null) {
                    collector.collect(data);
                }
            }
        }).assignTimestampsAndWatermarks(
                new WatermarkStrategy<PreTxData>() {
                    @Override
                    public WatermarkGenerator<PreTxData> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                        return new WatermarkGenerator<PreTxData>() {
                            private long currentMaxTimestamp;
                            @Override
                            public void onEvent(PreTxData event, long eventTimestamp, WatermarkOutput output) {
                                currentMaxTimestamp = Math.max(currentMaxTimestamp, eventTimestamp);
                                output.emitWatermark(new Watermark(currentMaxTimestamp));
                            }

                            @Override
                            public void onPeriodicEmit(WatermarkOutput output) {
                                // emit the watermark as current highest timestamp minus the out-of-orderness bound
                                output.emitWatermark(new Watermark(currentMaxTimestamp));
                            }
                        };
                    }
                }
//                WatermarkStrategy.<PreFraudTransactionData>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner((event, timestamp) -> event.createAt)
                .withIdleness(Duration.ofSeconds(1))
        );

//        DataStream<PreFraudTransactionData> aStream = eventDataStream.filter(x -> x.paymentAttemptId.equals("a"));
//        DataStream<PreFraudTransactionData> notaStream = eventDataStream.filter(x -> !x.paymentAttemptId.equals("a"));
//
//        DataStream<PreFraudTransactionData> dataStream = aStream.union(notaStream);

        /*
        //  top N

        DataStream<Tuple3<String, Instant, Integer>> result = eventDataStream.map(
                new MapFunction<PreFraudTransactionData, Tuple3<String, Instant, Integer>>() {
                    @Override
                    public Tuple3<String, Instant, Integer> map(PreFraudTransactionData preFraudTransactionData) throws Exception {
                        return new Tuple3(preFraudTransactionData.paymentIntentId, Instant.ofEpochMilli(preFraudTransactionData.createAt), preFraudTransactionData.num);
                    }
                });

        tableEnv.createTemporaryView("events", result,
                Schema.newBuilder()
                        .columnByExpression("rowtime", "CAST(f1 AS TIMESTAMP_LTZ(3))")
                        .watermark("rowtime", "rowtime - INTERVAL '10' SECOND")
                        .build()
                );

        String topNSql =
                " SELECT * FROM (" +
                        " SELECT" +
                        "   f0, f1, f2, rowtime " +
                        "   ,ROW_NUMBER() OVER (" +
                        "       PARTITION BY f0 " +
                        "       ORDER BY f1 DESC " +
                        "     ) AS row_num " +
                        "   FROM events )" +
                        " WHERE row_num = 1";

        Table table = tableEnv.sqlQuery(topNSql);

        DataStream<Row> dataStream = tableEnv.toChangelogStream(table);

         */


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

//        String groupSql =
//                "SELECT " +
//                        "   paymentIntentId, deviceIp, SUM(num) FROM events " +
//                        "   GROUP BY paymentIntentId, deviceIp";

        Table table = tableEnv.sqlQuery(sql);

        DataStream<Row> dataStream = tableEnv.toDataStream(table);
//        tableEnv.createTemporaryView("intent_device", table);



//
//        dataStream.filter(x -> x.getKind().equals(RowKind.INSERT) || x.getKind().equals(RowKind.UPDATE_AFTER)).print();

        dataStream.print();
        env.execute("WindowFlink");
    }
}