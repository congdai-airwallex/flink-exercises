package org.example.sql;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.example.model.PayData;
import org.example.util.Stddev;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import com.google.gson.Gson;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.*;

public class UdfMain {
    private static final Logger logger = LoggerFactory.getLogger(UdfMain.class);

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
        tableEnv.createTemporarySystemFunction("myStddev", Stddev.class);
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration.setString("table.exec.state.ttl", "0");

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

        DataStream<Tuple2<Long, PayData>> eventDataStream = sourceStream.flatMap(new RichFlatMapFunction<String, Tuple2<Long, PayData>>() {
            private Gson gson;

            @Override
            public void open(Configuration parameters) throws Exception {
                gson = new Gson();
            }

            @Override
            public void flatMap(String s, Collector<Tuple2<Long, PayData>> collector) throws Exception {
                PayData data = null;
                try {
                    data = gson.fromJson(s, PayData.class);
                } catch (Exception e) {

                }
                if (data != null) {
                    collector.collect(new Tuple2<>(data.createAt, data));
                }
            }
        }).assignTimestampsAndWatermarks(
                new WatermarkStrategy<Tuple2<Long, PayData>>() {
                    @Override
                    public WatermarkGenerator<Tuple2<Long, PayData>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                        return new WatermarkGenerator<Tuple2<Long, PayData>>() {
                            private long currentMaxTimestamp;

                            @Override
                            public void onEvent(Tuple2<Long, PayData> event, long eventTimestamp, WatermarkOutput output) {
                                currentMaxTimestamp = Math.max(currentMaxTimestamp, eventTimestamp);
                                output.emitWatermark(new Watermark(currentMaxTimestamp));
                            }

                            @Override
                            public void onPeriodicEmit(WatermarkOutput output) {
                                output.emitWatermark(new Watermark(currentMaxTimestamp));
                            }
                        };
                    }
                }
                .withTimestampAssigner((event, timestamp) -> event.f0).withIdleness(Duration.ofSeconds(1))
        );

        tableEnv.createTemporaryView("events", eventDataStream, $("f0"), $("f1"), $("f0").rowtime().as("f2"));

        String sql =
                " SELECT" +
                        "   f0 AS ts__, f2 AS row_time__, " +
                        "   f1.paymentAttemptId AS paymentAttemptId, " +
                        "   f1.partitionKey AS partitionKey, " +
                        "   f1.aggregationKeyMappingValue AS aggregationKeyMappingValue, " +
                        "   f1.createAt AS createAt " +
                        "   FROM events ";

        Table table = tableEnv.sqlQuery(sql);
        tableEnv.createTemporaryView("extracted", table);

        String newSql =
                "select  ts__, paymentAttemptId as id, "
                        + " myStddev(aggregationKeyMappingValue)"
                        + "OVER (PARTITION BY partitionKey ORDER BY row_time__ RANGE BETWEEN INTERVAL "
                        + "'10' MINUTE " + " PRECEDING AND CURRENT ROW) as `amount1`,"
                        + " stddev(aggregationKeyMappingValue)"
                        + "OVER (PARTITION BY partitionKey ORDER BY row_time__ RANGE BETWEEN INTERVAL "
                        + "'10' MINUTE " + " PRECEDING AND CURRENT ROW) as `amount2` " +
                        " FROM extracted";
        Table t = tableEnv.sqlQuery(newSql);

        DataStream<Row> dataStream = tableEnv.toDataStream(t);

        dataStream.print();
        env.execute("UDF");
    }
}