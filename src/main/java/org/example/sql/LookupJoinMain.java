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
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.example.model.PayFilterData;
import org.example.model.PreTxData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import com.google.gson.Gson;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class LookupJoinMain {
    private static final Logger logger = LoggerFactory.getLogger(LookupJoinMain.class);

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

        KafkaSource<String> preFraudSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setGroupId("test")
                .setTopics("test")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        KafkaSource<String> paymentSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setGroupId("test")
                .setTopics("test2")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();


        DataStream<String> preStream = env
                .fromSource(preFraudSource, WatermarkStrategy.noWatermarks(), "Kafka Source 1");

        DataStream<String> postStream = env
                .fromSource(paymentSource, WatermarkStrategy.noWatermarks(), "Kafka Source 2");

        DataStream<PreTxData> preDataStream = preStream.flatMap(new RichFlatMapFunction<String, PreTxData>() {
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
                    //data.createAt = System.currentTimeMillis();
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
                                //output.emitWatermark(new Watermark(currentMaxTimestamp));
                            }

                            @Override
                            public void onPeriodicEmit(WatermarkOutput output) {
                                // emit the watermark as current highest timestamp minus the out-of-orderness bound
                                output.emitWatermark(new Watermark(currentMaxTimestamp));
                            }
                        };
                    }
                }.withTimestampAssigner((event, timestamp) -> event.createAt).withIdleness(Duration.ofMillis(300))
        );

        DataStream<PayFilterData> postDataStream = postStream.flatMap(new RichFlatMapFunction<String, PayFilterData>() {
            private Gson gson;
            @Override
            public void open(Configuration parameters) throws Exception {
                gson = new Gson();
            }
            @Override
            public void flatMap(String s, Collector<PayFilterData> collector) throws Exception {
                PayFilterData data = null;
                try {
                    data = gson.fromJson(s, PayFilterData.class);
                    //data.createAt = System.currentTimeMillis();
                } catch (Exception e) {

                }
                if(data != null) {
                    collector.collect(data);
                }
            }
        }).assignTimestampsAndWatermarks(
                new WatermarkStrategy<PayFilterData>() {
                    @Override
                    public WatermarkGenerator<PayFilterData> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                        return new WatermarkGenerator<PayFilterData>() {
                            private long currentMaxTimestamp;
                            @Override
                            public void onEvent(PayFilterData event, long eventTimestamp, WatermarkOutput output) {
                                currentMaxTimestamp = Math.max(currentMaxTimestamp, eventTimestamp);
                                //output.emitWatermark(new Watermark(currentMaxTimestamp));
                            }

                            @Override
                            public void onPeriodicEmit(WatermarkOutput output) {
                                // emit the watermark as current highest timestamp minus the out-of-orderness bound
                                output.emitWatermark(new Watermark(currentMaxTimestamp));
                            }
                        };
                    }
                }.withTimestampAssigner((event, timestamp) -> event.createAt).withIdleness(Duration.ofMillis(300))
        );

        tableEnv.createTemporaryView("post", postDataStream,
                $("clientId"),
                $("paymentId"),
                $("amountusd"),
                $("filtered"),
                $("createAt")
                //$("createAt").rowtime().as("event_time")
        );

        String createWriteTable = "CREATE TABLE payment (" +
                " clientid STRING," +
                " createat BIGINT," +
                " paymentid STRING," +
                " amountusd DOUBLE" +
                ") WITH (" +
                " 'connector' = 'jdbc'," +
                " 'url' = 'jdbc:postgresql://localhost:5432/test', " +
                " 'table-name' = 'payment')";

        tableEnv.executeSql(createWriteTable);
        tableEnv.executeSql("INSERT INTO payment (clientid, createat, paymentid, amountusd) " +
                "SELECT clientId, createAt, paymentId, amountusd FROM post");

        String createReadTable = "CREATE TABLE payment_read (" +
                " clientid STRING," +
                " createat BIGINT," +
                " paymentid STRING PRIMARY KEY," +
                " amountusd DOUBLE" +
                ") WITH (" +
                " 'connector' = 'jdbc'," +
                " 'url' = 'jdbc:postgresql://localhost:5432/test', " +
                " 'table-name' = 'payment'," +
                " 'lookup.cache.max-rows' = '500'," +
                " 'lookup.cache.ttl' = '3600'," +
                " 'lookup.max-retries' = '1')";

        tableEnv.executeSql(createReadTable);

        tableEnv.createTemporaryView("pre", preDataStream,
                $("paymentIntentId"),
                $("paymentAttemptId"),
                $("deviceIp"),
                $("createAt"),
                $("event_time").proctime(),
                $("num"));

        Table table = tableEnv.sqlQuery("" +
                "SELECT " +
                " e.paymentAttemptId," +
                " e.createAt," +
                " p.amountusd" +
                " FROM pre AS e" +
                " JOIN payment_read FOR SYSTEM_TIME AS OF e.event_time AS p" +
                " ON e.deviceIp = p.clientid");

        DataStream<Row> dataStream = tableEnv.toDataStream(table);
        dataStream.print();

        env.execute("KeyedCoProcessFlink");
    }
}
