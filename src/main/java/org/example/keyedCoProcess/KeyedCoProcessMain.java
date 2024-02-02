package org.example.keyedCoProcess;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.example.model.PayFilterData;
import org.example.model.PreTxData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import com.google.gson.Gson;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class KeyedCoProcessMain {
    private static final Logger logger = LoggerFactory.getLogger(KeyedCoProcessMain.class);

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
                                output.emitWatermark(new Watermark(currentMaxTimestamp- 1000));
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
                                output.emitWatermark(new Watermark(currentMaxTimestamp - 1000));
                            }
                        };
                    }
                }.withTimestampAssigner((event, timestamp) -> event.createAt).withIdleness(Duration.ofMillis(300))
        );

        DataStream<PayFilterData> dataStream =
                preDataStream
                .keyBy(x -> x.deviceIp)
                .connect(postDataStream.keyBy(x -> x.clientId)).process(
                        new KeyedCoProcessFunction<String, PreTxData, PayFilterData, PayFilterData>() {
                            private ListState<PreTxData> left;
                            private MapState<Long, PayFilterData> right;
                            @Override
                            public void open(Configuration config) {
                                ListStateDescriptor desc = new ListStateDescriptor<PreTxData>(
                                        "left",
                                        TypeInformation.of(PreTxData.class)
                                );
                                left = getRuntimeContext().getListState(desc);
                                MapStateDescriptor<Long, PayFilterData> desc2 = new MapStateDescriptor<Long, PayFilterData>(
                                        "right", TypeInformation.of(Long.class), TypeInformation.of(PayFilterData.class));
                                right = getRuntimeContext().getMapState(desc2);
                            }
                            @Override
                            public void processElement1(PreTxData data, Context context, Collector<PayFilterData> out) throws Exception {
                                System.out.printf("processElement1 watermark %d\n", context.timerService().currentWatermark());
                                System.out.printf("processElement1 timestamp %d\n", context.timestamp());
                                System.out.println();
                                left.add(data);
                            }
                            @Override
                            public void processElement2(PayFilterData data, Context context, Collector<PayFilterData> out) throws Exception {
                                System.out.printf("processElement2 watermark %d\n", context.timerService().currentWatermark());
                                System.out.printf("processElement2 timestamp %d\n", context.timestamp());
                                System.out.println();
                                if(context.timestamp() <= context.timerService().currentWatermark()) {
                                    System.out.printf("late data");
                                }
                                right.put(data.createAt, data);
                                context.timerService().registerEventTimeTimer(data.createAt);
                            }
                            @Override
                            public void onTimer(long time, OnTimerContext context, Collector<PayFilterData> out) throws Exception {
                                System.out.printf("onTimer time %d\n", time);
                                System.out.printf("onTimer watermark %d\n", context.timerService().currentWatermark());
                                System.out.printf("onTimer timestamp %d\n", context.timestamp());
                                System.out.println();
                                PayFilterData data = right.get(time);
                                if(data == null) return;
                                Double sum = 0.0;
                                for(PreTxData i : left.get()) {
                                    sum += i.num;
                                }
                                out.collect(new PayFilterData(data.clientId, data.paymentId, sum,
                                        data.filtered, data.createAt));
                            }
                        }
                        );
        dataStream.print();
        env.execute("KeyedCoProcessFlink");
    }
}
