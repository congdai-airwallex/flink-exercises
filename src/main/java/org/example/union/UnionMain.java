package org.example.union;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.example.model.PayFilterData;
import org.example.util.BoundedOutOfOrdernessStrategy;
import org.example.util.FlinkUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import com.google.gson.Gson;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import static org.apache.flink.table.api.Expressions.*;

/*
* union + sql partition over may lost data
* */
public class UnionMain {
    private static final Logger logger = LoggerFactory.getLogger(UnionMain.class);

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration.setString("table.exec.state.ttl", "0" );
        env.getConfig().setAutoWatermarkInterval(1000);

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

        WatermarkStrategy<PayFilterData> wm1 = new BoundedOutOfOrdernessStrategy<>(0L);
        WatermarkStrategy<PayFilterData> wm2 = new BoundedOutOfOrdernessStrategy<>(0L);
        WatermarkStrategy<PayFilterData> wm3 = new BoundedOutOfOrdernessStrategy<>(0L);
        WatermarkStrategy<PayFilterData> wm4 = new BoundedOutOfOrdernessStrategy<>(0L);


        DataStream<PayFilterData> preDataStream = preStream.flatMap(new RichFlatMapFunction<String, PayFilterData>() {
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
                    data.createAt = System.currentTimeMillis();
                } catch (Exception e) {

                }
                if(data != null) {
                    collector.collect(data);
                }
            }
        }).assignTimestampsAndWatermarks(
                wm1.withTimestampAssigner((event, timestamp) -> event.createAt)
                        .withIdleness(Duration.ofMillis(300))
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
                    data.createAt = System.currentTimeMillis();
                } catch (Exception e) {

                }
                if(data != null) {
                    collector.collect(data);
                }
            }
        }).assignTimestampsAndWatermarks(
                wm2.withTimestampAssigner((event, timestamp) -> event.createAt)
                        .withIdleness(Duration.ofMillis(300))
        );

        DataStream<PayFilterData> unionDataStream = preDataStream.union(postDataStream)
                .assignTimestampsAndWatermarks(
                    wm3.withTimestampAssigner((event, timestamp) -> event.createAt)
                            .withIdleness(Duration.ofMillis(300))
        );

        DataStream<PayFilterData> orderedDataStream = unionDataStream.process(new ProcessFunction<PayFilterData, PayFilterData>() {
            @Override
            public void processElement(PayFilterData paymentFilterData, ProcessFunction<PayFilterData, PayFilterData>.Context context, Collector<PayFilterData> collector) throws Exception {
//                System.out.printf("processElement watermark: %d\n", context.timerService().currentWatermark());
//                if(context.timerService().currentWatermark() > paymentFilterData.createAt) {
//                    paymentFilterData.createAt = context.timerService().currentWatermark();
//                }
                paymentFilterData.createAt = System.currentTimeMillis();
                collector.collect(paymentFilterData);
            }
        }).assignTimestampsAndWatermarks(
                wm4.withTimestampAssigner((event, timestamp) -> event.createAt)
                        .withIdleness(Duration.ofMillis(300))
        );

        tableEnv.createTemporaryView("temp", orderedDataStream,
                $("clientId"),
                $("paymentId"),
                $("amountusd"),
                $("filtered"),
                $("createAt").rowtime().as("row_time__"),
                $("createAt"));

//        Table o = tableEnv.fromDataStream(orderedDataStream,
//                Schema.newBuilder()
//                        .columnByExpression("row_time__", "TO_TIMESTAMP_LTZ(createAt, 3)")
//                        .watermark("row_time__",  "row_time__")
//                        .build());
//        tableEnv.createTemporaryView("temp", o);
//        o.printSchema();

        String sql =
                "SELECT " +
                " paymentId as id " +
                " , createAt " +
                " ,SUM(amountusd) over (partition by clientId ORDER BY row_time__ RANGE BETWEEN INTERVAL '3' DAY(3) PRECEDING AND CURRENT ROW) as amount " +
                " , filtered FROM temp ";

        Table table = tableEnv.sqlQuery(sql);

        DataStream<Row> dataStream = tableEnv.toDataStream(table)
                .assignTimestampsAndWatermarks(
                        new WatermarkStrategy<Row>() {
                            @Override
                            public WatermarkGenerator<Row> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                                return new WatermarkGenerator<Row>() {
                                    private long currentMaxTimestamp;
                                    @Override
                                    public void onEvent(Row row, long eventTimestamp, WatermarkOutput output) {
                                        currentMaxTimestamp = Math.max(currentMaxTimestamp, eventTimestamp);
                                    }

                                    @Override
                                    public void onPeriodicEmit(WatermarkOutput output) {
                                        // emit the watermark as current highest timestamp minus the out-of-orderness bound
                                        // System.out.printf("orderedDataStream emit: %d\n", currentMaxTimestamp);
                                        output.emitWatermark(new Watermark(currentMaxTimestamp-100));
                                    }
                                };
                            }
                        }.withTimestampAssigner((Row, timestamp) -> (Long) Row.getField(1)).withIdleness(Duration.ofMillis(300)));;
        //.filter(row -> (Integer) row.getField(2) != 0);

        dataStream.print();
        env.execute("UnionFlink");
    }
}
