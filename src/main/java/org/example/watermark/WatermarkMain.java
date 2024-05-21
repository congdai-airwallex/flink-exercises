package org.example.watermark;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.example.model.PreTxData;
import org.example.util.BoundedOutOfOrdernessStrategy;
import org.example.util.FlinkUtil;
import org.example.util.PreTxDataParserRichFlatMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.example.util.FlinkUtil.logInfo;

public class WatermarkMain {
    private static final Logger logger = LoggerFactory.getLogger(WatermarkMain.class);

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration.setString("table.exec.state.ttl", "0" );

        FlinkUtil.initEnvironment(env);
        env.getConfig().setAutoWatermarkInterval(10000);
        env.setParallelism(2);

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

<<<<<<<< HEAD:src/main/java/org/example/watermark/WatermarkMain.java
        WatermarkStrategy<PreTxData> wm1 = new BoundedOutOfOrdernessStrategy<>("pre", 0L);
        WatermarkStrategy<PreTxData> wm2 = new BoundedOutOfOrdernessStrategy<>("post", 0L);
========
        WatermarkStrategy<PreTxData> wm1 = new BoundedOutOfOrdernessStrategy<>("",0L);
        WatermarkStrategy<PreTxData> wm2 = new BoundedOutOfOrdernessStrategy<>("",0L);
>>>>>>>> 57d3ce7feea2745c54fc0ab64f821893a51d9dc4:src/main/java/org/example/sql/JoinMain.java

        DataStream<PreTxData> preDataStream = preStream
                .flatMap(new PreTxDataParserRichFlatMap())
                .assignTimestampsAndWatermarks(
                        wm1.withTimestampAssigner((event, timestamp) -> event.createAt)
                );

        DataStream<PreTxData> postDataStream = postStream
                .flatMap(new PreTxDataParserRichFlatMap())
                .assignTimestampsAndWatermarks(
                        wm2.withTimestampAssigner((event, timestamp) -> event.createAt)
                );

        /* the union watermark is always the min of watermarks from all input streams*/
        WatermarkStrategy<PreTxData> wm3 = new BoundedOutOfOrdernessStrategy<>("union", 0L);
        DataStream<PreTxData> unionStream = preDataStream.union(postDataStream)
                        .assignTimestampsAndWatermarks(
                            wm3.withTimestampAssigner((event, timestamp) -> event.createAt)
                                    .withIdleness(Duration.ofMillis(50000))
                        );

        DataStream<PreTxData> processedStream = unionStream.process(new ProcessFunction<PreTxData, PreTxData>() {
            public int uuid;
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                uuid = getRuntimeContext().getIndexOfThisSubtask();
                logInfo("%d: open\n", uuid);
            }

            @Override
            public void processElement(PreTxData preTxData, Context context, Collector<PreTxData> collector) throws Exception {
                logInfo("%d: processElement watermark: %d\n", uuid, context.timerService().currentWatermark());
                logInfo("%d: processElement timestamp: %d\n", uuid, context.timestamp());
                collector.collect(preTxData);
            }
        });

        tableEnv.createTemporaryView("temp", unionStream,
                $("paymentIntentId"),
                $("paymentAttemptId"),
                $("deviceIp"),
                $("createAt"),
                $("createAt").rowtime().as("event_time"),
                $("num"));

        // simple sql output is not affected by watermark
        String simpleSql = "SELECT * FROM temp";

        // over sql output is not affected by watermark
        // watermark will trigger the computation
        String overSql =" SELECT " +
                        " paymentIntentId as id " +
                        " , createAt " +
                        " , SUM(num) over (partition by deviceIp ORDER BY event_time RANGE BETWEEN INTERVAL '3' DAY(3) PRECEDING AND CURRENT ROW) as amount " +
                        " FROM temp ";

        Table table = tableEnv.sqlQuery(overSql);

        DataStream<Row> dataStream = tableEnv.toDataStream(table);
        dataStream.print();

        env.execute("WatermarkMain");
    }
}
