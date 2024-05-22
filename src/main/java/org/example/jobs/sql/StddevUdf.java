package org.example.jobs.sql;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.model.PayData;
import org.example.util.BoundedOutOfOrdernessStrategy;
import org.example.util.FlinkUtil;
import org.example.util.PayDataParserRichFlatMap;
import org.example.udf.Stddev;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.*;

/*
    Since flink original stddev function has some bugs, we implement it by user defined aggregator function
 */
public class StddevUdf {
    private static final Logger logger = LoggerFactory.getLogger(StddevUdf.class);

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.createTemporarySystemFunction("myStddev", Stddev.class);
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration.setString("table.exec.state.ttl", "0");

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
        WatermarkStrategy<Tuple2<Long, PayData>> wt = new BoundedOutOfOrdernessStrategy<>("",0L);

        DataStream<Tuple2<Long, PayData>> eventDataStream = sourceStream.flatMap(new PayDataParserRichFlatMap())
                .map(new MapFunction<PayData, Tuple2<Long, PayData>>() {
                    @Override
                    public Tuple2<Long, PayData> map(PayData value) throws Exception {
                        return new Tuple2(value.createAt, value);
                    }
                }).assignTimestampsAndWatermarks(
                wt.withTimestampAssigner((event, timestamp) -> event.f0).withIdleness(Duration.ofSeconds(1))
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