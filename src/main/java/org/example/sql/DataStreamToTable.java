package org.example.sql;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.example.model.PreTxData;
import org.example.util.BoundedOutOfOrdernessStrategy;
import org.example.util.FlinkUtil;
import org.example.util.PreTxDataParserRichFlatMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DataStreamToTable {
    private static final Logger logger = LoggerFactory.getLogger(DataStreamToTable.class);

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

        WatermarkStrategy<PreTxData> wm1 = new BoundedOutOfOrdernessStrategy<>("preDataStreamWM",0L);

        DataStream<PreTxData> preDataStream = preStream
                .flatMap(new PreTxDataParserRichFlatMap())
                .assignTimestampsAndWatermarks(
                        wm1.withTimestampAssigner((event, timestamp) -> event.createAt)
                                .withIdleness(Duration.ofSeconds(1))
        );

        DataStream<PreTxData> postDataStream = postStream
                .flatMap(new PreTxDataParserRichFlatMap());

//     Method 1: eventtime and watermark derive from source
//     1. Schema
//     Since DataStream<PreTxData> is POJO so table schema derive all physical columns automatically
//     2. Event time and Watermark
//     The virtual DataStream table connector exposes the following metadata for every row:
//        rowtime	TIMESTAMP_LTZ(3) NOT NULL	Stream record's timestamp.
//     rowtime is from datastream's eventtime
//     The virtual DataStream table source implements SupportsSourceWatermark
//     and thus allows calling the SOURCE_WATERMARK() built-in function as a watermark strategy
//     to adopt watermarks from the DataStream API.
        tableEnv.createTemporaryView(
                "pre",
                preDataStream,
                Schema.newBuilder()
                        .columnByMetadata("rowtime", "TIMESTAMP_LTZ(3)")
                        .watermark("rowtime", "SOURCE_WATERMARK()")
                        .build());

        tableEnv.from("pre").printSchema();
//      (
//          `createAt` BIGINT,
//          `deviceIp` STRING,
//          `num` INT,
//          `paymentAttemptId` STRING,
//          `paymentIntentId` STRING,
//          `rowtime` TIMESTAMP_LTZ(3) *ROWTIME* METADATA,
//          WATERMARK FOR `rowtime`: TIMESTAMP_LTZ(3) AS SOURCE_WATERMARK()
//      )
        Table preTable = tableEnv.sqlQuery("" +
                " select \'pre\', paymentIntentId, paymentAttemptId, deviceIp, createAt, rowtime, " +
                " sum(num) over (" +
                "   partition by deviceIp " +
                "   order by rowtime RANGE BETWEEN INTERVAL '10' MINUTE PRECEDING AND CURRENT ROW) " +
                " from pre");
        tableEnv.toDataStream(preTable).print();
        tableEnv.dropTemporaryView("pre");


//     Method 2: eventtime and watermark is
//     1. Schema
//     table schema can define physical columns manually
//     2. Event time and Watermark
//     add computed columns (in this case for creating a rowtime attribute column) and a custom watermark strategy
//     WARNING !!!!!!!! source datastream CANNOT assignTimestampsAndWatermarks
        tableEnv.createTemporaryView(
                "post",
                postDataStream,
                Schema.newBuilder()
                        .column("paymentIntentId", "STRING")
                        .column("paymentAttemptId", "STRING")
                        .column("deviceIp", "STRING")
                        .column("createAt", "BIGINT")
                        .column("num", "INT")
                        .columnByExpression("rowtime", "TO_TIMESTAMP_LTZ(createAt, 3)")
                        .watermark("rowtime", "rowtime - INTERVAL '0' SECOND")
                        .build());
        tableEnv.from("post").printSchema();
//        (
//          `paymentIntentId` STRING,
//          `paymentAttemptId` STRING,
//          `deviceIp` STRING,
//          `createAt` BIGINT,
//          `num` INT,
//          `rowtime` TIMESTAMP_LTZ(3) *ROWTIME*
//        )
        Table postTable = tableEnv.sqlQuery("" +
                " select \'post\', paymentIntentId, paymentAttemptId, deviceIp, createAt, rowtime, " +
                " sum(num) over (" +
                "   partition by deviceIp " +
                "   order by rowtime RANGE BETWEEN INTERVAL '10' MINUTE PRECEDING AND CURRENT ROW) " +
                " from post");
        tableEnv.toDataStream(postTable).print();
        tableEnv.dropTemporaryView("post");

        env.execute("DataStreamToTable");
    }
}
