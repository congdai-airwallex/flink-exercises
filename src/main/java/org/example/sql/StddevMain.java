package org.example.sql;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.example.model.PayData;
import org.example.model.AmountData;
import org.example.util.BoundedOutOfOrdernessStrategy;
import org.example.util.FlinkUtil;
import org.example.util.PayDataParserRichFlatMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.time.Duration;
import com.google.gson.Gson;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.*;

public class StddevMain {
    private static final Logger logger = LoggerFactory.getLogger(StddevMain.class);

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
        WatermarkStrategy<Tuple2<Long, PayData>> wt = new BoundedOutOfOrdernessStrategy<>(0L);

        DataStream<Tuple2<Long, PayData>> eventDataStream = sourceStream
                .flatMap(new PayDataParserRichFlatMap())
                .map(new MapFunction<PayData, Tuple2<Long, PayData>>() {
                    @Override
                    public Tuple2<Long, PayData> map(PayData value) throws Exception {
                        return new Tuple2(System.currentTimeMillis(), value);
                    }
                }).assignTimestampsAndWatermarks(
                    wt.withTimestampAssigner((event, timestamp) -> event.f0)
                            .withIdleness(Duration.ofSeconds(1))
                );

        tableEnv.createTemporaryView("events", eventDataStream, $("f0"),  $("f1"), $("f0").rowtime().as("f2"));

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
                "select  ts__, paymentAttemptId as id, stddev(aggregationKeyMappingValue)"
                + "OVER (PARTITION BY partitionKey ORDER BY row_time__ RANGE BETWEEN INTERVAL "
                + "'10' MINUTE " + " PRECEDING AND CURRENT ROW) as `amount` FROM extracted";
        Table t = tableEnv.sqlQuery(newSql);
        //tableEnv.toDataStream(t).print();

        DataStream<AmountData> dataStream = tableEnv.toAppendStream(t, Row.class)
                .map(r -> {
                    System.out.println("before :" + r.getField(2));
                    AmountData x  = convertRow(r, AmountData.class);
                    System.out.println("convertRow " + x.amount);
                    return x;
                })
                .map(x -> {
                    return new AmountData(x.id, ((x.amount == null) || Double.isNaN(x.amount)) ? -2.0 : x.amount);
                });;


        dataStream.print();
        env.execute("stddev");
    }

    private static <T> T convertRow(final Row row, final Class<T> uc) {
            Field[] fields = uc.getDeclaredFields();
            // r contains an extra ts field;
            assert (row.getArity() == fields.length + 1);
            Object[] args = new Object[fields.length];
            Class<?>[] types = new Class<?>[fields.length];
            for (int i = 0; i < fields.length; i++) {
                args[i] = row.getField(i + 1);
                types[i] = fields[i].getType();
            }
            try {
                return uc.getDeclaredConstructor(types).newInstance(args);
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
    }
}