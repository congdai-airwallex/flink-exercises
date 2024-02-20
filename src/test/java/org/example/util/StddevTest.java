package org.example.util;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;


public class StddevTest {
    public static MiniClusterWithClientResource flinkCluster;
    public StreamExecutionEnvironment env;
    public StreamTableEnvironment tableEnv;

    @Before
    public void setup() throws Exception {
        flinkCluster = new MiniClusterWithClientResource(
                new MiniClusterResourceConfiguration.Builder()
                        .setNumberSlotsPerTaskManager(1)
                        .setNumberTaskManagers(1)
                        .build());
        flinkCluster.before();
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        tableEnv = StreamTableEnvironment.create(env);
        tableEnv.createTemporarySystemFunction("MyStddev", Stddev.class);
    }

    @Test
    public void testSQL() throws Exception {
        Sink.values.clear();
        DataStream<Tuple3<Long, String, Double>> source = env.fromElements(
                new Tuple3(1708403000000L, "a", 3.0),    // null
                new Tuple3(1708403300000L, "a", 6.0),    // 2.1213...
                new Tuple3(1708403600000L, "a", 9.0),    // stddev([3.0, 6.0, 9.0]) = 3.0
                new Tuple3(1708403900000L, "a", 12.0)    // stddev([6.0, 9.0, 12.0]) = 3.0
        );
        DataStream<Tuple3<Long, String, Double>> ds = source.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<Long, String, Double>>forMonotonousTimestamps().withTimestampAssigner(
                        (t, x) -> t.f0)
        );
        tableEnv.createTemporaryView("tmp", ds, $("f0"), $("f1"), $("f2"), $("f0").rowtime().as("row_time"));

        String sql =
                "SELECT f0, f1, " +
                        " MyStddev(f2) OVER (PARTITION BY f1 ORDER BY row_time RANGE BETWEEN INTERVAL '10' MINUTE PRECEDING AND CURRENT ROW) " +
                        " FROM tmp";
        Table t = tableEnv.sqlQuery(sql);
        tableEnv.toDataStream(t).addSink(new Sink());
        env.execute();
        Assert.assertEquals(3, Sink.values.size());

    }

    @After
    public void tearDown() throws Exception {
        flinkCluster.after();
    }

    private static class Sink implements SinkFunction<Row> {
        public static final List<Row> values = Collections.synchronizedList(new ArrayList<>());
        @Override
        public void invoke(Row value, SinkFunction.Context context) throws Exception {
            values.add(value);
        }
    }
}
