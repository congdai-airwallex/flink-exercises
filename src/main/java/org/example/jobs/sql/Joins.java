package org.example.jobs.sql;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.util.FlinkUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Joins {
    private static final Logger logger = LoggerFactory.getLogger(Joins.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration.setString("table.exec.state.ttl", "0" );

        FlinkUtil.initEnvironment(env);

        String createPreTable = "CREATE TABLE pre (" +
                "   paymentIntentId STRING, " +
                "   paymentAttemptId STRING, " +
                "   deviceIp STRING, " +
                "   createAt BIGINT, " +
                "   num INT, " +
                "   proc_time AS PROCTIME(), " +
                "   event_time AS TO_TIMESTAMP_LTZ(createAt, 3), " +
                "   WATERMARK FOR event_time AS event_time - INTERVAL '0' SECOND " +
                "   ) WITH ( " +
                "   'connector' = 'kafka', " +
                "   'topic' = 'test', " +
                "   'properties.bootstrap.servers' = 'localhost:9092', " +
                "   'properties.group.id' = 'test', " +
                "   'scan.startup.mode' = 'group-offsets', " +
                "   'format' = 'json'" +
                ");";

        String createPostTable = "CREATE TABLE post (" +
                "   paymentIntentId STRING, " +
                "   paymentAttemptId STRING, " +
                "   deviceIp STRING, " +
                "   createAt BIGINT, " +
                "   num INT, " +
                "   proc_time AS PROCTIME(), " +
                "   event_time AS TO_TIMESTAMP_LTZ(createAt, 3), " +
                "   WATERMARK FOR event_time AS event_time - INTERVAL '0' SECOND " +
                "   ) WITH ( " +
                "   'connector' = 'kafka', " +
                "   'topic' = 'test2', " +
                "   'properties.bootstrap.servers' = 'localhost:9092', " +
                "   'properties.group.id' = 'test', " +
                "   'scan.startup.mode' = 'group-offsets', " +
                "   'format' = 'json'" +
                ");";

        tableEnv.executeSql(createPreTable);
        tableEnv.executeSql(createPostTable);

        // https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/queries/joins/#outer-equi-join
        // regular join is like a big memory table, but returns the change log
        String regularJoin =
                " SELECT 'regular', pre.paymentAttemptId, post.paymentAttemptId " +
                "   FROM pre FULL JOIN post " +
                "   on pre.deviceIp = post.deviceIp " +
                "   WHERE pre.paymentAttemptId is NULL OR post.paymentAttemptId is NULL " +
                "   OR (post.event_time > pre.event_time - INTERVAL '1' DAY AND post.event_time < pre.event_time) ";

        Table t1 = tableEnv.sqlQuery(regularJoin);
        tableEnv.toChangelogStream(t1).print();

        // Returns a simple Cartesian product restricted by the join condition and a time constraint.
        // An interval join requires at least one equi-join predicate and a join condition that bounds the time on both sides.
        // WARNING: return only two sides all have values, similar to INNER JOIN,
        // but Since time attributes are quasi-monotonic increasing,
        // Flink can remove old values from its state without affecting the correctness of the result.
        String intervalJoin =
                " SELECT 'interval', pre.paymentAttemptId, post.paymentAttemptId " +
                "   FROM pre, post " +
                "   WHERE pre.deviceIp = post.deviceIp " +
                "   AND post.event_time > pre.event_time - INTERVAL '1' DAY AND post.event_time < pre.event_time ";

        Table t2 = tableEnv.sqlQuery(intervalJoin);
        tableEnv.toChangelogStream(t2).print();

        env.execute("Joins");
    }
}
