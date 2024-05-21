package org.example.sql;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.util.FlinkUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class RegularJoin {
    private static final Logger logger = LoggerFactory.getLogger(RegularJoin.class);

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

        String regularJoin =
                " SELECT pre.paymentAttemptId, sum(post.num) " +
                "   FROM pre FULL JOIN post " +
                "   on pre.deviceIp = post.deviceIp " +
                "   WHERE post.event_time > pre.event_time - INTERVAL '1' DAY AND post.event_time < pre.event_time " +
                "   GROUP BY pre.paymentAttemptId";

        Table t = tableEnv.sqlQuery(regularJoin);
        tableEnv.toChangelogStream(t).print();

        env.execute("RegularJoin");
    }
}
