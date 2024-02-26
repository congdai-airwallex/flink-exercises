package org.example.sql;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.TimeUnit;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

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

        String createExternalTable = "CREATE TEMPORARY TABLE Customers (" +
                        "   id INT, " +
                        "   customerId STRING, " +
                        "   country STRING, " +
                        "   zip STRING, " +
                        "   age INT, " +
                        "   updated BIGINT) WITH ( " +
                        "   'connector' = 'jdbc'," +
                        "   'url' = 'jdbc:postgresql://localhost:5432/customerdb'," +
                        "   'table-name' = 'customers'" +
                        ");";
        String createMainTable = "CREATE TABLE Orders (" +
                        "   orderId STRING, " +
                        "   customerId STRING, " +
                        "   createAt BIGINT, " +
                        "   total INT, " +
                        "   proc_time AS PROCTIME(), " +
                        "   event_time AS TO_TIMESTAMP_LTZ(createAt, 3), " +
                        "   WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND " +
                        "   ) WITH ( " +
                        "   'connector' = 'kafka', " +
                        "   'topic' = 'test', " +
                        "   'properties.bootstrap.servers' = 'localhost:9092', " +
                        "   'properties.group.id' = 'test', " +
                        "   'scan.startup.mode' = 'latest-offset', " +
                        "   'format' = 'json'" +
                        ");";

        tableEnv.executeSql(createExternalTable);
        tableEnv.executeSql(createMainTable);

        String joinSql = " SELECT " +
                        "   o.orderId, o.customerId, o.total, c.country, c.zip " +
                        "   FROM Orders AS o " +
                        "   LEFT JOIN Customers FOR SYSTEM_TIME AS OF o.proc_time AS c " +
                        "   ON o.customerId = c.customerId;";

        Table t = tableEnv.sqlQuery(joinSql);
        tableEnv.toDataStream(t).print();

        env.execute("LookupJoin");
    }
}
