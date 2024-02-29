package org.example.sql;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.example.util.FlinkUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class UdsJoin {
    private static final Logger logger = LoggerFactory.getLogger(UdsJoin.class);

    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        final String host = parameters.get("host", "localhost");
        final int port = parameters.getInt("port", 6379);
        final String password = parameters.get("password", "");
        final String dataType = parameters.get("redis-datatype", "list");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration.setString("table.exec.state.ttl", "0" );

        FlinkUtil.initEnvironment(env);

        String createRedisTable = "CREATE TEMPORARY TABLE test (" +
                "   rKey STRING, " +
                "   version INT, " +
                "   ts BIGINT, " +
                "   valueType STRING, " +
                "   mvalue DOUBLE " +
                ") WITH (" +
                "   'connector' = 'redis', " +
                "   'host' = '" + host + "', " +
                "   'port' = '" + port + "', " +
                "   'password' = '" + password + "', " +
                "   'datatype' = '" + dataType + "' " +
                ")";
        String createMainTable = "CREATE TABLE Orders (" +
                "   orderId STRING, " +
                "   total INT, " +
                "   proc_time AS PROCTIME() " +
                "   ) WITH ( " +
                "   'connector' = 'datagen', " +
                "   'rows-per-second' = '1', " +
                "   'fields.orderId.length' = '1', " +
                "   'fields.total.max' = '10', " +
                "   'fields.total.min' = '1' " +
                ");";

        tableEnv.executeSql(createRedisTable);
        tableEnv.executeSql(createMainTable);

        String joinSql = " SELECT " +
                "   o.orderId, o.total, t.rKey, t.version, t.ts, t.valueType, t.mvalue " +
                "   FROM Orders AS o " +
                "   LEFT JOIN test FOR SYSTEM_TIME AS OF o.proc_time AS t " +
                "   ON o.orderId = t.rKey;";

        Table t = tableEnv.sqlQuery(joinSql);
        tableEnv.toDataStream(t).print();

        env.execute("UDSJoin");
    }
}

