package org.example.costsaving;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.util.FlinkUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class MultiStreamsInOneSlot {
    private static final Logger logger = LoggerFactory.getLogger(MultiStreamsInOneSlot.class);

    public static String createSourceSQL(String tableName, String topic, String group) {
        String createSourceTable = "CREATE TABLE " + tableName + " (" +
                "   paymentAttemptId STRING, " +
                "   partitionKey STRING, " +
                "   createAt BIGINT, " +
                "   aggregationKeyMappingValue INT, " +
                "   proc_time AS PROCTIME(), " +
                "   event_time AS TO_TIMESTAMP_LTZ(createAt, 3), " +
                "   WATERMARK FOR event_time AS event_time - INTERVAL '0' SECOND " +
                "   ) WITH ( " +
                "   'connector' = 'kafka', " +
                "   'topic' = '"+ topic + "', " +
                "   'properties.bootstrap.servers' = 'localhost:9092', " +
                "   'properties.group.id' = '"+ group + "', " +
                "   'scan.startup.mode' = 'latest-offset', " +
                "   'format' = 'json'" +
                ");";
        return createSourceTable;
    }

    public static void execute(StreamTableEnvironment tableEnv, String tableName, String topic, String group) {
        String sourceSQL = createSourceSQL(tableName, topic, group);
        tableEnv.executeSql(sourceSQL);
        String sql =
                " SELECT" +
                        "   paymentAttemptId, partitionKey, createAt, event_time, proc_time, '" + tableName + "' " +
                        "   ,sum(aggregationKeyMappingValue) OVER (" +
                        "       PARTITION BY partitionKey " +
                        "       ORDER BY event_time " +
                        "       RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW " +
                        "     ) AS sum_num " +
                        "   FROM " + tableName + " ;";
        Table table = tableEnv.sqlQuery(sql);
        DataStream<Row> dataStream = tableEnv.toDataStream(table);
        dataStream.print();
    }

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration.setString("table.exec.state.ttl", "0" );

        FlinkUtil.initEnvironment(env);

        for(int i=0; i<3; i++) {
            execute(tableEnv, "source"+i, "test", "group" + i);
        }

        env.execute("MultiJobsInOneSlot");
    }
}