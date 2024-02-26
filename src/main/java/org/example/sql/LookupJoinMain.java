package org.example.sql;


import org.apache.flink.configuration.Configuration;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.example.util.FlinkUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/*
# postgre
> CREATE DATABASE customerdb;
> CREATE TABLE customers (ID SERIAL PRIMARY KEY, customerId VARCHAR(80), country VARCHAR(10), zip VARCHAR(10), age INT, updated BIGINT);
> \d customers
------------+-----------------------+----------+----------+---------------------------------------
 id         | integer               |          | not null | nextval('customers_id_seq'::regclass)
 customerid | character varying(80) |          |          |
 country    | character varying(10) |          |          |
 zip        | character varying(10) |          |          |
 age        | integer               |          |          |
 updated    | bigint                |          |          |
 */


public class LookupJoinMain {
    private static final Logger logger = LoggerFactory.getLogger(LookupJoinMain.class);

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration.setString("table.exec.state.ttl", "0" );

        FlinkUtil.initEnvironment(env);

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
