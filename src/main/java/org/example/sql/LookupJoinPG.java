package org.example.sql;


import org.apache.flink.api.java.utils.ParameterTool;
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

/*
> kafka-console-producer.sh --broker-list localhost:9092 --topic test
{"orderId":"o1","customerId":"c2","createAt":1708922054455,"total":3}
 */

/*
lookup chache: https://nightlies.apache.org/flink/flink-docs-release-1.11/zh/dev/table/connectors/jdbc.html#lookup-cache
By default, lookup cache is not enabled. You can enable it by setting both lookup.cache.max-rows and lookup.cache.ttl.
When lookup cache is enabled, each process (i.e. TaskManager) will hold a cache.
Flink will lookup the cache first, and only send requests to external database when cache missing, and update cache with the rows returned.
The oldest rows in cache will be expired when the cache hit to the max cached rows lookup.cache.max-rows
or when the row exceeds the max time to live lookup.cache.ttl.
 */


public class LookupJoinPG {
    private static final Logger logger = LoggerFactory.getLogger(LookupJoinPG.class);

    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        boolean allowedLookupCache = parameters.getBoolean("lookup_cache", false);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration.setString("table.exec.state.ttl", "0" );

        FlinkUtil.initEnvironment(env);

        String lookupCache = "";
        if (allowedLookupCache) {
            lookupCache += " 'lookup.cache.ttl' = '1min', 'lookup.cache.max-rows' = '1000', ";
        }

        String createExternalTable = "CREATE TEMPORARY TABLE Customers (" +
                        "   id INT, " +
                        "   customerId STRING, " +
                        "   country STRING, " +
                        "   zip STRING, " +
                        "   age INT, " +
                        "   updated BIGINT) WITH ( " +
                        "   'connector' = 'jdbc'," +
                        "   'url' = 'jdbc:postgresql://localhost:5432/customerdb'," + lookupCache +
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
                        "   'scan.startup.mode' = 'group-offsets', " +
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

        env.execute("LookupJoinPG");
    }
}
