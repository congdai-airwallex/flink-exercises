package org.example.uds;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

public class RedisLookUpFunction extends TableFunction<RowData> {
    private static final Logger logger = LoggerFactory.getLogger(RedisLookUpFunction.class);
    private final String hostname;
    private final int port;
    private final String password;
    private final String redisDatatype;
    private final DataType producedDataType;
    private List<LogicalType> parsingTypes;
    private StatefulRedisConnection<String, String> connection;
    private RedisCommands<String, String> commands;
    private final String REDIS_LIST_DATA_TYPE = "list";
    public RedisLookUpFunction(String hostname, int port, String password, String redisDatatype, DataType producedDataType) {
        this.hostname = hostname;
        this.port = port;
        this.password = password;
        this.redisDatatype = redisDatatype;
        this.producedDataType = producedDataType;
    }
    @Override
    public void open(FunctionContext context)  {
        parsingTypes = producedDataType.getLogicalType().getChildren();
        try {
            RedisClient client = RedisClient.create(
                    RedisURI.Builder.redis(hostname, port).withPassword(password).withTimeout(Duration.ofSeconds(3)).build()
            );
            connection = client.connect();
            commands = connection.sync();
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(ExceptionUtils.getStackTrace(e));
            throw new RuntimeException(e);
        }

    }

    private static Object parse(LogicalTypeRoot root, String value) {
        switch (root) {
            case INTEGER:
                return Integer.parseInt(value);
            case VARCHAR:
                return StringData.fromString(value);
            case DOUBLE:
                return Double.valueOf(value);
            case BIGINT:
                return Long.valueOf(value);
            case FLOAT:
                return Float.valueOf(value);
            case BOOLEAN:
                return Boolean.valueOf(value);
            default:
                throw new IllegalArgumentException();
        }
    }

    // lookup by one key
    // now only support lists data type
    public void eval(Object obj) {
        RowData lookupKey = GenericRowData.of(obj);
        StringData key = lookupKey.getString(0);
        List<String> l;
        switch (redisDatatype) {
            case REDIS_LIST_DATA_TYPE:
                try {
                    l = commands.lrange(key.toString(), 0, -1);
                } catch (Exception e) {
                    logger.error(e.getMessage());
                    return;
                }
                if (l == null || l.isEmpty()) return;
                if (l.size() + 1 != parsingTypes.size()) {
                    logger.error("result of lrange redis size is not match with table column number");
                    return;
                }
                break;
            default:
                throw new IllegalArgumentException();
        }
        Object[] s = new Object[parsingTypes.size()];
        s[0] = key;
        for(int i=0; i<l.size(); i++) {
            s[i+1] = parse(parsingTypes.get(i+1).getTypeRoot(), l.get(i));
        }
        RowData result = GenericRowData.of(s);
        collect(result);
    }

    // lookup by keys
    public void eval(Object... obj) {

    }

    @Override
    public void close() throws Exception {
        connection.close();
    }
}
