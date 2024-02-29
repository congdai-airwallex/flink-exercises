package org.example.uds;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.LinkedList;
import java.util.List;

public class RedisLookUpFunction extends TableFunction<RowData> {
    private static final Logger logger = LoggerFactory.getLogger(RedisLookUpFunction.class);
    private final String hostname;
    private final int port;
    private final String password;
    private final DataType producedDataType;
    private List<LogicalType> parsingTypes;
    private StatefulRedisConnection<String, String> connection;
    public RedisLookUpFunction(String hostname, int port, String password, DataType producedDataType) {
        this.hostname = hostname;
        this.port = port;
        this.password = password;
        this.producedDataType = producedDataType;
    }
    @Override
    public void open(FunctionContext context)  {
        parsingTypes = producedDataType.getLogicalType().getChildren();
//        for(int i = 0; i< parsingTypes.size(); i++) {
//            System.out.println(parsingTypes.get(i).getTypeRoot());
//        }
        try {
            RedisClient client = RedisClient.create(
                    RedisURI.Builder.redis(hostname, port).withPassword(password).withTimeout(Duration.ofSeconds(3)).build()
            );
            connection = client.connect();
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(ExceptionUtils.getStackTrace(e));
            throw new RuntimeException(e);
        }

    }

    // lookup by one key
    public void eval(Object obj) {
        RowData lookupKey = GenericRowData.of(obj);
        StringData key = lookupKey.getString(0);
        List<String> l  = connection.sync().lrange(key.toString(), 0, -1);
        if(l == null || l.isEmpty()) return;
        LinkedList<String> l2 = new LinkedList<>(l);
        l2.addFirst(key.toString());
        Object[] s = new Object[l2.size()];
        s[0] = key;
        s[1] = Integer.valueOf(l2.get(1));
        s[2] = Long.valueOf(l2.get(2));
        s[3] = StringData.fromString(l2.get(3));
        s[4] = Double.valueOf(l2.get(4));
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
