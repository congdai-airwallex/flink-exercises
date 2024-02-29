package org.example.uds;

import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.types.DataType;

public class RedisDynamicTableSource implements LookupTableSource {
    private final String hostname;
    private final int port;
    private final String password;
    private final String redisDatatype;
    private final DataType producedDataType;
    public RedisDynamicTableSource(String hostname, int port, String password, String redisDatatype, DataType producedDataType) {
        this.hostname = hostname;
        this.port = port;
        this.password = password;
        this.redisDatatype = redisDatatype;
        this.producedDataType = producedDataType;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        return TableFunctionProvider.of(new RedisLookUpFunction(hostname, port, password, redisDatatype, producedDataType));
    }

    @Override
    public DynamicTableSource copy() {
        return new RedisDynamicTableSource(hostname, port, password, redisDatatype, producedDataType);
    }

    @Override
    public String asSummaryString() {
        return "Redis Table Source";
    }
}
