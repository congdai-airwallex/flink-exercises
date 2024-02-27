package org.example.uds;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;

public class RedisLookUpFunction extends TableFunction<RowData> {
    private final String hostname;
    private final int port;
    private final String password;
    public RedisLookUpFunction(String hostname, int port, String password) {
        this.hostname = hostname;
        this.port = port;
        this.password = password;
    }
    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);

    }

    // lookup by one key
    public void eval(Object obj) {
        RowData lookupKey = GenericRowData.of(obj);

        StringData key = lookupKey.getString(0);
        String value = "hello world";
        RowData result = GenericRowData.of(key, StringData.fromString(value));
        collect(result);
    }

    // lookup by keys
    public void eval(Object... obj) {

    }

    @Override
    public void close() throws Exception {

    }
}
