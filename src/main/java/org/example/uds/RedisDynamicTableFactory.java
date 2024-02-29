package org.example.uds;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.util.HashSet;
import java.util.Set;

public class RedisDynamicTableFactory implements DynamicTableSourceFactory {
    // define all options statically
    public static final ConfigOption<String> HOSTNAME = ConfigOptions.key("host")
            .stringType()
            .noDefaultValue();
    public static final ConfigOption<Integer> PORT = ConfigOptions.key("port")
            .intType()
            .noDefaultValue();
    public static final ConfigOption<String> PASSWORD = ConfigOptions.key("password")
            .stringType()
            .noDefaultValue();
    public static final ConfigOption<String> DATATYPE = ConfigOptions.key("datatype")
            .stringType()
            .noDefaultValue();
    @Override
    public String factoryIdentifier() {
        return "redis"; // used for matching to `connector = '...'`
    }
    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOSTNAME);
        options.add(PORT);
        options.add(PASSWORD);
        options.add(DATATYPE);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        // validate all options
        helper.validate();
        // get the validated options
        final ReadableConfig options = helper.getOptions();
        final String hostname = options.get(HOSTNAME);
        final int port = options.get(PORT);
        final String password = options.get(PASSWORD);
        final String datatype = options.get(DATATYPE);
        // derive the produced data type (excluding computed columns) from the catalog table
        final DataType producedDataType =
                context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();

        return new RedisDynamicTableSource(hostname, port, password, datatype, producedDataType);
    }
}
