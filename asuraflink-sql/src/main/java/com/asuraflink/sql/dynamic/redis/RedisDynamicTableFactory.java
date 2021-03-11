package com.asuraflink.sql.dynamic.redis;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import redis.clients.jedis.Protocol;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.table.factories.FactoryUtil.createTableFactoryHelper;

public class RedisDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = createTableFactoryHelper(this, context);
        helper.validate();

        ReadableConfig options = helper.getOptions();
        validateOptions(options);

        TableSchema schema = context.getCatalogTable().getSchema();
        return new RedisDynamicTableSink(options, schema);
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = createTableFactoryHelper(this, context);
        helper.validate();

        ReadableConfig options = helper.getOptions();
        validateOptions(options);

        TableSchema schema = context.getCatalogTable().getSchema();
        return new RedisDynamicTableSource(options, schema);
    }

    @Override
    public String factoryIdentifier() {
        return "redis";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(MODE);
        requiredOptions.add(COMMAND);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(SINGLE_HOST);
        optionalOptions.add(SINGLE_PORT);
        optionalOptions.add(PASSWORD);
        optionalOptions.add(COMMAND);
        optionalOptions.add(DB_NUM);
        optionalOptions.add(CONNECTION_TIMEOUT_MS);
        optionalOptions.add(CONNECTION_MAX_TOTAL);
        optionalOptions.add(CONNECTION_MAX_IDLE);
        optionalOptions.add(CONNECTION_MIN_IDLE);
        optionalOptions.add(CONNECTION_TEST_ON_BORROW);
        optionalOptions.add(CONNECTION_TEST_ON_RETURN);
        optionalOptions.add(CONNECTION_TEST_WHILE_IDLE);
        optionalOptions.add(LOOKUP_ADDITIONAL_KEY);
        return optionalOptions;
    }

    private void validateOptions(ReadableConfig options) {
        switch (options.get(MODE)) {
            case "single":
                if (StringUtils.isEmpty(options.get(SINGLE_HOST))) {
                    throw new IllegalArgumentException("Parameter single.host must be provided in single mode");
                }
                break;
//            case "cluster":
//                if (StringUtils.isEmpty(options.get(CLUSTER_NODES))) {
//                    throw new IllegalArgumentException("Parameter cluster.nodes must be provided in cluster mode");
//                }
//                break;
//            case "sentinel":
//                if (StringUtils.isEmpty(options.get(SENTINEL_NODES)) || StringUtils.isEmpty(options.get(SENTINEL_MASTER))) {
//                    throw new IllegalArgumentException("Parameters sentinel.nodes and sentinel.master must be provided in sentinel mode");
//                }
//                break;
            default:
                throw new IllegalArgumentException("Invalid Redis mode. Must be single/cluster/sentinel");
        }
    }
    // 创建语句 with 后的属性值
    public static final ConfigOption<String> MODE = ConfigOptions
            .key("mode")
            .stringType()
            .defaultValue("single");
    public static final ConfigOption<String> SINGLE_HOST = ConfigOptions
            .key("single.host")
            .stringType()
            .defaultValue(Protocol.DEFAULT_HOST);
    public static final ConfigOption<Integer> SINGLE_PORT = ConfigOptions
            .key("single.port")
            .intType()
            .defaultValue(Protocol.DEFAULT_PORT);
    // cluster
    public static final ConfigOption<String> PASSWORD = ConfigOptions
            .key("password")
            .stringType()
            .noDefaultValue();
    public static final ConfigOption<String> COMMAND = ConfigOptions
            .key("command")
            .stringType()
            .noDefaultValue();
    public static final ConfigOption<Integer> DB_NUM = ConfigOptions
            .key("db-num")
            .intType()
            .defaultValue(Protocol.DEFAULT_DATABASE);
    public static final ConfigOption<Integer> CONNECTION_TIMEOUT_MS = ConfigOptions
            .key("connection.timeout-ms")
            .intType()
            .defaultValue(Protocol.DEFAULT_TIMEOUT);
    public static final ConfigOption<Integer> CONNECTION_MAX_TOTAL = ConfigOptions
            .key("connection.max-total")
            .intType()
            .defaultValue(GenericObjectPoolConfig.DEFAULT_MAX_TOTAL);
    public static final ConfigOption<Integer> CONNECTION_MAX_IDLE = ConfigOptions
            .key("connection.max-idle")
            .intType()
            .defaultValue(GenericObjectPoolConfig.DEFAULT_MAX_IDLE);
    public static final ConfigOption<Integer> CONNECTION_MIN_IDLE = ConfigOptions
            .key("connection.min-idle")
            .intType()
            .defaultValue(GenericObjectPoolConfig.DEFAULT_MIN_IDLE);
    public static final ConfigOption<Boolean> CONNECTION_TEST_ON_BORROW = ConfigOptions
            .key("connection.test-on-borrow")
            .booleanType()
            .defaultValue(GenericObjectPoolConfig.DEFAULT_TEST_ON_BORROW);
    public static final ConfigOption<Boolean> CONNECTION_TEST_ON_RETURN = ConfigOptions
            .key("connection.test-on-return")
            .booleanType()
            .defaultValue(GenericObjectPoolConfig.DEFAULT_TEST_ON_RETURN);
    public static final ConfigOption<Boolean> CONNECTION_TEST_WHILE_IDLE = ConfigOptions
            .key("connection.test-while-idle")
            .booleanType()
            .defaultValue(GenericObjectPoolConfig.DEFAULT_TEST_WHILE_IDLE);
    public static final ConfigOption<String> LOOKUP_ADDITIONAL_KEY = ConfigOptions
            .key("lookup.additional-key")
            .stringType()
            .noDefaultValue();
}
