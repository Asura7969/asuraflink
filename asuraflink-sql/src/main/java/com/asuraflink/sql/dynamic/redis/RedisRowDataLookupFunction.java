package com.asuraflink.sql.dynamic.redis;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.calcite.shaded.com.google.common.base.Preconditions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import redis.clients.jedis.JedisPool;

import static com.asuraflink.sql.dynamic.redis.RedisDynamicTableFactory.*;

/**
 * https://cloud.tencent.com/developer/article/1560553
 */
@Slf4j
public class RedisRowDataLookupFunction extends TableFunction<RowData> {
    private static final long serialVersionUID = 1L;

    private final ReadableConfig options;
    private final String command;
    private final String additionalKey;

    private RedisSingle redisSingle;

    public RedisRowDataLookupFunction(ReadableConfig options) {
        Preconditions.checkNotNull(options, "No options supplied");
        this.options = options;
        this.additionalKey = options.get(ADDITIONAL_KEY);

        command = options.get(COMMAND).toUpperCase();
        Preconditions.checkArgument(command.equals("GET") ||
                command.equals("HGET"), "Redis table source only supports GET and HGET commands");

    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);

        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        poolConfig.setMaxIdle(options.get(CONNECTION_MAX_IDLE));
        poolConfig.setMinIdle(options.get(CONNECTION_MIN_IDLE));
        poolConfig.setMaxTotal(options.get(CONNECTION_MAX_TOTAL));

        JedisPool jedisPool = new JedisPool(poolConfig, options.get(SINGLE_HOST),
                options.get(SINGLE_PORT), options.get(CONNECTION_TIMEOUT_MS), options.get(PASSWORD),
                options.get(DB_NUM));

        redisSingle = new RedisSingle(jedisPool);
    }

    @Override
    public void close() throws Exception {
        super.close();
        redisSingle.close();
    }

    public void eval(Object obj) {
        RowData lookupKey = GenericRowData.of(obj);

        StringData key = lookupKey.getString(0);
        String value = command.equals("GET") ? redisSingle.get(key.toString()) : redisSingle.hget(additionalKey, key.toString());
        RowData result = GenericRowData.of(key, StringData.fromString(value));

//        cache.put(lookupKey, result);
        collect(result);

    }
}
