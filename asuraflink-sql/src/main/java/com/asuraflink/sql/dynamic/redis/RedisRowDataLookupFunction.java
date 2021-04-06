package com.asuraflink.sql.dynamic.redis;

import com.asuraflink.sql.dynamic.redis.config.RedisLookupOptions;
import com.asuraflink.sql.dynamic.redis.config.RedisOptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.calcite.shaded.com.google.common.base.Preconditions;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.runtime.operators.join.lookup.DelayAttributes;
import org.apache.flink.table.runtime.operators.join.lookup.DelayStrategy;

/**
 * https://cloud.tencent.com/developer/article/1560553
 */
@Slf4j
public class RedisRowDataLookupFunction extends TableFunction<RowData> implements DelayAttributes {
    private static final long serialVersionUID = 1L;

    private final RedisOptions redisOptions;
    private final RedisLookupOptions redisLookupOptions;
    private final String command;
    private final String additionalKey;

    private RedisSingle redisSingle;

    public RedisRowDataLookupFunction(RedisOptions redisOptions, RedisLookupOptions redisLookupOptions) {
        Preconditions.checkNotNull(redisOptions, "No options supplied");
        Preconditions.checkNotNull(redisLookupOptions, "No options supplied");
        this.redisOptions = redisOptions;
        this.redisLookupOptions = redisLookupOptions;
        this.additionalKey = redisOptions.getAdditionalKey();

        command = redisOptions.getCommand().toUpperCase();
        Preconditions.checkArgument(command.equals("GET") ||
                command.equals("HGET"), "Redis table source only supports GET and HGET commands");

    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        redisSingle = RedisUtils.buildJedisPool(redisOptions);
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
        StringData rightOutput = StringData.fromString(value);
        if (!collectIsNull(rightOutput)) {
            RowData result = GenericRowData.of(key, rightOutput);

//        cache.put(lookupKey, result);
            collect(result);
        }


    }
}
