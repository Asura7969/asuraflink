package com.asuraflink.sql.user.delay.lookup;

import com.asuraflink.sql.user.delay.RedisReadHelper;
import com.asuraflink.sql.user.delay.func.DelayOption;
import com.asuraflink.sql.user.delay.func.DimensionDelayFunc;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;

public class RedisLookup implements LookupTableSource {

    private final DelayOption delayOption;
    private final RedisReadHelper redisReadHelper;

    public RedisLookup(DelayOption delayOption, RedisReadHelper redisReadHelper) {
        this.delayOption = delayOption;
        this.redisReadHelper = redisReadHelper;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        return TableFunctionProvider.of(new DimensionDelayFunc<>(delayOption, redisReadHelper));
    }

    @Override
    public DynamicTableSource copy() {
        return new RedisLookup(delayOption, redisReadHelper);
    }

    @Override
    public String asSummaryString() {
        return "Redis";
    }
}
