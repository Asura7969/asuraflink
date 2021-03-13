package com.asuraflink.sql.dynamic.redis;

import com.asuraflink.sql.dynamic.redis.config.RedisLookupOptions;
import com.asuraflink.sql.dynamic.redis.config.RedisOptions;
import com.asuraflink.sql.dynamic.redis.config.RedisReadOptions;
import org.apache.flink.calcite.shaded.com.google.common.base.Preconditions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.*;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

public class RedisDynamicTableSource implements LookupTableSource, ScanTableSource {
    private final RedisOptions redisOptions;
    private final RedisReadOptions redisReadOptions;
    private final RedisLookupOptions redisLookupOptions;
    private final TableSchema physicalSchema;

    public RedisDynamicTableSource(RedisOptions redisOptions, RedisLookupOptions redisLookupOptions, RedisReadOptions redisReadOptions, TableSchema physicalSchema) {
        this.redisOptions = redisOptions;
        this.redisLookupOptions = redisLookupOptions;
        this.redisReadOptions = redisReadOptions;
        this.physicalSchema = physicalSchema;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        Preconditions.checkArgument(context.getKeys().length == 1 && context.getKeys()[0].length == 1,
                "Redis source only supports lookup by single key");
        validate();
        return TableFunctionProvider.of(new RedisRowDataLookupFunction(redisOptions, redisLookupOptions));
    }

    @Override
    public DynamicTableSource copy() {
        return new RedisDynamicTableSource(redisOptions, redisLookupOptions, redisReadOptions, physicalSchema);
    }

    @Override
    public String asSummaryString() {
        return "Redis Dynamic Table Source";
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        validate();
        return InputFormatProvider.of(new RedisRowDataInputFormat.Builder().setOptions(redisOptions, redisReadOptions).build());
    }

    private void validate() {
        int fieldCount = physicalSchema.getFieldCount();
        if (fieldCount != 2) {
            throw new ValidationException("Redis source only supports 2 columns");
        }
        DataType[] dataTypes = physicalSchema.getFieldDataTypes();
        for (int i = 0; i < fieldCount; i++) {
            if (!dataTypes[i].getLogicalType().getTypeRoot().equals(LogicalTypeRoot.VARCHAR)) {
                throw new ValidationException("Redis connector only supports STRING type");
            }
        }
    }
}
