package com.asuraflink.sql.dynamic.redis;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.calcite.shaded.com.google.common.base.Preconditions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import static com.asuraflink.sql.dynamic.redis.RedisDynamicTableFactory.*;
import static com.asuraflink.sql.dynamic.redis.RedisDynamicTableFactory.DB_NUM;

@Slf4j
public class RedisRowDataInputFormat implements InputFormat<RowData, InputSplit> {

    private final String redisCommand;
    private final RedisSingle redisSingle;
    private String cursor;
    private final ScanParams scanParams;
    private String additionalKey;
    private InnerIterator<?> currentIter;

    private transient boolean hasNext;

    public RedisRowDataInputFormat(int count, String matchKey, String additionalKey, String redisCommand, ReadableConfig options) {
        this.redisCommand = redisCommand;
        if (redisCommand.toUpperCase().equals("HSCAN")) {
            Preconditions.checkArgument(null != additionalKey, "hscan must have additionalKey");
            this.additionalKey = additionalKey;
        }
        this.cursor = ScanParams.SCAN_POINTER_START;
        this.scanParams = new ScanParams().count(count).match(matchKey);

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
    public void configure(Configuration parameters) {

    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
        return cachedStatistics;
    }

    @Override
    public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
        return new GenericInputSplit[]{new GenericInputSplit(0, 1)};
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
        return new DefaultInputSplitAssigner(inputSplits);
    }

    @Override
    public void open(InputSplit split) throws IOException {
        switch (redisCommand.toUpperCase()){
            case "SCAN":
                ScanResult<String> scanR = redisSingle.scan(cursor, scanParams);
                cursor = scanR.getCursor();
                Iterator<String> source = scanR.getResult().iterator();
                currentIter = new InnerIterator<>(source);
                hasNext = !scanR.isCompleteIteration();

            case "HSCAN":
                ScanResult<Map.Entry<String, String>> hscanR =
                        redisSingle.hscan(additionalKey, cursor, scanParams);
                cursor = hscanR.getCursor();
                Iterator<Map.Entry<String, String>> hsource = hscanR.getResult().iterator();
                currentIter = new InnerIterator<>(hsource);
                hasNext = !hscanR.isCompleteIteration();

            default:
                log.error("Unsupport redis type: " + redisCommand);
        }
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return !hasNext;
    }

    @Override
    public RowData nextRecord(RowData reuse) throws IOException {
        GenericRowData genericRowData = null;
        switch (redisCommand.toUpperCase()) {
            case "SCAN":
                genericRowData = new GenericRowData(1);
                genericRowData.setField(0, currentIter.next);
            case "HSCAN":
                Map.Entry<String, String> next = (Map.Entry<String, String>)currentIter.next;
                genericRowData = new GenericRowData(2);
                genericRowData.setField(0, next.getKey());
                genericRowData.setField(1, next.getValue());
            default:
                log.error("Unsupport redis type: " + redisCommand);
        }
        return genericRowData;
    }

    @Override
    public void close() throws IOException {
        if(null != redisSingle) {
            redisSingle.close();
        }
    }


    public static class Builder {
        private int count = 100;
        private String matchKey;
        private String additionalKey;
        private String redisCommand;
        private ReadableConfig options;

        public Builder setOptions(ReadableConfig options) {
            this.options = options;
            this.additionalKey = options.get(ADDITIONAL_KEY);
            this.redisCommand = options.get(COMMAND);
            this.matchKey = options.get(MATCH_KEY);
            this.count = options.get(SCAN_COUNT);
            return this;
        }

        public RedisRowDataInputFormat build() {
            return new RedisRowDataInputFormat(count, matchKey, additionalKey, redisCommand, options);
        }
    }

    static class InnerIterator<T> implements Iterator<T> {
        private final Iterator<T> sourceIterator;
        private T next;

        public InnerIterator(Iterator<T> sourceIterator) {
            this.sourceIterator = sourceIterator;
        }

        @Override
        public boolean hasNext() {
            return sourceIterator.hasNext();
        }

        @Override
        public T next() {
            next = sourceIterator.next();
            return next;
        }
    }
}
