package com.asuraflink.sql.dynamic.redis;

import com.asuraflink.sql.dynamic.redis.config.RedisOptions;
import com.asuraflink.sql.dynamic.redis.config.RedisReadOptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.calcite.shaded.com.google.common.base.Preconditions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Slf4j
public class RedisRowDataInputFormat extends RichInputFormat<RowData, InputSplit> implements ResultTypeQueryable<RowData> {

    private final String redisCommand;
    private RedisSingle redisSingle;
    private final RedisOptions redisOptions;
    private final RedisReadOptions readOptions;
    private String cursor;
    private ScanParams scanParams;
    private String additionalKey;
    private InnerIterator<?> currentIter;
    private boolean hasNextScan;

    private transient boolean hasNext;

    public RedisRowDataInputFormat(RedisOptions redisOptions, RedisReadOptions readOptions) {
        Preconditions.checkNotNull(redisOptions, "No options supplied");
        this.redisOptions = redisOptions;
        this.readOptions = readOptions;
        this.redisCommand = redisOptions.getCommand();
        if (redisCommand.toUpperCase().equals("HSCAN")) {
            Preconditions.checkArgument(null != redisOptions.getAdditionalKey(), "hscan must have additionalKey");
            this.additionalKey = redisOptions.getAdditionalKey();
        }
        this.hasNextScan = true;
    }

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();
        this.cursor = ScanParams.SCAN_POINTER_START;
        this.scanParams = new ScanParams()
                .count(readOptions.getScanCount())
                .match(readOptions.getMatchKey());

        this.redisSingle = RedisUtils.buildJedisPool(redisOptions);
    }

    @Override
    public void closeInputFormat() throws IOException {
        super.closeInputFormat();
        if (null != redisSingle) {
            redisSingle.close();
        }
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
        if (hasNextScan) {
            switch (redisCommand.toUpperCase()){
                case "SCAN":
                    ScanResult<String> scanR = redisSingle.scan(cursor, scanParams);
                    cursor = scanR.getCursor();
                    List<String> scanRResult = scanR.getResult();
                    if (scanRResult.size() > 0) {
                        hasNext = true;
                    }
                    Iterator<String> source = scanRResult.iterator();
                    currentIter = new InnerIterator<>(source);
                    hasNextScan = !scanR.isCompleteIteration();
                    break;
                case "HSCAN":
                    ScanResult<Map.Entry<String, String>> hscanR =
                            redisSingle.hscan(additionalKey, cursor, scanParams);
                    cursor = hscanR.getCursor();
                    List<Map.Entry<String, String>> hscanRResult = hscanR.getResult();
                    if (hscanRResult.size() > 0) {
                        hasNext = true;
                    }
                    Iterator<Map.Entry<String, String>> hsource = hscanRResult.iterator();

                    currentIter = new InnerIterator<>(hsource);
                    hasNextScan = !hscanR.isCompleteIteration();
                    break;
                default:
                    log.error("Unsupport redis type: " + redisCommand);
            }
        } else {
            hasNext = false;
        }
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return !hasNext;
    }

    @Override
    public RowData nextRecord(RowData reuse) throws IOException {
        if (currentIter.hasNext()) {
            GenericRowData genericRowData = null;
            switch (redisCommand.toUpperCase()) {
                case "SCAN":
                    genericRowData = new GenericRowData(1);
                    genericRowData.setField(0, currentIter.next());
                    break;

                case "HSCAN":

                    Map.Entry<String, String> next = (Map.Entry<String, String>)currentIter.next();
                    genericRowData = new GenericRowData(2);
                    genericRowData.setField(0, toStringData(next.getKey()));
                    genericRowData.setField(1, toStringData(next.getValue()));
                    break;

                default:
                    log.error("Unsupport redis type: " + redisCommand);
            }
            return genericRowData;
        }
        return null;
    }

    private static StringData toStringData(String value) {
        return StringData.fromString(value);
    }

    @Override
    public void close() throws IOException {
        if(null != redisSingle) {
            redisSingle.close();
        }
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        System.out.println("getProducedType");
        return null;
    }


    public static class Builder {
        private RedisOptions redisOptions;
        private RedisReadOptions readOptions;

        public Builder setOptions(RedisOptions redisOptions, RedisReadOptions readOptions) {
            this.redisOptions = redisOptions;
            this.readOptions = readOptions;
            return this;
        }

        public RedisRowDataInputFormat build() {
            return new RedisRowDataInputFormat(redisOptions, readOptions);
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
