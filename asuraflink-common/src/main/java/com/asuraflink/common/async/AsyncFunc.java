package com.asuraflink.common.async;

import com.asuraflink.common.async.config.DimensionSourceConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public abstract class AsyncFunc<IN, OUT> extends RichAsyncFunction<IN, OUT> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncFunc.class);
    private static final long serialVersionUID = 1L;

    protected DimensionSourceConfig dimensionSourceConfig;
    private transient volatile Cache<String, String> cache;

    public AsyncFunc() {
    }

    /**
     * 初始化连接、缓存
     */
    public abstract void init(Configuration parameters);
    public abstract void shutdown();

    @Override
    public void open(Configuration parameters) throws Exception {

        init(parameters);

        if(dimensionSourceConfig.isHasCache()){
            cache = CacheBuilder.newBuilder()
                    .initialCapacity(10_000)
                    .maximumSize(20_000)
                    .expireAfterAccess(1, TimeUnit.HOURS)
                    .build();
        } else {
            LOGGER.warn("No cache!");
        }
    }

    @Override
    public void close() throws Exception {
        shutdown();
        cache.invalidateAll();
    }

    @Override
    public void asyncInvoke(IN input, ResultFuture<OUT> resultFuture) throws Exception {
        doAsyncInvoke(input, resultFuture);
    }

    public abstract void doAsyncInvoke(IN input, ResultFuture<OUT> resultFuture);


    public DimensionSourceConfig getDimensionSourceConfig() {
        return dimensionSourceConfig;
    }

    public void setJdbcDimensionSourceConfig(DimensionSourceConfig dimensionSourceConfig) {
        this.dimensionSourceConfig = dimensionSourceConfig;
    }
}
