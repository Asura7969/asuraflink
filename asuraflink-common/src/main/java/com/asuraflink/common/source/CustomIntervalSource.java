package com.asuraflink.common.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Random;

/**
 * 定时随机产生数据
 * @param <T>
 */
abstract public class CustomIntervalSource<T> extends RichSourceFunction<T> {

    public static final int DEFAULT_INTERVAL_OF_SECONDS = 2;

    private Random random = new Random();

    private int interval;

    private volatile boolean isRunning;

    public CustomIntervalSource() {
    }

    private CustomIntervalSource(int interval) {
        this.interval = interval * 1000;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        interval = getIntervalOfSeconds();
        isRunning = true;
    }

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        while (isRunning){
            ctx.collect(getMsg(random));
            Thread.sleep(interval);
        }
    }

    protected int getIntervalOfSeconds(){
        return DEFAULT_INTERVAL_OF_SECONDS * 1000;
    }

    abstract protected T getMsg(Random random);

    @Override
    public void cancel() {
        isRunning = false;
    }
}
