package com.asuraflink.sql.user.delay.func;

import com.asuraflink.sql.user.delay.model.Pay;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

public class PaySourceFunc extends RichSourceFunction<Pay> {
    private SourceGenerator generator;
    private long sleepTime;
    public LinkedBlockingQueue<Pay> queue;

    public PaySourceFunc(long sleepTime) {
        this.generator = new SourceGenerator();
        this.sleepTime = sleepTime;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        new Thread(new Runnable() {
            @Override
            public void run() {
                generator.generatorPay(sleepTime);
            }
        }).start();

        queue = generator.queue;
    }

    @Override
    public void run(SourceContext<Pay> ctx) throws Exception {
        for (;;) {
            Object lock = ctx.getCheckpointLock();
            Pay pay = queue.take();
            synchronized (lock) {
                ctx.collect(pay);
            }
        }

    }

    @Override
    public void cancel() {

    }
}
