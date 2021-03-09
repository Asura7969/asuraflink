package com.asuraflink.sql.user.delay.func;

import com.asuraflink.sql.user.delay.model.Order;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.concurrent.LinkedBlockingQueue;

public class OrderSourceFunc extends RichSourceFunction<Order> {
    private SourceGenerator generator;
    private long sleepTime;
    private LinkedBlockingQueue<Order> deque;

    public OrderSourceFunc(long sleepTime) {
        this.generator = new SourceGenerator();
        this.sleepTime = sleepTime;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        new Thread(new Runnable() {
            @Override
            public void run() {
                generator.generatorOrder(sleepTime);
            }
        }).start();
        deque = generator.orderQueue;
    }

    @Override
    public void run(SourceContext<Order> ctx) throws Exception {
        for (;;) {
            Object lock = ctx.getCheckpointLock();
            Order order = deque.take();
//            System.out.println(order.toString());
            synchronized (lock) {
                ctx.collect(order);
            }
        }

    }

    @Override
    public void cancel() {

    }
}
