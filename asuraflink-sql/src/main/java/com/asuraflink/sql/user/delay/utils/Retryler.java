package com.asuraflink.sql.user.delay.utils;

import java.util.concurrent.DelayQueue;

public class Retryler {
    private DelayQueue<RetryerElement<Object>> delayQueue;
    private long delayTime;

    public Retryler(long delayTime) {
        this.delayQueue = new DelayQueue<>();
        this.delayTime = delayTime;
    }

    public void add(Object value) {
        delayQueue.add(new RetryerElement<>(value, delayTime + System.currentTimeMillis()));
    }

    public void take() {
//        RetryerElement<Object> take = delayQueue.take();
    }
}
