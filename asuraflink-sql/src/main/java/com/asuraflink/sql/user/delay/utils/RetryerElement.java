package com.asuraflink.sql.user.delay.utils;

import java.io.Serializable;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class RetryerElement<T> implements Delayed, Serializable {
    private T value;
    private T[] values;
    private long time;
    private AtomicInteger times = new AtomicInteger(1);

    public RetryerElement(T value, long time, AtomicInteger times) {
        this.value = value;
        this.time = time;
        this.times = times;
    }

    public RetryerElement<T> resetTime(long delay) {
        this.time = System.currentTimeMillis() + delay;
        return this;
    }

    public RetryerElement(T value, long time) {
        this.value = value;
        this.time = time;
    }

    @SafeVarargs
    public RetryerElement(long time, T... values) {
        this.values = values;
        this.time = time;
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public int getTimes() {
        return times.get();
    }

    public int incrementAndGet() {
        return times.incrementAndGet();
    }

    public void setTimes(AtomicInteger times) {
        this.times = times;
    }

    public T[] getValues() {
        return values;
    }

    public void setValues(T[] values) {
        this.values = values;
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return time - System.currentTimeMillis();
    }

    @Override
    public int compareTo(Delayed o) {
        if (this == o) {
            return 0;
        }
        RetryerElement element = (RetryerElement)o;
        long diff = this.time - element.time;
        if (diff <= 0) {
            return -1;
        } else {
            return 1;
        }
    }
}
