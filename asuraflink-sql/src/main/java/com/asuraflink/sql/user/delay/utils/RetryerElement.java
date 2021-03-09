package com.asuraflink.sql.user.delay.utils;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class RetryerElement<T> implements Delayed {
    private T value;
    private long time;
    private int times = 1;

    public RetryerElement(T value, long time, int times) {
        this.value = value;
        this.time = time;
        this.times = times;
    }

    public RetryerElement<T> reset(long delay) {
        this.time = System.currentTimeMillis() + delay;
        this.times += 1;
        return this;
    }

    public RetryerElement(T value, long time) {
        this.value = value;
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
        return times;
    }

    public void setTimes(int times) {
        this.times = times;
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
