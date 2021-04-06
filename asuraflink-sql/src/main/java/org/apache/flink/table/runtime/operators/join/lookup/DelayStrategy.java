package org.apache.flink.table.runtime.operators.join.lookup;

import org.apache.flink.api.common.time.Time;

import java.io.Serializable;

/**
 * @author asura7969
 * @create 2021-03-30-21:51
 */
public class DelayStrategy implements Serializable {
    private static final long serialVersionUID = -2834618130720015802L;

    private int retryCount;
    private Time delayTime;

    public int getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public Time getDelayTime() {
        return delayTime;
    }

    public void setDelayTime(Time delayTime) {
        this.delayTime = delayTime;
    }

    public DelayStrategy() {
    }

    public DelayStrategy(int retryCount, Time delayTime) {
        this.retryCount = retryCount;
        this.delayTime = delayTime;
    }
}
