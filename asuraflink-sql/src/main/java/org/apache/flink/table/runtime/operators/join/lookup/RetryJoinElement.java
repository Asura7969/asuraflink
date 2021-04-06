package org.apache.flink.table.runtime.operators.join.lookup;

import org.apache.flink.table.data.RowData;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author asura7969
 * @create 2021-03-31-22:46
 */
public class RetryJoinElement {

    private AtomicInteger retryCount;
    private RowData leftKey;
    private long triggerTimestamp;

    public RetryJoinElement(AtomicInteger retryCount, RowData leftKey, long triggerTimestamp) {
        this.retryCount = retryCount;
        this.leftKey = leftKey;
        this.triggerTimestamp = triggerTimestamp;
    }

    public AtomicInteger getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(AtomicInteger retryCount) {
        this.retryCount = retryCount;
    }

    public RowData getLeftKey() {
        return leftKey;
    }

    public void setLeftKey(RowData leftKey) {
        this.leftKey = leftKey;
    }

    public long getTriggerTimestamp() {
        return triggerTimestamp;
    }

    public void setTriggerTimestamp(long triggerTimestamp) {
        this.triggerTimestamp = triggerTimestamp;
    }

    public boolean isContinue(int maxRetryCount) {
        return this.retryCount.incrementAndGet() <= maxRetryCount;
    }

    public static RetryJoinElement of(RowData leftKey, long triggerTimestamp) {
        return new RetryJoinElement(new AtomicInteger(0), leftKey, triggerTimestamp);
    }
}
