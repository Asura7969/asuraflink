package org.apache.flink.table.runtime.operators.join.lookup;

import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author asura7969
 * @create 2021-03-31-22:46
 */
public class RetryJoinElement {

    private AtomicInteger retryCount;
    private RowData leftKey;
    private long triggerTimestamp;
    private Collector<RowData> out;

    public RetryJoinElement(AtomicInteger retryCount, RowData leftKey, long triggerTimestamp, Collector<RowData> out) {
        this.retryCount = retryCount;
        this.leftKey = leftKey;
        this.triggerTimestamp = triggerTimestamp;
        this.out = out;
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

    public Collector<RowData> getOut() {
        return out;
    }

    public void setTriggerTimestamp(long triggerTimestamp) {
        this.triggerTimestamp = triggerTimestamp;
    }

    public boolean isContinue(int maxRetryCount) {
        return this.retryCount.incrementAndGet() <= maxRetryCount;
    }

    public static RetryJoinElement of(RowData leftKey, long triggerTimestamp, Collector<RowData> out) {
        return new RetryJoinElement(new AtomicInteger(0), leftKey, triggerTimestamp, out);
    }
}
