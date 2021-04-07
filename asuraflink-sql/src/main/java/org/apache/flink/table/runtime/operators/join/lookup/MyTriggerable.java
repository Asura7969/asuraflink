package org.apache.flink.table.runtime.operators.join.lookup;

import org.apache.flink.streaming.api.operators.InternalTimer;

/**
 * @author asura7969
 * @create 2021-04-06-21:20
 */
public interface MyTriggerable {
    // 暂时没使用 eventTime
//    void onEventTime(Long timer) throws Exception;

    /** Invoked when a processing-time timer fires. */
    void onProcessingTime(Long timestamp) throws Exception;
}
