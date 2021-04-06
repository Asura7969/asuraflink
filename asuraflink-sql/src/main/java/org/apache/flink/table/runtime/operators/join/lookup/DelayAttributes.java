package org.apache.flink.table.runtime.operators.join.lookup;

import org.apache.flink.api.common.time.Time;

import java.util.Objects;

/**
 * 定义延迟策略,默认为重试3次，每次间隔5秒
 *
 * @author asura7969
 * @create 2021-04-03-14:23
 */
public interface DelayAttributes {

    default DelayStrategy getDelayStrategy() {
        return new DelayStrategy(3, Time.seconds(5));
    }

    default boolean collectIsNull (Object rightOutput) {
        return Objects.isNull(rightOutput);
    }
}
