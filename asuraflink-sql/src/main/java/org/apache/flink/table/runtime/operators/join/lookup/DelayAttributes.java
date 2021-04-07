package org.apache.flink.table.runtime.operators.join.lookup;

import org.apache.flink.api.common.time.Time;

import java.util.Objects;

/**
 * 定义延迟策略,默认为重试3次，每次间隔5秒
 *
 * <pre>{@code
 * class MyFunction extends TableFunction<Integer> implements DelayAttributes{
 *   public void eval(Integer leftKey) {
 *
 *     // 通过leftKey 查询其他外部资源的数据
 *     Row right = connect.join(leftKey);
 *
 *     if (collectIsNotNull(right)) {
 *
 *         // 判断不为null 才最终调用 TableFunction.collect 方法
 *         collect(Row.of(leftKey, right));
 *     }
 *   }
 * }
 * }</pre>
 * @author asura7969
 * @create 2021-04-03-14:23
 */
public interface DelayAttributes {

    /**
     * 默认策略：重试3次, 每次间隔5s
     * 可通过重写该方法实现自定义策略
     * @return 重新join策略
     */
    default DelayStrategy getDelayStrategy() {
        return new DelayStrategy(3, Time.seconds(5));
    }

    default boolean collectIsNotNull (Object rightOutput) {
        return !Objects.isNull(rightOutput);
    }
}
