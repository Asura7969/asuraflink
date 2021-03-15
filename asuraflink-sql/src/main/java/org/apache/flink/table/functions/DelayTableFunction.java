package org.apache.flink.table.functions;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author asura7969
 * @create 2021-03-15-8:06
 *
 * TODO: 延迟算子，未完成
 */
public abstract class DelayTableFunction<T> extends TableFunction<T> {

    private static final long serialVersionUID = 7727751547146301159L;

    private DelayOperatorImpl delayOperator;

//    public DelayTableFunction() {
//        delayOperator = new DelayOperatorImpl();
//    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        delayOperator = new DelayOperatorImpl();
        delayOperator.open();
    }

    private void userFunction(Object obj) {

    }

    public void eval(Object obj) {
        // 这里调用的 collect() （codeGenerate生成的TableCollect）
        // todo:需要阻止该调用（collect()）
    }



    @Override
    public void setCollector(Collector<T> collector) {
        super.setCollector(collector);
    }

    private class DelayOperatorImpl extends DelayOperator<Object, T> {

        private Map<Long, List<DelayElement<Object>>> state;
        private long delay;
        private long maxRetry;
        private static final long serialVersionUID = 2022364490868054866L;
        private Collector<T> collector;

        public void setCollector(Collector<T> collector) {
            this.collector = collector;
        }

        @Override
        public void open() throws Exception {
            super.open();
            state = new HashMap<>();
        }

        @Override
        public void processElement(StreamRecord<Object> element) throws Exception {
            Object in = element.getValue();
            DelayElement<Object> delayElement = DelayElement.build(in);


            long currentTime = timeService.getCurrentProcessingTime();

            long registerTime = currentTime + delay;
            List<DelayElement<Object>> list = state.get(registerTime);
            if (null == list) {
                state.put(registerTime, list);
            }
            list.add(delayElement);
            timeService.registerTimer(registerTime, new ProcessingTimeCallImpl());
        }

        private class ProcessingTimeCallImpl implements ProcessingTimeCallback {

            @Override
            public void onProcessingTime(long timestamp) throws Exception {
                Iterator<DelayElement<Object>> iterator = state.get(timestamp).iterator();
                while (iterator.hasNext()) {
                    DelayElement<Object> e = iterator.next();
                    if (e.getAndIncrement() < maxRetry) {
                        eval(e.getValue());
                        // 判断collect 标识是否发送，发送则有数据，没发送则无数据，重新注册
                        timeService.registerTimer(timeService.getCurrentProcessingTime() + delay, this);

                    } else {
                        // flink metric
                    }
                    // 去除 list中旧的延迟数据
                    iterator.remove();
                }

            }
        }
    }

    private static class DelayElement<T> {
        private T value;
        private AtomicInteger retryCount;

        public DelayElement(T value, AtomicInteger retryCount) {
            this.value = value;
            this.retryCount = retryCount;
        }

        public T getValue() {
            return value;
        }

        public int getAndIncrement() {
            return this.retryCount.getAndIncrement();
        }
        public static <T> DelayElement<T> build(T value) {
            return new DelayElement(value, new AtomicInteger(0));
        }
    }
}
