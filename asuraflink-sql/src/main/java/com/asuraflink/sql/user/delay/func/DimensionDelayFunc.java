package com.asuraflink.sql.user.delay.func;

import com.asuraflink.sql.user.delay.utils.RetryerElement;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.inference.TypeInference;

import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class DimensionDelayFunc<T>
        extends TableFunction<T>
        implements CheckpointedFunction, ProcessingTimeCallback {

    private DelayOption option;
    private ReadHelper<T> readHelper;
    private DelayQueue<RetryerElement<Object>> delayQueue;
    private long delayTime;
    private LinkedBlockingQueue<T> paperElements;
    private volatile boolean running;
    /* 是否忽略过期数据, 主要用户 flink failover时候 state数据回放处理; true:忽略，false：不忽略 */
    private boolean ignoreExpiredData;
//    private Retryer<Boolean> retryer;

    private ListState<T> paperElementState;
    private ListState<RetryerElement<Object>> delayState;

    private Thread collectThread;
    private Thread retryThread;

    public DimensionDelayFunc(DelayOption option, ReadHelper<T> readHelper) {
        this.option = option;
        this.delayTime = option.getDelayTime().toMillis();
        this.ignoreExpiredData = option.getIgnoreExpiredData();
        this.readHelper = readHelper;
        this.delayQueue = new DelayQueue<>();
        this.paperElements = new LinkedBlockingQueue<>();

        this.collectThread = new Thread(() -> {
            try {
                while (running) {
                    T e = paperElements.take();
                    collect(e);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }, "dimensionDelayFunc-collectThread");

        this.retryThread = new Thread(() -> {
            try {
                while (running) {
                    RetryerElement<Object> take = delayQueue.take();
                    T tmpResult = readHelper.eval(take.getValue());
                    if (null == tmpResult) {
                        if (!(take.incrementAndGet() > option.getMaxRetryTimes())) {
                            delayQueue.add(take.resetTime(delayTime));
                        }
                    } else {
                        paperElements.add(tmpResult);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }, "dimensionDelayFunc-retryThread");
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        running = true;
        readHelper.open();
        collectThread.start();
        retryThread.start();
    }

    @Override
    public TypeInformation<T> getResultType() {
        return readHelper.getResultType();
    }

    @Override
    public TypeInformation<?>[] getParameterTypes(Class<?>[] signature) {
        return readHelper.getParameterTypes(signature);
    }

    public void eval(Object value) {
        T tmpResult = readHelper.eval(value);
        if (null == tmpResult) {
            delayQueue.add(new RetryerElement<>(value, delayTime + System.currentTimeMillis()));
        } else {
            paperElements.add(tmpResult);
        }
    }

    public void eval(Object... value) {
        T tmpResult = readHelper.eval(value);
        if (null == tmpResult) {
            delayQueue.add(new RetryerElement<>(value, delayTime + System.currentTimeMillis()));
        } else {
            paperElements.add(tmpResult);
        }
    }

    /**
     * watermark 怎么办？
     * checkpoint 恢复时候， queue中过期的怎么办？
     * 解决：snapshot时候加上 snapshot的时间，恢复时候按snapshot的时间计算？
     */



    @Override
    public void close() throws Exception {
        running = false;
        readHelper.close();
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return readHelper.getTypeInference(typeFactory);
    }

    public static <T> DimensionDelayFunc<T> of(DelayOption option, ReadHelper<T> readHelper) {
        return new DimensionDelayFunc<>(option, readHelper);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        paperElementState.clear();
        Iterator<T> paperElementsIt = paperElements.iterator();
        while (paperElementsIt.hasNext()) {
            T next = paperElementsIt.next();
            paperElementState.add(next);
        }

        delayState.clear();
        Iterator<RetryerElement<Object>> delayIt = delayQueue.iterator();
        while (delayIt.hasNext()) {
            RetryerElement<Object> next = delayIt.next();
            delayState.add(next);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

        ListStateDescriptor<T> paperElementStateDesc =
                new ListStateDescriptor<>("paperElementState-listState",
                        TypeInformation.of(new TypeHint<T>() {}));
        paperElementState = context.getOperatorStateStore().getListState(paperElementStateDesc);

        ListStateDescriptor<RetryerElement<Object>> delayStateDesc =
                new ListStateDescriptor<>("delayState-listState",
                        TypeInformation.of(new TypeHint<RetryerElement<Object>>() {}));
        delayState = context.getOperatorStateStore().getListState(delayStateDesc);

        if (context.isRestored()) {
            long currTime = System.currentTimeMillis();
            paperElementState.get().forEach(paperElement -> paperElements.add(paperElement));

            delayState.get().forEach(delayElement -> {
                if (ignoreExpiredData) {
                    if (delayElement.getTime() > currTime) {
                        delayQueue.add(delayElement);
                    }
                } else {
                    delayQueue.add(delayElement);
                }
            });
        }
    }

    @Override
    public void onProcessingTime(long timestamp) throws Exception {

    }
}
