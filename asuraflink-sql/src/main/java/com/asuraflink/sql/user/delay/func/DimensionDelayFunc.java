package com.asuraflink.sql.user.delay.func;

import com.asuraflink.sql.user.delay.utils.RetryerElement;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.inference.TypeInference;

import java.util.concurrent.DelayQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class DimensionDelayFunc<T> extends TableFunction<T> implements CheckpointedFunction {
    private DelayOption option;
    private ReadHelper<T> readHelper;
    private DelayQueue<RetryerElement<Object>> delayQueue;
    private long delayTime;
    private Object lock = new Object();
    private LinkedBlockingQueue<T> paperElements;
//    private Retryer<Boolean> retryer;

    // todo:add flink state


    private Thread collectThread;
    private Thread retryThread;

    public DimensionDelayFunc(DelayOption option, ReadHelper<T> readHelper) {
        this.option = option;
        this.delayTime = option.getDelayTime().toMillis();
        this.readHelper = readHelper;
        this.delayQueue = new DelayQueue<>();
        this.paperElements = new LinkedBlockingQueue<>();

        // todo: 线程如何优雅的停止？
        this.collectThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    T e = paperElements.take();
                    collect(e);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }, "dimensionDelayFunc-collectThread");

        this.retryThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    RetryerElement<Object> take = delayQueue.take();
                    T tmpResult = readHelper.eval(take.getValue());
                    if (null == tmpResult) {
                        if (!(take.getTimes() + 1 > option.getMaxRetryTimes())) {
                            delayQueue.add(take.reset(delayTime));
                        }
                    } else {
                        paperElements.add(tmpResult);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }, "dimensionDelayFunc-retryThread");

        collectThread.start();
        retryThread.start();

    }

    @Override
    public void open(FunctionContext context) throws Exception {
        readHelper.open();
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

        } else {
            paperElements.add(tmpResult);
        }
    }

    /**
     * watermark 怎么办？
     * checkpoint 恢复时候， queue中过期的怎么办？
     * 解决：snapshot时候加上 snapshot的时间，回复时候按snapshot的时间计算？
     *
     * @param value
     */
    private void ifNullToQueue(Object value) {
        boolean add = delayQueue.add(new RetryerElement<>(value, delayTime + System.currentTimeMillis(), 1));
    }


    @Override
    public void close() throws Exception {
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

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

    }
}
