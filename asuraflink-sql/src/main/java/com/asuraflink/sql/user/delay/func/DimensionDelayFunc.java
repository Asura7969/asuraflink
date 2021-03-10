package com.asuraflink.sql.user.delay.func;

import com.asuraflink.sql.user.delay.utils.RetryerElement;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.inference.TypeInference;

import java.util.Iterator;
import java.util.concurrent.*;

public class DimensionDelayFunc<IN, OUT>
        extends TableFunction<OUT>
        implements CheckpointedFunction {

    private final DelayOption option;

    private final ReadHelper<IN, OUT> readHelper;

    private final DelayQueue<RetryerElement<IN>> delayQueue;

    private final long delayTime;

    private final LinkedBlockingQueue<OUT> paperElements;

    private volatile boolean running;

    /* 是否忽略过期数据, 主要用户 flink failover时候 state数据回放处理; true:忽略，false：不忽略 */
    private final boolean ignoreExpiredData;

    private ListState<OUT> paperElementState;

    private ListState<RetryerElement<IN>> delayState;

    private final ExecutorService collectService;

    private final ExecutorService retryService;

    public DimensionDelayFunc(DelayOption option, ReadHelper<IN, OUT> readHelper) {
        this.option = option;
        this.delayTime = option.getDelayTime().toMillis();
        this.ignoreExpiredData = option.getIgnoreExpiredData();
        this.readHelper = readHelper;
        this.delayQueue = new DelayQueue<>();
        this.paperElements = new LinkedBlockingQueue<>();

        this.collectService = Executors.newFixedThreadPool(1,
                r -> new Thread(r, "dimensionDelayFunc-collectThread"));

        this.retryService = Executors.newFixedThreadPool(1,
                r -> new Thread(r, "dimensionDelayFunc-retryThread"));

    }

    @Override
    public void open(FunctionContext context) throws Exception {
        running = true;
        readHelper.open();
        collectService.execute(new CollectTask());
        retryService.execute(new RetryTask());
    }

    @Override
    public TypeInformation<OUT> getResultType() {
        return readHelper.getResultType();
    }

    @Override
    public TypeInformation<?>[] getParameterTypes(Class<?>[] signature) {
        return readHelper.getParameterTypes(signature);
    }

    public void eval(IN value) {
        OUT tmpResult = readHelper.eval(value);
        if (null == tmpResult) {
            delayQueue.add(new RetryerElement<>(value, delayTime + System.currentTimeMillis()));
        } else {
            paperElements.add(tmpResult);
        }
    }

    public void eval(IN... value) {
        OUT tmpResult = readHelper.eval(value);
        if (null == tmpResult) {
            delayQueue.add(new RetryerElement<>(delayTime + System.currentTimeMillis(), value));
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
        if (retryService.isShutdown()) {
            retryService.shutdown();
        }
        if (collectService.isShutdown()) {
            collectService.shutdown();
        }
        readHelper.close();
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return readHelper.getTypeInference(typeFactory);
    }

    public static <IN, OUT> DimensionDelayFunc<IN, OUT> of(DelayOption option, ReadHelper<IN, OUT> readHelper) {
        return new DimensionDelayFunc<>(option, readHelper);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        paperElementState.clear();
        Iterator<OUT> paperElementsIt = paperElements.iterator();
        while (paperElementsIt.hasNext()) {
            OUT next = paperElementsIt.next();
            paperElementState.add(next);
        }

        delayState.clear();
        Iterator<RetryerElement<IN>> delayIt = delayQueue.iterator();
        while (delayIt.hasNext()) {
            RetryerElement<IN> next = delayIt.next();
            delayState.add(next);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

        ListStateDescriptor<OUT> paperElementStateDesc =
                new ListStateDescriptor<>("paperElementState-listState",
                        TypeInformation.of(new TypeHint<OUT>() {}));
        paperElementState = context.getOperatorStateStore().getListState(paperElementStateDesc);

        ListStateDescriptor<RetryerElement<IN>> delayStateDesc =
                new ListStateDescriptor<>("delayState-listState",
                        TypeInformation.of(new TypeHint<RetryerElement<IN>>() {}));
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

    /**
     * 延迟重试任务
     */
    class RetryTask implements Runnable {
        @Override
        public void run() {
            try {
                while (running) {
                    RetryerElement<IN> take = delayQueue.take();
                    OUT tmpResult = readHelper.eval(take.getValue());
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
        }
    }

    /**
     * 往下游 operator 发送数据
     */
    class CollectTask implements Runnable {
        @Override
        public void run() {
            try {
                while (running) {
                    OUT e = paperElements.take();
                    collect(e);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }
}
