package org.apache.flink.table.runtime.operators.join.lookup;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.taskmanager.DispatcherThreadFactory;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.collector.TableFunctionCollector;
import org.apache.flink.table.runtime.generated.GeneratedCollector;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.concurrent.ThreadFactory;

/**
 * 新增维表延迟join功能
 * 重写 {@link LookupJoinRunner}
 * @author asura7969
 * @create 2021-03-30-21:50
 */
public class LookupJoinRunner extends ProcessFunction<RowData, RowData> implements CheckpointedFunction, MyTriggerable {
    private static final long serialVersionUID = 1642851357194351122L;

    public static final ThreadGroup TRIGGER_THREAD_GROUP = new ThreadGroup("LookupJoin-Triggers");

    private final GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedFetcher;
    private final GeneratedCollector<TableFunctionCollector<RowData>> generatedCollector;
    private final boolean isLeftOuterJoin;
    private final int tableFieldsCount;

    private transient FlatMapFunction<RowData, RowData> fetcher;
    protected transient TableFunctionCollector<RowData> collector;
    private transient GenericRowData nullRow;
    private transient JoinedRowData outRow;

    private int retryCount;
    private long delayMilliseconds;
    private boolean delayMark;
    private ListState<RetryJoinElement> state;
    private List<RetryJoinElement> memList;
    private MyTimeService timeService;
//    private Map<Long, List<RetryJoinElement>> mapState;

    public LookupJoinRunner(
            GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedFetcher,
            GeneratedCollector<TableFunctionCollector<RowData>> generatedCollector,
            boolean isLeftOuterJoin,
            int tableFieldsCount) {
        this.generatedFetcher = generatedFetcher;
        this.generatedCollector = generatedCollector;
        this.isLeftOuterJoin = isLeftOuterJoin;
        this.tableFieldsCount = tableFieldsCount;
        // 判断是否需要延迟 join
        if (generatedFetcher.getReferences()[0] instanceof DelayAttributes) {
            this.delayMark = true;
            DelayStrategy delayStrategy = ((DelayAttributes)generatedFetcher.getReferences()[0]).getDelayStrategy();
            this.retryCount = delayStrategy.getRetryCount();
            this.delayMilliseconds = delayStrategy.getDelayTime().toMilliseconds();
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.fetcher = generatedFetcher.newInstance(getRuntimeContext().getUserCodeClassLoader());
        this.collector =
                generatedCollector.newInstance(getRuntimeContext().getUserCodeClassLoader());

        FunctionUtils.setFunctionRuntimeContext(fetcher, getRuntimeContext());
        FunctionUtils.setFunctionRuntimeContext(collector, getRuntimeContext());
        FunctionUtils.openFunction(fetcher, parameters);
        FunctionUtils.openFunction(collector, parameters);

        this.nullRow = new GenericRowData(tableFieldsCount);
        this.outRow = new JoinedRowData();

        String name = getRuntimeContext().getTaskNameWithSubtasks();
        ThreadFactory timerThreadFactory =
                new DispatcherThreadFactory(
                        TRIGGER_THREAD_GROUP, "Time Trigger for " + name);
        this.timeService = new MyTimeService(timerThreadFactory, this);
    }

    @Override
    public void processElement(RowData in, Context ctx, Collector<RowData> out) throws Exception {
        collector.setCollector(out);
        collector.setInput(in);
        collector.reset();

        // fetcher has copied the input field when object reuse is enabled
        fetcher.flatMap(in, getFetcherCollector());

        if (delayMark) {
            if (!collector.isCollected()) {
                // 没有 join到数据,放入state,延迟 join
                long nextJoinTime = timeService.getCurrentProcessingTime() + delayMilliseconds;
                timeService.registerProcessingTimeTimer(nextJoinTime);
                memList.add(RetryJoinElement.of(in, nextJoinTime));
            }
        } else {
            if (isLeftOuterJoin && !collector.isCollected()) {
                outRow.replace(in, nullRow);
                outRow.setRowKind(in.getRowKind());
                out.collect(outRow);
            }
        }
    }

    @Override
    public void onProcessingTime(Long timestamp) throws Exception {
        List<RetryJoinElement> newList = new ArrayList<>();
        memList.forEach(next -> {
            if (next.getTriggerTimestamp() <= timestamp) {
                System.out.println(timestamp - next.getTriggerTimestamp() + "ms : " + next.getLeftKey().getString(0));
                try {
                    fetcher.flatMap(next.getLeftKey(), getFetcherCollector());
                } catch (Exception e) {
                    e.printStackTrace();
                }
                if(!collector.isCollected()) {
                    if (next.isContinue(retryCount)) {
                        long nextJoinTime = timestamp + delayMilliseconds;
                        next.setTriggerTimestamp(nextJoinTime);
                        newList.add(next);
                        timeService.registerProcessingTimeTimer(nextJoinTime);
                        System.out.println("重新注册:" + next.getLeftKey().getString(0));
                    } else if (isLeftOuterJoin){
                        outRow.replace(next.getLeftKey(), nullRow);
                        outRow.setRowKind(next.getLeftKey().getRowKind());
                        getFetcherCollector().collect(outRow);
                    } else {
                        System.out.println("丢弃数据，没有 join 到:" + next.getLeftKey());
                    }
                }
            } else {
                newList.add(next);
            }
        });
        memList.clear();
        memList = newList;
    }

    public Collector<RowData> getFetcherCollector() {
        return collector;
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (fetcher != null) {
            FunctionUtils.closeFunction(fetcher);
        }
        if (collector != null) {
            FunctionUtils.closeFunction(collector);
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        if (delayMark) {
            state.clear();
            if (memList.size() > 0) {
                state.addAll(memList);
            }
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        if (delayMark) {
            System.out.println("启用延迟功能");
            ListStateDescriptor<RetryJoinElement> retryJoinListStateDesc =
                    new ListStateDescriptor<>("retryJoinListState", RetryJoinElement.class);
            state = context.getOperatorStateStore().getListState(retryJoinListStateDesc);
            memList = new ArrayList<>();
            if (context.isRestored()) {
                for (RetryJoinElement retryJoinElement : state.get()) {
                    memList.add(retryJoinElement);
                }
            }
        }
    }
}
