package org.apache.flink.table.runtime.operators.join.lookup;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.collector.TableFunctionCollector;
import org.apache.flink.table.runtime.generated.GeneratedCollector;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 新增维表延迟join功能
 * 重写 {@link KeyByLookupJoinRunner}
 * @author asura7969
 * @create 2021-03-30-21:50
 */
public class KeyByLookupJoinRunner extends ProcessFunction<RowData, RowData> {
    private static final long serialVersionUID = 1642851357194351121L;

    private static final Logger logger = LoggerFactory.getLogger(KeyByLookupJoinRunner.class);

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
    private MapState<Long, List<RetryJoinElement>> mapState;

    public KeyByLookupJoinRunner(
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
            logger.info("开启延迟join");
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

        this.mapState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>(
                        "retryJoinMapState",
                        BasicTypeInfo.LONG_TYPE_INFO, new ListTypeInfo<>(RetryJoinElement.class))
        );
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
                long nextJoinTime = ctx.timerService().currentProcessingTime() + delayMilliseconds;
                ctx.timerService().registerProcessingTimeTimer(nextJoinTime);
                List<RetryJoinElement> retryJoinElements = mapState.get(nextJoinTime);
                if (Objects.isNull(retryJoinElements)) {
                    retryJoinElements = new ArrayList<>();
                }
                retryJoinElements.add(RetryJoinElement.of(in, nextJoinTime));
                mapState.put(nextJoinTime, retryJoinElements);
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
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<RowData> out) throws Exception {
        List<RetryJoinElement> retryJoinElements = mapState.get(timestamp);
        List<RetryJoinElement> newList = new ArrayList<>();
        long nextJoinTime = timestamp + delayMilliseconds;

        retryJoinElements.forEach(next -> {
            if (next.getTriggerTimestamp() <= timestamp) {
                logger.debug("{}ms : {}", timestamp - next.getTriggerTimestamp(), next.getLeftKey().getString(0));
                try {
                    fetcher.flatMap(next.getLeftKey(), getFetcherCollector());
                } catch (Exception e) {
                    e.printStackTrace();
                }
                if(!collector.isCollected()) {
                    if (next.isContinue(retryCount)) {
                        next.setTriggerTimestamp(nextJoinTime);
                        newList.add(next);
                        ctx.timerService().registerProcessingTimeTimer(nextJoinTime);
                        logger.info("重新注册: {}", next.getLeftKey().getString(0));
                    } else if (isLeftOuterJoin){
                        outRow.replace(next.getLeftKey(), nullRow);
                        outRow.setRowKind(next.getLeftKey().getRowKind());
                        next.getOut().collect(outRow);
                    } else {
                        logger.info("丢弃数据，没有 join 到:{}", next.getLeftKey());
                    }
                }
            }
            else {
                newList.add(next);
            }
        });
        if (newList.isEmpty()) {
            mapState.remove(timestamp);
        } else {
            mapState.put(nextJoinTime, newList);
        }
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
}
