package org.apache.flink.table.runtime.operators.join.temporal;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.RowDataUtil;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class TemporalBroadcastRowTimeFunction extends KeyedBroadcastProcessFunction<RowData, RowData, RowData, RowData> {
//public class TemporalBroadcastRwoTimeFunction extends KeyedBroadcastProcessFunction<RowData, RowData, RowData, RowData> {

    private final GeneratedJoinCondition generatedJoinCondition;
    private final InternalTypeInfo<RowData> rightType;
    private final MapStateDescriptor<Long, List<RowData>> temporalBroadcastStateDescriptor;
    private final boolean isLeftOuterJoin;
    private final long intervalUpdate = 10000L;
    private final RowtimeComparator rightRowtimeComparator;

    private transient AtomicLong registeredTimer;
    private transient AtomicInteger registeredSide;
    private transient Long latestUpdateTimer;
    private transient Long nextLeftIndex;
    private transient JoinCondition joinCondition;
    private transient JoinedRowData outRow;
    private transient GenericRowData rightNullRow;
    private final int leftTimeAttribute;
    private final int rightTimeAttribute;
    private Map<Long, RowData> leftState;

    public TemporalBroadcastRowTimeFunction(
            GeneratedJoinCondition generatedJoinCondition,
            int leftTimeAttribute,
            int rightTimeAttribute,
            InternalTypeInfo<RowData> rightType,
            MapStateDescriptor<Long, List<RowData>> temporalBroadcastStateDescriptor,
            boolean isLeftOuterJoin) {
        this.generatedJoinCondition = generatedJoinCondition;
        this.leftTimeAttribute = leftTimeAttribute;
        this.rightTimeAttribute = rightTimeAttribute;
        this.rightType = rightType;
        this.temporalBroadcastStateDescriptor = temporalBroadcastStateDescriptor;
        this.isLeftOuterJoin = isLeftOuterJoin;
        this.rightRowtimeComparator = new RowtimeComparator(rightTimeAttribute);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.joinCondition =
                generatedJoinCondition.newInstance(getRuntimeContext().getUserCodeClassLoader());
        this.outRow = new JoinedRowData();
        this.rightNullRow = new GenericRowData(rightType.toRowSize());
        this.registeredTimer = new AtomicLong(0);
        this.registeredSide = new AtomicInteger(0);
        this.leftState = new HashMap<>();
        this.nextLeftIndex = 0L;
    }

    @Override
    public void processElement(
            RowData leftSideRow,
            ReadOnlyContext ctx,
            Collector<RowData> out) throws Exception {

        long leftTime = getLeftTime(leftSideRow);
        TimerService timerService = ctx.timerService();
        if (registeredSide.get() != 2) {
            registerSmallestTimer(leftTime).ifPresent(timerService::deleteEventTimeTimer);
            timerService.registerEventTimeTimer(getLeftTime(leftSideRow));
            registeredSide.set(1);
        } else {
            if (timerService.currentWatermark() >= registeredTimer.get()) {
                ReadOnlyBroadcastState<Long, List<RowData>> broadcastState = ctx.getBroadcastState(
                        temporalBroadcastStateDescriptor);
                atOnceJoin(broadcastState, timerService, out);


            } else {

            }
        }
        leftState.put(getNextLeftIndex(), leftSideRow);

    }

    @Override
    public void processBroadcastElement(
            RowData rightRow,
            Context ctx,
            Collector<RowData> out) throws Exception {

        BroadcastState<Long, List<RowData>> broadcastState = ctx.getBroadcastState(
                temporalBroadcastStateDescriptor);

        long rowTime = getRightTime(rightRow);

        List<RowData> rowDataList = broadcastState.get(rowTime);
        if (Objects.isNull(rowDataList)) {
            rowDataList = new ArrayList<>();
            broadcastState.put(rowTime, rowDataList);
        }
        rowDataList.add(rightRow);
        registerSmallestTimer(rowTime);
        registeredSide.set(2);

    }

    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext ctx,
            Collector<RowData> out) throws Exception {
        ReadOnlyBroadcastState<Long, List<RowData>> broadcastState = ctx.getBroadcastState(
                temporalBroadcastStateDescriptor);
        atOnceJoin(broadcastState, ctx.timerService(), out);
    }

    private Optional<RowData> join(List<RowData> rightRowsSorted, long leftTime) {
        return latestRightRowToJoin(rightRowsSorted, leftTime);
    }

    private Optional<Long> registerSmallestTimer(long timestamp) throws IOException {
        long registerTime = registeredTimer.get();
        if (registerTime == 0L) {
            registeredTimer.set(timestamp);
            return Optional.empty();
//            timerService.registerEventTimeTimer(timestamp);
        } else if (registerTime > timestamp) {
//            timerService.deleteEventTimeTimer(registerTime);
            registeredTimer.set(timestamp);
            return Optional.of(registerTime);
//            timerService.registerEventTimeTimer(timestamp);
        }
        return Optional.empty();
    }

    private void atOnceJoin(ReadOnlyBroadcastState<Long, List<RowData>> broadcastState,
                            TimerService timerService, Collector<RowData> out) {
        long currentWatermark = timerService.currentWatermark();

        Iterator<Map.Entry<Long, RowData>> leftIterator = leftState.entrySet().iterator();
        final Map<Long, RowData> orderedLeftRecords = new TreeMap<>();
        while (leftIterator.hasNext()) {
            Map.Entry<Long, RowData> entry = leftIterator.next();
            Long leftSeq = entry.getKey();
            RowData leftRow = entry.getValue();
            long lt = getLeftTime(leftRow);
            if (lt <= currentWatermark) {
                orderedLeftRecords.put(leftSeq, leftRow);
                leftIterator.remove();
            }
        }

        orderedLeftRecords.forEach((leftSeq, leftRow) -> {
            try {
                long leftTime = getLeftTime(leftRow);
                List<RowData> rightRowSorted = getRightRowSorted(broadcastState, rightRowtimeComparator);
                Optional<RowData> rightRow = join(rightRowSorted, leftTime);
                if (rightRow.isPresent() && RowDataUtil.isAccumulateMsg(rightRow.get())) {
                    if (joinCondition.apply(leftRow, rightRow.get())) {
                        collectJoinedRow(leftRow, rightRow.get(), out);
                    } else {
                        if (isLeftOuterJoin) {
                            collectJoinedRow(leftRow, rightNullRow, out);
                        }
                    }
                } else {
                    if (isLeftOuterJoin) {
                        collectJoinedRow(leftRow, rightNullRow, out);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        orderedLeftRecords.clear();
    }


    private List<RowData> getRightRowSorted(ReadOnlyBroadcastState<Long, List<RowData>> broadcastState, RowtimeComparator rowtimeComparator) throws Exception {
        List<RowData> rightRows = new ArrayList<>();
        broadcastState.immutableEntries().forEach(row -> {
            rightRows.addAll(row.getValue());
        });
        rightRows.sort(rowtimeComparator);
        return rightRows;
    }


    @Override
    public void close() throws Exception {
        super.close();
    }

    private void collectJoinedRow(RowData leftRow, RowData rightRow, Collector<RowData> collector) {
        outRow.setRowKind(leftRow.getRowKind());
        outRow.replace(leftRow, rightRow);
        collector.collect(outRow);
    }

    private long getNextLeftIndex() throws IOException {
        return nextLeftIndex += 1;
    }

    private long getLeftTime(RowData leftRow) {
        return leftRow.getLong(leftTimeAttribute);
    }

    private long getRightTime(RowData rightRow) {
        return rightRow.getLong(rightTimeAttribute);
    }

    private Optional<RowData> latestRightRowToJoin(List<RowData> rightRowsSorted, long leftTime) {
        return latestRightRowToJoin(rightRowsSorted, 0, rightRowsSorted.size() - 1, leftTime);
    }

    private Optional<RowData> latestRightRowToJoin(
            List<RowData> rightRowsSorted, int low, int high, long leftTime) {
        if (low > high) {
            // exact value not found, we are returning largest from the values smaller then leftTime
            if (low - 1 < 0) {
                return Optional.empty();
            } else {
                return Optional.of(rightRowsSorted.get(low - 1));
            }
        } else {
            int mid = (low + high) >>> 1;
            RowData midRow = rightRowsSorted.get(mid);
            long midTime = getRightTime(midRow);
            int cmp = Long.compare(midTime, leftTime);
            if (cmp < 0) {
                return latestRightRowToJoin(rightRowsSorted, mid + 1, high, leftTime);
            } else if (cmp > 0) {
                return latestRightRowToJoin(rightRowsSorted, low, mid - 1, leftTime);
            } else {
                return Optional.of(midRow);
            }
        }
    }

    private static class RowtimeComparator implements Comparator<RowData>, Serializable {

        private static final long serialVersionUID = 8160134014590716914L;

        private final int timeAttribute;

        private RowtimeComparator(int timeAttribute) {
            this.timeAttribute = timeAttribute;
        }

        @Override
        public int compare(RowData o1, RowData o2) {
            long o1Time = o1.getLong(timeAttribute);
            long o2Time = o2.getLong(timeAttribute);
            return Long.compare(o1Time, o2Time);
        }
    }

}
