package com.asuraflink.window;

import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class IncrementalTrigger extends Trigger<Object, TimeWindow> /*implements CheckpointedFunction*/ {

    // 多久触发一次
    private Time intervalTime;

    private final ValueStateDescriptor<Long> ntStateDecs =
            new ValueStateDescriptor<>("nextTimeState", LongSerializer.INSTANCE);

    private ValueState<Long> ntState;
    private long nextTime = Long.MIN_VALUE;


    public IncrementalTrigger(Time intervalTime) {
        this.intervalTime = intervalTime;

    }


    @Override
    public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {

        long nextTriggerTime = nextTimeForTime(window.getStart());
        if (nextTime == Long.MIN_VALUE) {
            ctx.registerEventTimeTimer(nextTriggerTime);
            return TriggerResult.CONTINUE;
        } else if (ctx.getCurrentWatermark() < nextTime) {
            return TriggerResult.CONTINUE;
        } else {
            ctx.registerEventTimeTimer(nextTime + intervalTime.toMilliseconds());
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        long firstTriggerTimeOfWindow = nextTimeForTime(window.getStart());
        if (nextTime < firstTriggerTimeOfWindow) {
            ctx.registerEventTimeTimer(firstTriggerTimeOfWindow);
        }
        return time == window.maxTimestamp() ? TriggerResult.FIRE_AND_PURGE : TriggerResult.FIRE;
    }

    private long nextTimeForTime(long time) {
        return time / 1000 * 1000 + intervalTime.toMilliseconds();
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

    }

//    @Override
//    public void snapshotState(FunctionSnapshotContext context) throws Exception {
//
//    }

//    @Override
//    public void initializeState(FunctionInitializationContext context) throws Exception {
//        OperatorStateStore operatorStateStore = context.getOperatorStateStore();
//        operatorStateStore.get
//        //new ValueStateDescriptor<>("nextTriggerTime-state", LongSerializer.INSTANCE)
//    }
}
