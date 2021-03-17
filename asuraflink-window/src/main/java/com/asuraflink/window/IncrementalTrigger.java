package com.asuraflink.window;

import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.PriorityQueue;
import java.util.Queue;

public class IncrementalTrigger extends Trigger<Object, TimeWindow> {

    // 多久触发一次
    private long intervalTime;
    private long nextTime = Long.MIN_VALUE;

    private final Queue<TimeWindow> registerWindowQueue;


    public IncrementalTrigger(Time intervalTime) {
        this.intervalTime = intervalTime.toMilliseconds();
        this.registerWindowQueue = new PriorityQueue<>();

    }

    // todo:是否始终要多注册一个窗口
    @Override
    public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        tryRegister(window, ctx);
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return time == window.maxTimestamp() ? TriggerResult.FIRE_AND_PURGE : TriggerResult.FIRE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

    }

    /**
     * 注册给定窗口的所有时间
     * @param window
     * @param ctx
     */
    private void registerTime(TimeWindow window, TriggerContext ctx) {
        registerWindowQueue.add(window);
        long registerTimeOfWindow = window.getStart();
        for (long i = registerTimeOfWindow; i <= window.getEnd(); i++) {
            ctx.registerEventTimeTimer(window.getStart());
            if (registerTimeOfWindow == window.maxTimestamp()) {
                break;
            }
            registerTimeOfWindow = registerTimeOfWindow + intervalTime;
        }
    }

    private void tryRegister(TimeWindow window, TriggerContext ctx) {
        if (registerWindowQueue.isEmpty()) {
            registerTime(window, ctx);
        } else {
            if (!registerWindowQueue.contains(window)) {
                registerTime(window, ctx);
            }
        }
    }
}
