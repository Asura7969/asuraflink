package com.asuraflink.window;

import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;

public class IncrementalTrigger extends Trigger<Object, TimeWindow> {

    // 多久触发一次
    private long intervalTime;
    private long nextTime = Long.MIN_VALUE;

    private final Queue<TimeWindow> registerWindowQueue;


    public IncrementalTrigger(Time intervalTime) {
        this.intervalTime = intervalTime.toMilliseconds();
        this.registerWindowQueue = new PriorityQueue<>(new Comparator<TimeWindow>() {
            @Override
            public int compare(TimeWindow o1, TimeWindow o2) {
                return Integer.parseInt((o1.getStart() - o2.getStart()) + "");
            }
        });

    }

    // todo:是否始终要多注册一个窗口
    @Override
    public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
            registerWindowQueue.remove(window);
            return TriggerResult.FIRE;
        }
        tryRegister(window, ctx);
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        if (time == window.maxTimestamp()) {
            registerWindowQueue.remove(window);
        }
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
            ctx.registerEventTimeTimer(registerTimeOfWindow);
            if (registerTimeOfWindow == window.maxTimestamp()) {
                break;
            }
            registerTimeOfWindow += intervalTime;
        }
    }

    private void tryRegister(TimeWindow window, TriggerContext ctx) {
        if (registerWindowQueue.isEmpty() || !registerWindowQueue.contains(window)) {
            registerTime(window, ctx);
        }
        // tudo: 此方法不能覆盖 sessionWindow的场景
        TimeWindow nextWindow = new TimeWindow(window.getEnd(), window.getEnd() * 2 - window.getStart());
        if (!registerWindowQueue.contains(window)) {
            registerTime(nextWindow, ctx);
        }
    }
}
