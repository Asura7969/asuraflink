package com.asuraflink.window;

import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static com.asuraflink.window.DateUtils.getStringTime;

public class IncrementalTrigger extends Trigger<Object, TimeWindow> {

    // 多久触发一次
    private long intervalTime;
    private List<TimeWindow> registerWindowQueue = new ArrayList<>();


    public IncrementalTrigger(Time intervalTime) {
        this.intervalTime = intervalTime.toMilliseconds();
    }

    @Override
    public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        System.out.println("当前水印:" + getStringTime(ctx.getCurrentWatermark()));
        if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
            registerWindowQueue.remove(window);
            return TriggerResult.FIRE;
        }
        registerTime(window, ctx);
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        DateTimeFormatter ftf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        System.out.println("触发时间:" + ftf.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(time), ZoneId.systemDefault())));

        if (time == window.maxTimestamp()) {
//            registerWindowQueue.remove(window);
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
        System.out.println("window 窗口时间:" + getStringTime(window.getStart()) + " ~ " + getStringTime(window.getEnd()));
//        registerWindowQueue.add(window);
        long registerTimeOfWindow = window.getStart();
        for (long i = registerTimeOfWindow; i <= window.getEnd(); i++) {

            if (registerTimeOfWindow - 1 == window.maxTimestamp()) {
                ctx.registerEventTimeTimer(registerTimeOfWindow - 1);
                break;
            }
            ctx.registerEventTimeTimer(registerTimeOfWindow);

            registerTimeOfWindow += intervalTime;
        }
    }

    private void tryRegister(TimeWindow window, TriggerContext ctx) {
        if (registerWindowQueue.isEmpty() || !registerWindowQueue.contains(window)) {
            registerTime(window, ctx);
        }
    }
}
