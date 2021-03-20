package com.asuraflink.window;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * @author asura7969
 * @create 2021-03-17-22:09
 */
public class WindowTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        env.getConfig().setAutoWatermarkInterval(500);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.addSource(new SourceFunction<Tuple2<String,Long>>() {
            private static final long serialVersionUID = -2379895866820106777L;
            private volatile boolean running = true;
            private long count = 0L;
            @Override
            public void run(SourceContext<Tuple2<String,Long>> ctx) throws Exception {

                while (running && count < 100) {
                    synchronized (ctx.getCheckpointLock()) {
                        Thread.sleep(10000);
                        long currentTimeMillis = System.currentTimeMillis();
                        ctx.collectWithTimestamp(Tuple2.of(count+ "", currentTimeMillis), currentTimeMillis);
                        ctx.emitWatermark(new Watermark(currentTimeMillis));
                        count++;
                    }
                }
            }
            @Override
            public void cancel() {
                running = false;
            }
        })
                .assignTimestampsAndWatermarks((WatermarkStrategy<Tuple2<String,Long>>) context -> new WatermarkGenerator<Tuple2<String,Long>>() {
            @Override
            public void onEvent(Tuple2<String,Long> event, long eventTimestamp, WatermarkOutput output) {
                output.emitWatermark(new org.apache.flink.api.common.eventtime.Watermark(event.f1));
            }

            @Override
            public void onPeriodicEmit(WatermarkOutput output) {
//                output.emitWatermark(new Watermark(System.currentTimeMillis()));
            }
        })
//                .keyBy((KeySelector<Tuple2<String, Long>, Integer>) value -> Integer.parseInt(value.f0) % 2 == 0 ? 1 : 2)
                .keyBy((KeySelector<Tuple2<String, Long>, Integer>) value -> 1)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .trigger(new IncrementalTrigger(Time.seconds(10)))
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, Integer, TimeWindow>() {
                    private static final long serialVersionUID = 6239834468102289206L;

                    @Override
                    public void process(Integer key, Context context,
                                        Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                        long start = context.window().getStart();
                        long end = context.window().getEnd();

                        List<String> list = new ArrayList<>();
                        Iterator<Tuple2<String, Long>> iterator = elements.iterator();
                        while (iterator.hasNext()) {
                            list.add(iterator.next().f0);
                        }
                        out.collect("窗口:" + DateUtils.getStringTime(start) + " ~ " + DateUtils.getStringTime(end) + "\nkey:" + key + " elements : " + list);
                    }
                }).print();

        env.execute("");

    }
}
