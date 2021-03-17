package com.asuraflink.window;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
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
        env.getConfig().setAutoWatermarkInterval(500);

        env.addSource(new SourceFunction<String>() {
            private static final long serialVersionUID = -2379895866820106777L;
            private volatile boolean running = true;
            private long count = 0L;
            @Override
            public void run(SourceContext<String> ctx) throws Exception {

                while (running && count < 100) {
                    synchronized (ctx.getCheckpointLock()) {
                        Thread.sleep(10000);
                        ctx.collect(count + "");
                        count++;
                    }
                }
            }
            @Override
            public void cancel() {
                running = false;
            }
        })
//                .assignTimestampsAndWatermarks((WatermarkStrategy<String>) context -> new WatermarkGenerator<String>() {
//            @Override
//            public void onEvent(String event, long eventTimestamp, WatermarkOutput output) {
//                output.emitWatermark(new Watermark(System.currentTimeMillis()));
//            }
//
//            @Override
//            public void onPeriodicEmit(WatermarkOutput output) {
//                output.emitWatermark(new Watermark(System.currentTimeMillis()));
//            }
//        })

                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<String>(){
                    @Override
                    public long extractAscendingTimestamp(String userBehavior) {
                        // 原始数据单位秒，将其转成毫秒
                        return System.currentTimeMillis();
                    }
                })
                .keyBy((KeySelector<String, Integer>) value -> Integer.parseInt(value) % 2 == 0 ? 1 : 2)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .trigger(new IncrementalTrigger(Time.seconds(2)))
                .process(new ProcessWindowFunction<String, String, Integer, TimeWindow>() {
                    private static final long serialVersionUID = 6239834468102289206L;

                    @Override
                    public void process(Integer key, Context context,
                                        Iterable<String> elements, Collector<String> out) throws Exception {

                        List<String> list = new ArrayList<>();
                        Iterator<String> iterator = elements.iterator();
                        while (iterator.hasNext()) {
                            list.add(iterator.next());
                        }
                        out.collect("key:" + key + " elements : " + list);
                    }
                }).print();

        env.execute("");

    }
}
