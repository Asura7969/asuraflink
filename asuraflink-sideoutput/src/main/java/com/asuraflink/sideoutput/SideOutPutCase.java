package com.asuraflink.sideoutput;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SideOutPutCase {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        OutputTag<Integer> oddTag = new OutputTag<Integer>("odd"){};// 奇数
        OutputTag<Integer> evenTag = new OutputTag<Integer>("even"){};// 偶数

        SingleOutputStreamOperator<Integer> mainSource = env.addSource(new NumberSource())
                .process(new ProcessFunction<Integer, Integer>() {
                    @Override
                    public void processElement(Integer num, Context ctx, Collector<Integer> out) throws Exception {
                        if (num % 2 == 0) {
                            ctx.output(evenTag, num);
                        } else {
                            ctx.output(oddTag, num);
                        }
                    }
                });

        /**
         * SideOutputTransformation
         */
        mainSource.getSideOutput(oddTag).print();
        mainSource.getSideOutput(evenTag).print();
        System.out.println(env.getExecutionPlan());
        env.execute();

    }



    static class NumberSource extends RichSourceFunction<Integer> {
        private volatile boolean isRunning = true;
        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            int start = 0;
            while (isRunning) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(start);
                    start ++;
                    Thread.sleep(1000L);
                }
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
