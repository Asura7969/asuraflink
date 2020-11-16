package com.asuraflink.broadcast;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class BroadCastStream {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers","localhost:9092");
        kafkaProperties.put("group.id","broadcast-groupId");

        FlinkKafkaConsumer010<String> kafkaConsumer =
                new FlinkKafkaConsumer010<>("broadcast-topic", new SimpleStringSchema(), kafkaProperties);
        kafkaConsumer.setStartFromLatest();

        DataStream<String> kafkaSource = env.addSource(kafkaConsumer).name("KafkaSource").uid("source-id-kafka-source");
        DataStreamSource<String> configStream = env.addSource(new DefinedSource(2));

        // (1) 先建立MapStateDescriptor
        MapStateDescriptor<Void, String> configDescriptor = new MapStateDescriptor<>("config", Types.VOID, Types.STRING);

        // (2) 将配置流广播，形成BroadcastStream
        BroadcastStream<String> broadcast = configStream.broadcast(configDescriptor);

        kafkaSource.connect(broadcast).process(new CustomBroadcastProcessFunction()).print();

        env.execute("broadcast stream");
    }

    static class CustomBroadcastProcessFunction extends BroadcastProcessFunction<String,String,Integer> {
        MapStateDescriptor<Void, String> configDescriptor = new MapStateDescriptor<>("config", Types.VOID, Types.STRING);
        @Override
        public void processElement(String value, ReadOnlyContext ctx, Collector<Integer> out) throws Exception {
            ReadOnlyBroadcastState<Void, String> broadcastState = ctx.getBroadcastState(configDescriptor);
            String broadcastValue = broadcastState.get(null);
            if (broadcastValue.equals(value)) {
                out.collect(1);
            }
        }

        @Override
        public void processBroadcastElement(String value, Context ctx, Collector<Integer> out) throws Exception {
            //获取状态
            final BroadcastState<Void, String> broadcastState = ctx.getBroadcastState(configDescriptor);
            //清空状态
            broadcastState.clear();
            //更新状态
            broadcastState.put(null,value);
        }
    }

    static class DefinedSource extends RichSourceFunction<String> {
        private volatile boolean isRunning = true;
        private int secondInterval;

        public DefinedSource(int secondInterval) {
            this.secondInterval = secondInterval;
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while (isRunning){
                // do something
                ctx.collect("output");
                Thread.sleep(1000 * secondInterval);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

}
