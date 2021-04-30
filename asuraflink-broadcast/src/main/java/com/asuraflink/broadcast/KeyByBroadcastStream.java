package com.asuraflink.broadcast;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;

import java.util.*;

public class KeyByBroadcastStream {

    private static MapStateDescriptor<Integer, Product> temporalBroadcastStateDescriptor = new MapStateDescriptor<>(
            "temporalBroadcastState",
            BasicTypeInfo.INT_TYPE_INFO,
            TypeInformation.of(new TypeHint<Product>() {}));

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KeyedStream<Order, Integer> continueSource = env.addSource(new ContinueSource())
                .keyBy((KeySelector<Order, Integer>) value -> value.productId);

        BroadcastStream<Product> broadcast = env.addSource(new TemporalSource())
                .broadcast(temporalBroadcastStateDescriptor);

        BroadcastConnectedStream<Order, Product> connect = continueSource.connect(broadcast);

        connect.process(new KeyedBroadcastProcessFunction<String, Order, Product, String>() {

            @Override
            public void processElement(Order value, ReadOnlyContext ctx, Collector<String> out) throws Exception {

                ReadOnlyBroadcastState<Integer, Product> state = ctx.getBroadcastState(temporalBroadcastStateDescriptor);
                Product product = state.get(value.productId);
                if (Objects.nonNull(product)) {
                    System.out.println("订单流当前key: " + value.productId + "    Match : " + product.name);
                } else {
                    System.out.println("订单流当前key: " + value.productId + "    No match");
                }

            }

            @Override
            public void processBroadcastElement(Product in, Context ctx, Collector<String> out) throws Exception {
                BroadcastState<Integer, Product> broadcastState = ctx.getBroadcastState(temporalBroadcastStateDescriptor);
                if (!broadcastState.contains(in.id)) {
                    broadcastState.put(in.id, in);
                }
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                super.onTimer(timestamp, ctx, out);
            }
        }).print();
//        System.out.println(env.getExecutionPlan());
        env.execute("KeyByBroadcastStream");



    }

    public static class TemporalSource extends RichSourceFunction<Product> {
        private volatile boolean isRunning = true;
        private List<Product> products;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            products = new ArrayList<>();
            products.add(new Product(1,"手机"));
            products.add(new Product(2, "牙刷"));
            products.add(new Product(3, "电脑"));
            products.add(new Product(4, "鼠标"));
            products.add(new Product(5, "衣服"));

        }

        @Override
        public void run(SourceContext<Product> ctx) throws Exception {
            int index = 0;
            while (isRunning) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(products.get(index));
                    if (index == 4) {
                        index = -1;
                    }
                    index ++;
                    Thread.sleep(1000);
                }
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    public static class ContinueSource extends RichSourceFunction<Order> {
        private volatile boolean isRunning = true;
        private List<Order> orders;
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            orders = new ArrayList<>();
            orders.add(new Order(1, 1, "A-订单"));
            orders.add(new Order(2, 2, "B-订单"));
            orders.add(new Order(3, 3, "C-订单"));
            orders.add(new Order(4, 4, "D-订单"));
            orders.add(new Order(5, 5, "E-订单"));
        }

        @Override
        public void run(SourceContext<Order> ctx) throws Exception {
            int index = 0;
            while (isRunning) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(orders.get(index));
                    if (index == 4) {
                        index = -1;
                    }
                    index ++;
                    Thread.sleep(5000);
                }
            }

        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    public static class Product {
        public int id;
        public String name;

        public Product() {
        }

        public Product(int id, String name) {
            this.id = id;
            this.name = name;
        }
    }

    public static class Order {
        public int id;
        public int productId;
        public String name;

        public Order() {
        }

        public Order(int id, int productId, String name) {
            this.id = id;
            this.productId = productId;
            this.name = name;
        }
    }
}
