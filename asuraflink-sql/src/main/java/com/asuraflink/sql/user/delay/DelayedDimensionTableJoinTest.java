package com.asuraflink.sql.user.delay;

import com.asuraflink.sql.user.delay.func.OrderSourceFunc;
import com.asuraflink.sql.user.delay.func.PaySourceFunc;
import com.asuraflink.sql.user.delay.func.SourceGenerator;
import com.asuraflink.sql.user.delay.model.Order;
import com.asuraflink.sql.user.delay.model.Pay;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/sql/queries.html
 * 延迟维表 join
 */
public class DelayedDimensionTableJoinTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

//        DataStream<Order> orderA = env.fromCollection(Arrays.asList(
//                new Order(1L, "beer", 3),
//                new Order(1L, "diaper", 4),
//                new Order(3L, "rubber", 2)));
//
//        DataStream<Order> orderB = env.fromCollection(Arrays.asList(
//                new Order(2L, "pen", 3),
//                new Order(2L, "rubber", 3),
//                new Order(4L, "beer", 1)));
//
//
        DataStreamSource<Order> orderStream = env.addSource(new OrderSourceFunc(3000));
        DataStreamSource<Pay> payStream = env.addSource(new PaySourceFunc(6000));

        orderStream.map(Order::toString).print();
        payStream.map(Pay::toString).print();



        // register DataStream as Table
//        tEnv.registerDataStream("OrderA", orderA, "user, product, amount");
//        tEnv.registerDataStream("OrderB", orderB, "user, product, amount");


//        tEnv.toAppendStream(result, Order.class).print();

        env.execute();
    }

}
