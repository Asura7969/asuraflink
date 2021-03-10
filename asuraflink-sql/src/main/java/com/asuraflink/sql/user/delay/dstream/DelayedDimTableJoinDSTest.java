package com.asuraflink.sql.user.delay.dstream;

import com.asuraflink.sql.user.delay.func.DelayOption;
import com.asuraflink.sql.user.delay.func.OrderSourceFunc;
import com.asuraflink.sql.user.delay.func.PaySourceFunc;
import com.asuraflink.sql.user.delay.func.SourceGenerator;
import com.asuraflink.sql.user.delay.model.Order;
import com.asuraflink.sql.user.delay.model.Pay;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.Expressions.$;

/**
 * https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/sql/queries.html
 * https://segmentfault.com/a/1190000023548096
 *
 * 延迟维表 join
 */
public class DelayedDimTableJoinDSTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStreamSource<Order> orderStream = env.addSource(new OrderSourceFunc(3000));
        DataStreamSource<Pay> payStream = env.addSource(new PaySourceFunc(6000));


        /**
         * 设置 watermark
         * 注：如果使用 rowtime，必须设置 watermark
         */
        DataStream<Order> orderWatermarkStream =
                orderStream.assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Order>forBoundedOutOfOrderness(Duration.ofMillis(10))
                        .withIdleness(Duration.ofMinutes(1))
                        .withTimestampAssigner((element, recordTimestamp) -> Math.max(element.getLogTime().getTime(), recordTimestamp)));

        DataStream<Pay> payWatermarkStream =
                payStream.assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Pay>forBoundedOutOfOrderness(Duration.ofMillis(10))
                        .withIdleness(Duration.ofMinutes(1))
                        .withTimestampAssigner((element, recordTimestamp) -> Math.max(element.getTtl().getTime(), recordTimestamp)));

        DelayOption delayOption = new DelayOption.Builder()
                .setDelayTime(Duration.ofSeconds(10))
                .setIgnoreExpiredData(false)
                .setIntervalDuration(Duration.ofSeconds(5))
                .setMaxRetryTimes(3)
                .build();



        // rowtime 定义表
        Table orderTable = tEnv.fromDataStream(orderWatermarkStream,
                $("user"), $("product"), $("amount"), $("orderId"), $("logTime").rowtime());
        Table payTable = tEnv.fromDataStream(payWatermarkStream,
                $("user"), $("amount"), $("orderId"), $("payId"), $("ttl").rowtime());

        // proctime 定义表
//        Table orderTable = tEnv.fromDataStream(orderWatermarkStream, $("user"), $("product"), $("amount"), $("orderId"), $("proc").proctime());
//        Table payTable = tEnv.fromDataStream(payStream, $("user"), $("amount"), $("orderId"), $("payId"), $("proc").proctime());
        TemporalTableFunction dimPayTable = payTable.createTemporalTableFunction($("ttl"), $("orderId"));


        tEnv.registerTable("orderTable", orderTable);
//        tEnv.registerTable("payTable", dimPayTable);
        tEnv.registerFunction("payTableFunc", dimPayTable);

        // perform a temporal table join query
        String sql1 =
                "select " +
                        "o.orderId, pay.orderId, o.product " +
                "from " +
                        "orderTable as o " +
                        "join payTable FOR SYSTEM_TIME AS OF o.logTime ON o.orderId = payTable.orderId";

        // 参考：https://blog.csdn.net/wangpei1949/article/details/105335340
        String sql2 =
                "select " +
                        "o.orderId, payInfo.orderId as p_orderId, payInfo.payId as p_pid, o.product " +
                        "from " +
                        "orderTable as o, " +
                        "LATERAL TABLE (payTableFunc(o.logTime)) as payInfo where o.orderId = payInfo.orderId";
        tEnv.executeSql(sql2).print();

//        DataStream resultDs = tEnv.toAppendStream(result, Row.class);
//        resultDs.print();

//        Table time = tEnv.sqlQuery("select logTime from orderTable");
//        DataStream<Row> rowDataStream = tEnv.toAppendStream(time, Row.class);
//        rowDataStream.print();

//        Iterator<Row> collect = tableResult.collect();
//        List<String> print = Lists.newArrayList(collect).stream()
//                .map(Row::toString)
//                .sorted()
//                .collect(Collectors.toList());
//        tEnv.toAppendStream(orderTable, Order.class).print();



//        Iterator<Row> collected = tEnv.executeSql(sql).collect();
//        List<String> result = Lists.newArrayList(collected).stream()
//                .map(Row::toString)
//                .sorted()
//                .collect(Collectors.toList());

//        result.forEach(System.out::println);
//        tEnv.toAppendStream(result, Order.class).print();

        env.execute();
    }

}
