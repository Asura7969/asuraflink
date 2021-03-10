package com.asuraflink.sql.user.delay.sql;

import com.asuraflink.sql.user.delay.ExternalReadHelper;
import com.asuraflink.sql.user.delay.func.DelayOption;
import com.asuraflink.sql.user.delay.func.DimensionDelayFunc;
import com.asuraflink.sql.user.delay.func.OrderSourceFunc;
import com.asuraflink.sql.user.delay.func.PaySourceFunc;
import com.asuraflink.sql.user.delay.model.Order;
import com.asuraflink.sql.user.delay.model.Pay;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/sql/queries.html
 * https://segmentfault.com/a/1190000023548096
 *
 * 延迟维表 join
 */
public class DelayedDimTableJoinSQLTest {

    private static final String topic = "order-stream";

    private static final String withKafka =
            "WITH (\n" +
            "    'connector.type' = 'kafka',\n" +
            "    'connector.version' = 'universal',\n" +
            "    'connector.topic' = '" + topic + "',\n" +
            "    'connector.startup-mode' = 'earliest-offset',\n" +
            "    'connector.properties.0.key' = 'zookeeper.connect',\n" +
            "    'connector.properties.0.value' = 'localhost:2181',\n" +
            "    'connector.properties.1.key' = 'bootstrap.servers',\n" +
            "    'connector.properties.1.value' = 'localhost:9092',\n" +
            "    'connector.properties.2.key' = 'group.id',\n" +
            "    'connector.properties.2.value' = 'testGroup',\n" +
            "    'update-mode' = 'append',\n" +
            "    'format.type' = 'json',\n" +
            "    'format.derive-schema' = 'true'\n" +
            ")";

    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(settings);


        String waterMarkDDL =
                "CREATE TABLE order (\n" +
                "    user INT,\n" +
                "    product VARCHAR,\n" +
                "    amount INT,\n" +
                "    orderId INT,\n" +
                "    logTime TIMESTAMP(3),\n" +
                // 在ts上定义5 秒延迟的 watermark
                "    WATERMARK FOR ts AS logTime - INTERVAL '5' SECOND" +
                ")" + withKafka;

        tEnv.sqlUpdate(waterMarkDDL);



        DelayOption delayOption = new DelayOption.Builder()
                .setDelayTime(Duration.ofSeconds(10))
                .setIgnoreExpiredData(false)
                .setIntervalDuration(Duration.ofSeconds(5))
                .setMaxRetryTimes(3)
                .build();

        ExternalReadHelper externalReadHelper = new ExternalReadHelper();

        /**
         * TODO:
         *  Pay返回值（DimensionDelayFunc）
         *  order pay 自定义数据源
         *  注：时间戳
         */
        DimensionDelayFunc<Long, Pay> delayFunc = new DimensionDelayFunc<>(delayOption, externalReadHelper);
        tEnv.createTemporarySystemFunction("pay", delayFunc);

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

//        env.execute();
    }

}
