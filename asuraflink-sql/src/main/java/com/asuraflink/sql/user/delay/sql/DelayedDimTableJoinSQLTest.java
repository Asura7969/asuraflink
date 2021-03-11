package com.asuraflink.sql.user.delay.sql;

import com.asuraflink.sql.user.delay.RedisReadHelper;
import com.asuraflink.sql.user.delay.func.DelayOption;
import com.asuraflink.sql.user.delay.func.DimensionDelayFunc;
import com.asuraflink.sql.user.delay.model.Pay;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import java.time.Duration;

/**
 *
 * 延迟维表 join
 *
 * 数据生成规则：
 *      订单流：kafka
 *          每 2s 产生一个订单
 *      支付流：redis
 *          每个订单产生后 10s 产生一个对应的支付信息，支付信息过期时间为 5s
 *
 * https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/sql/queries.html
 * https://segmentfault.com/a/1190000023548096
 *
 * https://liurio.github.io/2020/03/28/Flink%E6%B5%81%E4%B8%8E%E7%BB%B4%E8%A1%A8%E7%9A%84%E5%85%B3%E8%81%94/
 */
public class DelayedDimTableJoinSQLTest {

    private static final String topic = "order-stream";

    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        String waterMarkDDL =
                "CREATE TABLE order_stream (\n" +
                "    user_id BIGINT,\n" +
                "    product_name VARCHAR,\n" +
                "    amount BIGINT,\n" +
                "    order_id BIGINT,\n" +
                "    logTime BIGINT,\n" +  // 13位时间戳
                "    ts AS TO_TIMESTAMP(FROM_UNIXTIME(logTime / 1000, 'yyyy-MM-dd HH:mm:ss'))\n" +
                // 注：简化逻辑,不使用 watermark
                // 在ts上定义5 秒延迟的 watermark
//                "    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND" +
                ")" + withKafka;

        tEnv.sqlUpdate(waterMarkDDL);

//        tEnv.executeSql("DESCRIBE order_stream").print();

        DelayOption delayOption = new DelayOption.Builder()
                .setDelayTime(Duration.ofSeconds(2))
                .setIgnoreExpiredData(false)
                .setIntervalDuration(Duration.ofSeconds(2))
                .setMaxRetryTimes(3)
                .build();

        RedisReadHelper externalReadHelper = new RedisReadHelper();

        DimensionDelayFunc<Long, Pay> delayFunc = new DimensionDelayFunc<>(delayOption, externalReadHelper);
        tEnv.createTemporarySystemFunction("delayFunc", delayFunc);

        // perform a temporal table join query
        // 参考：https://blog.csdn.net/wangpei1949/article/details/105335340
        String sql2 =
                "select " +
//                        "o.order_id, payInfo.order_id as p_orderId, payInfo.pay_id as p_pid, o.product_name " +
                        "o.order_id, o.product_name " +
                "from " +
                        "order_stream as o, " +
                        "LATERAL TABLE(delayFunc(o.order_id))";
        tEnv.executeSql(sql2).print();

        tEnv.execute("SQL Job");

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

}
