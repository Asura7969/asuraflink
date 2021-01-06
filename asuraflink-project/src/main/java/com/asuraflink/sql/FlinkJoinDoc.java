package com.asuraflink.sql;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;

/**
 * https://mp.weixin.qq.com/s?__biz=MzU3Mzg4OTMyNQ==&mid=2247490009&idx=1&sn=3a218df4f2f88d46775c82358fb01ea3&chksm=fd3b979bca4c1e8d5e0b4b92fa79c4a7a3fbedc7d7d6b18f0c7816c4ddca6bcad7d8607b6d41&token=1623898787&lang=zh_CN%23rd
 * http://www.whitewood.me/2019/12/15/Flink-SQL-%E5%A6%82%E4%BD%95%E5%AE%9E%E7%8E%B0%E6%95%B0%E6%8D%AE%E6%B5%81%E7%9A%84-Join/
 */
public class FlinkJoinDoc {

    /**
     * flink Sql维表join
     * https://juejin.cn/post/6857506774728212488
     * @param args
     */
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        // use blink
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner()
                .inStreamingMode()
                .build();
        TableEnvironmentImpl tEnv = (TableEnvironmentImpl) StreamTableEnvironmentImpl.create(env, settings, new TableConfig());


        String orderDDL =
                "CREATE TABLE order (\n" +
                "  id INT COMMENT '订单唯一ID',\n" +
                "  productId BIGINT COMMENT '产品唯一ID',\n" +
                "  shiptimeId BIGINT COMMENT '运输单唯一ID',\n" +
                "  timestamp BIGINT COMMENT '时间戳(毫秒)',\n" +
//                "  procTime AS PROCTIME(), \n" +
                "  ets AS TO_TIMESTAMP(FROM_UNIXTIME(timestamp / 1000)),\n" +
                "  WATERMARK FOR ets AS ets - INTERVAL '5' MINUTE\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'adsdw.dwd.max.click.mobileapp',\n" +
                "  'properties.group.id' = 'adsdw.dwd.max.click.mobileapp_group',\n" +
                "  'properties.bootstrap.servers' = 'broker1:9092,broker2:9092,broker3:9092',\n" +
                "  'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"kafka-administrator\" password=\"kafka-administrator-password\";',\n" +
                "  'properties.security.protocol' = 'SASL_PLAINTEXT',\n" +
                "  'properties.sasl.mechanism' = 'SCRAM-SHA-256',\n" +
                "  'avro-confluent.schema-registry.url' = 'http://schema.registry.url:8081',\n" +
                "  'avro-confluent.schema-registry.subject' = 'adsdw.dwd.max.click.mobileapp-value',\n" +
                "  'format' = 'avro-confluent'\n" +
                ");";

        String productDDL =
                "CREATE TABLE product (\n" +
                "  id INT COMMENT '产品唯一ID',\n" +
                "  timestamp BIGINT COMMENT '时间戳(毫秒)',\n" +
                "     ets AS TO_TIMESTAMP(FROM_UNIXTIME(timestamp / 1000)),\n" +
                "     WATERMARK FOR ets AS ets - INTERVAL '5' MINUTE\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'adsdw.dwd.max.show.mobileapp',\n" +
                "  'properties.group.id' = 'adsdw.dwd.max.show.mobileapp_group',\n" +
                "  'properties.bootstrap.servers' = 'broker1:9092,broker2:9092,broker3:9092',\n" +
                "  'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"kafka-administrator\" password=\"kafka-administrator-password\";',\n" +
                "  'properties.security.protocol' = 'SASL_PLAINTEXT',\n" +
                "  'properties.sasl.mechanism' = 'SCRAM-SHA-256',\n" +
                "  'avro-confluent.schema-registry.url' = 'http://schema.registry.url:8081',\n" +
                "  'avro-confluent.schema-registry.subject' = 'adsdw.dwd.max.show.mobileapp-value',\n" +
                "  'format' = 'avro-confluent'\n" +
                ");";
        String shiptimeDDL = "省略";
        tEnv.sqlUpdate(orderDDL);
        tEnv.sqlUpdate(productDDL);
        tEnv.sqlUpdate(shiptimeDDL);

        /**
         * regular join(没有数据清理机制)
         * 使用场景：离线场景和小数据量场景,一般只用做有界数据流的 Join
         * 任何一张表的变更都会和另一张表的历史和未来记录进行匹配
         *
         * SELECT columns
         * FROM t1  [AS <alias1>]
         * [LEFT/INNER/FULL OUTER] JOIN t2
         * ON t1.column1 = t2.key-name1
         */

        /**
         * interval Join(需要定义时间属性字段，EventTime or ProcessTime)
         *
         * -- 写法一
         * SELECT columns
         * FROM t1  [AS <alias1>]
         * [LEFT/INNER/FULL OUTER] JOIN t2
         * ON t1.column1 = t2.key-name1 AND t1.timestamp BETWEEN t2.timestamp  AND  BETWEEN t2.timestamp + INTERVAL '10' MINUTE;
         *
         * -- 写法二
         * SELECT columns
         * FROM t1  [AS <alias1>]
         * [LEFT/INNER/FULL OUTER] JOIN t2
         * ON t1.column1 = t2.key-name1 AND t2.timestamp <= t1.timestamp and t1.timestamp <=  t2.timestamp + INTERVAL ’10' MINUTE ;
         *
         * ## 示例
         * 订单表(Orders(orderId, productName, orderTime))
         * 付款表(Payment(orderId, payType, payTime))
         * 查询：统计在下单一小时内付款的订单信息
         * SELECT
         *   o.orderId,
         *   o.productName,
         *   p.payType,
         *   o.orderTime，
         *   cast(payTime as timestamp) as payTime
         * FROM
         *   Orders AS o JOIN Payment AS p ON
         *   o.orderId = p.orderId AND
         *   p.payTime BETWEEN orderTime AND
         *   orderTime + INTERVAL '1' HOUR
         *
         *
         * > https://developer.aliyun.com/article/683681
         */

        /**
         * temproal table join(流表与维度表join的场景)
         *
         * SELECT columns
         * FROM t1  [AS <alias1>]
         * [LEFT] JOIN t2 FOR SYSTEM_TIME AS OF t1.proctime [AS <alias2>]
         * ON t1.column1 = t2.key-name1
         */
    }
}
