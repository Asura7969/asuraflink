package com.asuraflink.sql.user;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 窗口函数:
 * https://segmentfault.com/a/1190000023296719
 * 时间处理函数：
 * https://help.aliyun.com/knowledge_list/62717.html?spm=a2c4g.11186623.2.5.7afc215f1SEjNr&page=1
 *
 */
public class UserBehavior {

    public static void main(String[] args) throws Exception {

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        Configuration configuration = tEnv.getConfig().getConfiguration();

        configuration.setString("table.exec.mini-batch.enabled", "true");
        configuration.setString("table.exec.mini-batch.allow-latency", "1s");
        configuration.setString("table.exec.mini-batch.size", "1000");
        configuration.setString("table.optimizer.agg-phase-strategy", "TWO_PHASE");
        configuration.setString("table.optimizer.distinct-agg.split.enabled", "true");

        String noWaterMarkDDL =
                "CREATE TABLE user_log (\n" +
                "    user_id VARCHAR,\n" +
                "    item_id VARCHAR,\n" +
                "    category_id VARCHAR,\n" +
                "    behavior VARCHAR,\n" +
                "    ts TIMESTAMP(3)\n" +
                ")" + withKafka;

        String waterMarkDDL =
                "CREATE TABLE user_log (\n" +
                "    user_id VARCHAR,\n" +
                "    item_id VARCHAR,\n" +
                "    category_id VARCHAR,\n" +
                "    behavior VARCHAR,\n" +
                "    ts TIMESTAMP(3),\n" +
                // 在ts上定义5 秒延迟的 watermark
                "    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND" +
                ")" + withKafka;

        tEnv.sqlUpdate(waterMarkDDL);

        tEnv.executeSql("DESCRIBE user_log").print();

        String querySql =
                "SELECT\n" +
                "  DATE_FORMAT(ts, 'yyyy-MM-dd HH:00') dt,\n" +
                "  COUNT(*) AS pv,\n" +
                "  COUNT(DISTINCT user_id) AS uv\n" +
                "FROM user_log\n" +
                "GROUP BY DATE_FORMAT(ts, 'yyyy-MM-dd HH:00')";

        // ==================== GroupBy Window Aggregation ==============================
        // xxx:TUMBLE、HOP、SESSION
        // XXX_START: 窗口开始时间
        // XXX_END: 窗口结束时间（时间范围：左闭右闭，例如：2017-11-26T01:01:00 ~ 2017-11-26T01:01:30）
        // XXX_ROWTIME: （窗口结束时间，事件时间）（时间范围：左闭右开，例如：2017-11-26T01:01:00 ~ 2017-11-26T01:01:29.999）
        // XXX_PROCTIME: （窗口结束时间，处理时间）同上

        String tumpleSql =
                "SELECT\n" +
                "   TUMBLE_START(ts, INTERVAL '5' MINUTE) dt,\n" +
                "   item_id,\n" +
                "   COUNT(item_id)\n" +
                "FROM user_log\n" +
                "GROUP BY TUMBLE(ts, INTERVAL '5' MINUTE), item_id";

        // 每分钟计算一次每种item最近5分钟的总数（滑动窗口）
        String hopSql =
                "SELECT\n" +
                "   HOP_START(ts, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE),\n" +
                "   item_id,\n" +
                "   COUNT(item_id)\n" +
                "FROM user_log\n" +
                "GROUP BY HOP(ts, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE), item_id";


        String sessionSql =
                "SELECT\n" +
                "   SESSION_START(ts, INTERVAL '30' SECOND) AS sStart,\n" +
                "   SESSION_END(ts, INTERVAL '30' SECOND) AS snd,\n" +
                "   SESSION_ROWTIME(ts, INTERVAL '30' SECOND) AS snd,\n" +
                "   item_id,\n" +
                "   COUNT(item_id)\n" +
                "FROM user_log\n" +
                "GROUP BY SESSION(ts, INTERVAL '30' SECOND), item_id";


        // ==================== Over Window aggregation ==============================
        //https://www.alibabacloud.com/help/zh/doc-detail/62514.htm


        tEnv.executeSql(sessionSql).print();
        tEnv.execute("SQL Job");
    }





















    private static final String withKafka =
            "WITH (\n" +
            "    'connector.type' = 'kafka',\n" +
            "    'connector.version' = 'universal',\n" +
            "    'connector.topic' = 'user-behavior',\n" +
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
