package com.asuraflink.sql.rule;

import com.asuraflink.sql.delay.DelayedJoinTest;
import com.asuraflink.sql.utils.AddRuleUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.calcite.CalciteConfigBuilder;
import org.apache.flink.table.planner.plan.optimize.program.*;
import org.apache.flink.table.planner.calcite.CalciteConfig;


import static org.apache.flink.table.api.Expressions.$;

public class AddMyLookupRule {

    public static final String JOIN_KEY = "flink-join";
    public static final String INPUT_TABLE = "redisDynamicTableSource";
    public static final String HGET = "HGET";


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings envSettings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, envSettings);

        // ---------------------------- 添加自定义规则 ----------------------------
        FlinkChainedProgram<StreamOptimizeContext> program = AddRuleUtils.addLookupKeyBy(tEnv);
        // 开启 keyby
        tEnv.getConfig().getConfiguration().setBoolean(AddRuleUtils.LOOKUP_KEY_BY_ENABLE(), true);


        CalciteConfig cc = new CalciteConfigBuilder().replaceStreamProgram(program).build();
        tEnv.getConfig().setPlannerConfig(cc);


        DataStreamSource<Tuple2<String, String>> continueSource = env.addSource(new DelayedJoinTest.ContinueSource());
        Table t = tEnv.fromDataStream(continueSource,
                $("id1"),
                $("id2"),
                $("proctime").proctime());

        tEnv.createTemporaryView("T", t);

        tEnv.executeSql(
                "CREATE TABLE " + INPUT_TABLE + " (\n" +
                        "  cityId STRING,\n" +
                        "  cityName STRING\n" +
                        ") WITH (\n" +
                        "  'connector' = 'redis',\n" +
                        "  'mode' = 'single',\n" +
                        "  'single.host' = 'localhost',\n" +
                        "  'single.port' = '6379',\n" +
                        "  'db-num' = '0',\n" +
                        "  'command' = '" + HGET + "',\n" +
                        "  'connection.timeout-ms' = '5000',\n" +
                        "  'additional-key' = '"+ JOIN_KEY +"'" +
                        ")"
        );

        String sqlQuery =
                "SELECT source.id1, source.id2, L.cityId FROM T AS source "
                        + "LEFT JOIN " + INPUT_TABLE + " for system_time as of source.proctime AS L "
                        + "ON source.id1 = L.cityId";

        tEnv.toAppendStream(tEnv.sqlQuery(sqlQuery), P.class);
        // 查看执行计划
        System.out.println(env.getExecutionPlan());

        tEnv.executeSql(sqlQuery).print();

        tEnv.execute("AddMyLookupRule");
    }


    public static class P {
        public String id1;
        public String id2;
        public String cityId;
    }
}
