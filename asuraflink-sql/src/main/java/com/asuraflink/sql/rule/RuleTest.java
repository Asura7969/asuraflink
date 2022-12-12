package com.asuraflink.sql.rule;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.calcite.CalciteConfig;
import org.apache.flink.table.planner.calcite.CalciteConfigBuilder;
import org.apache.flink.table.planner.plan.rules.FlinkStreamRuleSets;

import java.util.ArrayList;
import java.util.List;

/**
 * @author asura7969
 * @create 2021-03-15-22:30
 */
public class RuleTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings envSettings =
                EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, envSettings);

        List<RelOptRule> ruleList = new ArrayList<>();
        FlinkStreamRuleSets.LOGICAL_OPT_RULES().forEach((rule) -> {
            if (!rule.equals(CoreRules.FILTER_INTO_JOIN)) {
                ruleList.add(rule);
            }
        });
        // 添加自定义规则
        ruleList.add(TableScanRule.INSTANCE());
        RuleSet rules = RuleSets.ofList(ruleList);
        CalciteConfig cc = new CalciteConfigBuilder()
                // TODO: 替换规则
//                .replaceStreamProgram(rules)
                .build();
        tEnv.getConfig().setPlannerConfig(cc);

        // ...

//        tEnv.execute("custom rule");
    }
}
