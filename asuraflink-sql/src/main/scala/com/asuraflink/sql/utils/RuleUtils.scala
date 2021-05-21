package com.asuraflink.sql.utils

import java.util
import com.asuraflink.sql.rule.{BroadcastTemporal, KeyByLookupRule}
import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.calcite.tools.RuleSets
import org.apache.flink.table.api.{TableConfig, TableEnvironment}
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.table.planner.calcite.CalciteConfig
import org.apache.flink.table.planner.plan.optimize.program.FlinkStreamProgram.TEMPORAL_JOIN_REWRITE
import org.apache.flink.table.planner.plan.optimize.program.{FlinkChainedProgram, FlinkGroupProgramBuilder, FlinkHepRuleSetProgramBuilder, FlinkStreamProgram, HEP_RULES_EXECUTION_TYPE, StreamOptimizeContext}
import org.apache.flink.table.planner.plan.rules.FlinkStreamRuleSets
import org.apache.flink.table.planner.utils.TableConfigUtils

class RuleUtils(tEnv: StreamTableEnvironment) {

  val program: FlinkChainedProgram[StreamOptimizeContext] =
    FlinkStreamProgram.buildProgram(tEnv.getConfig.getConfiguration)

  def addLookupKeyBy(): RuleUtils = {

    val newPhysicalRewriteList = new util.ArrayList[RelOptRule]()
    newPhysicalRewriteList.add(KeyByLookupRule.INSTANCE)
    val newPhysicalRewrite = RuleSets.ofList(newPhysicalRewriteList)

    program.addLast("myRule",
      FlinkGroupProgramBuilder.newBuilder[StreamOptimizeContext]
        .addProgram(FlinkHepRuleSetProgramBuilder.newBuilder
          .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
          .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
          .add(newPhysicalRewrite)
          .build(), "lookup keyby").build())
    this
  }

  def addBroadcastTemporalLookupJoinRule(): RuleUtils = {
    val newJoinRewriteList = new util.ArrayList[RelOptRule]()
    newJoinRewriteList.add(BroadcastTemporal.LOOKUP_BROADCAST_JOIN_WITH_FILTER)
    val newJoinRewrite = RuleSets.ofList(newJoinRewriteList)

    program.addLast(
      "rewrite temporal join",
      FlinkGroupProgramBuilder.newBuilder[StreamOptimizeContext]
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
            .add(newJoinRewrite)
            .build(), "rewrite join to temporal table join").build())

    this
  }

  def build: FlinkChainedProgram[StreamOptimizeContext] = program


}


object RuleUtils {

  val LOOKUP_KEY_BY_ENABLE: String = "lookup-keyby.enabled"

  def apply(tEnv: StreamTableEnvironment): RuleUtils = new RuleUtils(tEnv)

  def setUpCurrentRule(tEnv: StreamTableEnvironment, rule: RelOptRule): Unit = {
    val programs = new FlinkChainedProgram[StreamOptimizeContext]()
    programs.addLast(
      "rules",
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(RuleSets.ofList(rule))
        .build()
    )
    replaceStreamProgram(tEnv, programs)
  }

  def replaceStreamProgram(tableEnv: TableEnvironment, program: FlinkChainedProgram[StreamOptimizeContext]): Unit = {
    var calciteConfig = TableConfigUtils.getCalciteConfig(tableEnv.getConfig)
    calciteConfig = CalciteConfig.createBuilder(calciteConfig)
      .replaceStreamProgram(program).build()
    tableEnv.getConfig.setPlannerConfig(calciteConfig)
  }
}
