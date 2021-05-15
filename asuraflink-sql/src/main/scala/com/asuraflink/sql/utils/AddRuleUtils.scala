package com.asuraflink.sql.utils

import java.util

import com.asuraflink.sql.rule.KeyByLookupRule
import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.calcite.tools.RuleSets
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.table.planner.plan.optimize.program.{FlinkChainedProgram, FlinkGroupProgramBuilder, FlinkHepRuleSetProgramBuilder, FlinkStreamProgram, HEP_RULES_EXECUTION_TYPE, StreamOptimizeContext}
//import org.apache.flink.table.planner.plan.rules.physical.stream.StreamExecTemporalBatchJoinRule

object AddRuleUtils {

  val LOOKUP_KEY_BY_ENABLE: String = "lookup-keyby.enabled"

  def addLookupKeyBy(tEnv: StreamTableEnvironment): FlinkChainedProgram[StreamOptimizeContext] = {

    val program = FlinkStreamProgram.buildProgram(tEnv.getConfig.getConfiguration)

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
    program
  }

//  def addBatchJoin(tEnv: StreamTableEnvironment) : FlinkChainedProgram[StreamOptimizeContext] = {
//    val program = FlinkStreamProgram.buildProgram(tEnv.getConfig.getConfiguration)
//    val newPhysicalRewriteList = new util.ArrayList[RelOptRule]()
//    newPhysicalRewriteList.add(StreamExecTemporalBatchJoinRule.INSTANCE)
//    val newPhysicalRewrite = RuleSets.ofList(newPhysicalRewriteList)
//
//    program.addLast("myRule",
//      FlinkGroupProgramBuilder.newBuilder[StreamOptimizeContext]
//        .addProgram(FlinkHepRuleSetProgramBuilder.newBuilder
//          .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
//          .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
//          .add(newPhysicalRewrite)
//          .build(), "batch join").build())
//    program
//  }

}
