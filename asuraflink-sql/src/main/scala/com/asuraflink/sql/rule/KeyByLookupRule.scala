package com.asuraflink.sql.rule

import java.util

import com.asuraflink.sql.utils.AddRuleUtils
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.rel.RelNode
import org.apache.flink.table.planner.calcite.FlinkContext
import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecLookupJoin
import org.apache.flink.table.planner.plan.rules.physical.FlinkExpandConversionRule.satisfyDistribution


/**
 * @author asura7969
 * @create 2021-04-18-16:00
 */
class KeyByLookupRule extends RelOptRule(
  operand(classOf[StreamExecLookupJoin], any()),
  "KeyByLookupRule"){

  override def matches(call: RelOptRuleCall): Boolean = {
    val tableConfig = call.getPlanner.getContext.unwrap(classOf[FlinkContext]).getTableConfig
    tableConfig.getConfiguration.getBoolean(AddRuleUtils.LOOKUP_KEY_BY_ENABLE, false)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val execLookupJoin: StreamExecLookupJoin = call.rel(0)
    val joinInfo = execLookupJoin.joinInfo

    val keys = joinInfo.leftKeys

    val inputList = new util.ArrayList[RelNode]()
    val newInput = satisfyDistribution(
      FlinkConventions.STREAM_PHYSICAL, execLookupJoin.getInput, FlinkRelDistribution.hash(keys, requireStrict = true))
    inputList.add(newInput)
    call.transformTo(execLookupJoin.copy(execLookupJoin.getTraitSet, inputList))
  }
}

object KeyByLookupRule {
  val INSTANCE = new KeyByLookupRule
}

