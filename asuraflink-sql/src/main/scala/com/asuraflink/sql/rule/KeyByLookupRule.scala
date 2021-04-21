package com.asuraflink.sql.rule

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.plan.RelOptRule.{any, operand}
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


  override def onMatch(call: RelOptRuleCall): Unit = {
    val execLookupJoin:StreamExecLookupJoin = call.rel(0)
    val cluster = execLookupJoin.getCluster
    val joinType = execLookupJoin.joinType
    val joinInfo = execLookupJoin.joinInfo

    val keys = joinInfo.leftKeys

    val newInput = satisfyDistribution(
      FlinkConventions.STREAM_PHYSICAL, execLookupJoin.getInput, FlinkRelDistribution.hash(keys, requireStrict = true))

//    val a = new StreamExecLookupJoin(
//      cluster,
//      execLookupJoin.getTraitSet,
//      newInput,
//      // TODO: 需要 temporalTable
//      execLookupJoin.getTemporalTable,
//      execLookupJoin.getCalcOnTemporalTable,
//      joinInfo, joinType)
//
//    call.transformTo(a)
  }
}

object KeyByLookupRule {
  val INSTANCE = new KeyByLookupRule
}

