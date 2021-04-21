package com.asuraflink.sql.rule

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.flink.table.planner.delegation.StreamPlanner
import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode
import org.apache.flink.table.planner.plan.nodes.physical.stream.{StreamExecExchange, StreamExecLookupJoin}
import org.apache.flink.table.planner.plan.rules.physical.FlinkExpandConversionRule.satisfyDistribution

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * @author asura7969
 * @create 2021-04-18-16:00
 */
class KeyByLookupRule
  extends RelOptRule(
    operand(classOf[StreamExecLookupJoin], any()),
      "KeyByLookupRule"){


  override def onMatch(call: RelOptRuleCall): Unit = {
    val execLookupJoin:StreamExecLookupJoin = call.rel(0)
    val cluster = execLookupJoin.getCluster
    val joinType = execLookupJoin.joinType
    val joinInfo = execLookupJoin.joinInfo
    val inputNodes: mutable.Seq[ExecNode[StreamPlanner, _]] = execLookupJoin.getInputNodes.asScala

    val keys = joinInfo.leftKeys

    val newInput = satisfyDistribution(
      FlinkConventions.STREAM_PHYSICAL, execLookupJoin.getInput, FlinkRelDistribution.hash(keys, requireStrict = true))

    val a = new StreamExecLookupJoin(
      cluster,
      execLookupJoin.getTraitSet,
      newInput,
      // TODO: 需要 temporalTable
      execLookupJoin.getTable,
      Option.empty,
      joinInfo, joinType)

    call.transformTo(a)
  }
}
