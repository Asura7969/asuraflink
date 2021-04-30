package com.asuraflink.sql.rule

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.rel.core.TableScan
import org.apache.flink.table.planner.plan.nodes.logical.{FlinkLogicalCalc, FlinkLogicalJoin, FlinkLogicalRel, FlinkLogicalSnapshot}

class BroadcastTemporal(description: String) extends RelOptRule(
  operand(classOf[FlinkLogicalJoin],
    operand(classOf[FlinkLogicalRel], any()),
    operand(classOf[FlinkLogicalSnapshot],
      operand(classOf[FlinkLogicalCalc],
        operand(classOf[TableScan], any())))),
  description){

  /**
   * [[org.apache.flink.table.planner.plan.rules.physical.common.CommonLookupJoinRule]]
   * @param call
   * @return
   */
  override def matches(call: RelOptRuleCall): Boolean = {
    super.matches(call)
    // 配置开关，是否打开


  }
  override def onMatch(call: RelOptRuleCall): Unit = {
    val logicalJoin: FlinkLogicalJoin = call.rel(0)

  }
}
