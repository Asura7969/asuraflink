package com.asuraflink.sql.rule

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptUtil}
import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.logical.LogicalTableScan
/**
 * @author asura7969
 * @create 2021-03-15-22:31
 */
class TableScanRule(description: String) extends RelOptRule(operand(classOf[LogicalTableScan],any()), description){

  override def matches(call: RelOptRuleCall): Boolean = {
    // 默认返回true，此方法返回true才会执行下面的 onMatch 方法
    super.matches(call)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val oldRel: LogicalTableScan = call.rel(0)
    val newRel: RelNode = oldRel.getTable.toRel(
      RelOptUtil.getContext(oldRel.getCluster)
    )
    call.transformTo(newRel)
  }
}

object TableScanRule {
  val INSTANCE = new TableScanRule("TableScanRule")
}
