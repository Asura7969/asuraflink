package com.asuraflink.sql.rule

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptRuleOperand}
import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.logical.{LogicalCorrelate, LogicalFilter, LogicalSnapshot}
import org.apache.calcite.rex.{RexCorrelVariable, RexFieldAccess, RexInputRef, RexNode, RexShuttle}
import org.apache.flink.table.planner.plan.nodes.logical.{FlinkLogicalCalc, FlinkLogicalJoin, FlinkLogicalRel, FlinkLogicalSnapshot}
import com.asuraflink.sql.rule.utils.RuleUtils._
import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistribution

abstract class BroadcastTemporal(operand: RelOptRuleOperand, description: String)
  extends RelOptRule(operand, description) {

  def getFilterCondition(call: RelOptRuleCall): RexNode

  def getLogicalSnapshot(call: RelOptRuleCall): LogicalSnapshot

  def getLogicalSnapshotInput(call: RelOptRuleCall): RelNode

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
    val correlate: LogicalCorrelate = call.rel(0)
    val leftInput: RelNode = call.rel(1)
    val filterCondition = getFilterCondition(call)
    val snapshot = getLogicalSnapshot(call)

    validateSnapshotInCorrelate(snapshot, correlate)

    val leftRowType = leftInput.getRowType
    // 获取lookupTable, join key, hash
    val joinCondition = filterCondition.accept(new RexShuttle() {
      override def visitFieldAccess(fieldAccess: RexFieldAccess): RexNode = {
        fieldAccess.getReferenceExpr match {
          case corVar: RexCorrelVariable =>
            require(correlate.getCorrelationId.equals(corVar.id))
            val index = leftRowType.getFieldList.indexOf(fieldAccess.getField)
            RexInputRef.of(index, leftRowType)
          case _ => super.visitFieldAccess(fieldAccess)
        }
      }

      // update the field index from right side
      override def visitInputRef(inputRef: RexInputRef): RexNode = {
        val rightIndex = leftRowType.getFieldCount + inputRef.getIndex
        new RexInputRef(rightIndex, inputRef.getType)
      }
    })
    val traitSet = snapshot.getTraitSet.replace(FlinkRelDistribution.BROADCAST_DISTRIBUTED)
    val newSnapshot = snapshot.copy(traitSet, snapshot.getInput, snapshot.getPeriod)
    val builder = call.builder()
    builder.push(leftInput)
    builder.push(newSnapshot)
    builder.join(correlate.getJoinType, joinCondition)

    val rel = builder.build()
    call.transformTo(rel)
  }
}

class BroadcastTemporalWithJoin extends BroadcastTemporal(
  operand(classOf[LogicalCorrelate],
    operand(classOf[RelNode], any()),
    operand(classOf[LogicalFilter],
      operand(classOf[LogicalSnapshot],
        operand(classOf[RelNode], any())))),
  "BroadcastTemporalWithJoin") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val snapshot: LogicalSnapshot = call.rel(3)
    val snapshotInput: RelNode = trimHep(call.rel(4))
    isLookupJoin(snapshot, snapshotInput)
  }

  override def getFilterCondition(call: RelOptRuleCall): RexNode = {
    val filter: LogicalFilter = call.rel(2)
    filter.getCondition
  }

  override def getLogicalSnapshot(call: RelOptRuleCall): LogicalSnapshot = {
    call.rels(3).asInstanceOf[LogicalSnapshot]
  }

  override def getLogicalSnapshotInput(call: RelOptRuleCall): RelNode = {
    trimHep(call.rel(4))
  }
}


object BroadcastTemporal {
  val LOOKUP_BROADCAST_JOIN_WITH_FILTER = new BroadcastTemporalWithJoin
}