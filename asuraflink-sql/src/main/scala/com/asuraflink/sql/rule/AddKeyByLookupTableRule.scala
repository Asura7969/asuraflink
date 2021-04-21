package com.asuraflink.sql.rule

import org.apache.calcite.plan.RelOptRule.{any, operand, unordered}
import org.apache.calcite.plan.{RelOptRuleCall, RelOptRuleOperand}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataTypeField
import org.apache.calcite.rel.logical.{LogicalCorrelate, LogicalFilter, LogicalSnapshot}
import org.apache.calcite.rex.{RexCall, RexCorrelVariable, RexFieldAccess, RexInputRef, RexNode, RexShuttle}
import org.apache.flink.table.planner.plan.rules.logical.{LogicalCorrelateToJoinFromLookupTemporalTableRule, LogicalCorrelateToJoinFromTemporalTableRule}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * @author asura7969
 * @create 2021-04-17-13:13
 */
class AddKeyByLookupTableRule
  extends LogicalCorrelateToJoinFromLookupTemporalTableRule(
    operand(classOf[LogicalCorrelate],
      operand(classOf[RelNode], any()),
      operand(classOf[LogicalFilter],
        operand(classOf[LogicalSnapshot],
          operand(classOf[RelNode], any())))),
    "AddKeyByLookupTableRule") {

  override def getFilterCondition(call: RelOptRuleCall): RexNode = {
    val filter: LogicalFilter = call.rel(2)
    filter.getCondition
  }

  override def getLogicalSnapshot(call: RelOptRuleCall): LogicalSnapshot = {
    call.rels(3).asInstanceOf[LogicalSnapshot]
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val correlate: LogicalCorrelate = call.rel(0)
    val leftInput: RelNode = call.rel(1)
    val filterCondition = getFilterCondition(call).asInstanceOf[RexCall]
    val snapshot = getLogicalSnapshot(call)

    val leftRowType = leftInput.getRowType
    val leftJoinFields = new ArrayBuffer[RexInputRef]()
    val joinCondition = filterCondition.accept(new RexShuttle() {
      // change correlate variable expression to normal RexInputRef (which is from left side)
      override def visitFieldAccess(fieldAccess: RexFieldAccess): RexNode = {
        fieldAccess.getReferenceExpr match {
          case corVar: RexCorrelVariable =>
            require(correlate.getCorrelationId.equals(corVar.id))
            val index = leftRowType.getFieldList.indexOf(fieldAccess.getField)
            leftJoinFields.append(RexInputRef.of(index, leftRowType))
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

    val builder = call.builder()
    if (leftJoinFields.nonEmpty) {
      val value = builder.push(leftInput) groupKey leftJoinFields
      builder.push(value)
    } else {
      builder.push(leftInput)
    }

    builder.push(snapshot)
    builder.join(correlate.getJoinType, filterCondition)

    val rel = builder.build()
    call.transformTo(rel)
  }
}
