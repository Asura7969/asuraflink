package com.asuraflink.sql.rule.utils

import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.logical.{LogicalCorrelate, LogicalProject, LogicalSnapshot, LogicalTableScan}
import org.apache.calcite.rex.{RexCorrelVariable, RexFieldAccess}
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.connector.source.LookupTableSource
import org.apache.flink.table.planner.plan.nodes.logical.{FlinkLogicalLegacyTableSourceScan, FlinkLogicalTableSourceScan}
import org.apache.flink.table.planner.plan.schema.{LegacyTableSourceTable, TableSourceTable, TimeIndicatorRelDataType}
import org.apache.flink.table.sources.LookupableTableSource

/**
 * @author asura7969
 * @create 2021-04-30-22:38
 */
object RuleUtils {

  def validateSnapshotInCorrelate(snapshot: LogicalSnapshot,
                                  correlate: LogicalCorrelate): Unit = {
    // period specification check
    snapshot.getPeriod.getType match {
      // validate type is event-time or processing time
      case t: TimeIndicatorRelDataType => // do nothing
      case _ =>
        throw new ValidationException("Temporal table join currently only supports " +
          "'FOR SYSTEM_TIME AS OF' left table's time attribute field")
    }

    snapshot.getPeriod match {
      // validate period comes from left table's field
      case r: RexFieldAccess if r.getReferenceExpr.isInstanceOf[RexCorrelVariable] &&
        correlate.getCorrelationId.equals(r.getReferenceExpr.asInstanceOf[RexCorrelVariable].id)
      => // do nothing
      case _ =>
        throw new ValidationException("Temporal table join currently only supports " +
          "'FOR SYSTEM_TIME AS OF' left table's time attribute field'")
    }
  }

  def isLookupJoin(snapshot: LogicalSnapshot, snapshotInput: RelNode): Boolean = {
    val isProcessingTime = snapshot.getPeriod.getType match {
      case t: TimeIndicatorRelDataType if !t.isEventTime => true
      case _ => false
    }

    val tableScan = getTableScan(snapshotInput)
    val snapshotOnLookupSource = tableScan match {
      case Some(scan) => isTableSourceScan(scan) && isLookupTableSource(scan)
      case _ => false
    }

    isProcessingTime && snapshotOnLookupSource
  }

  def getTableScan(snapshotInput: RelNode): Option[TableScan] = {
    snapshotInput match {
      case tableScan: TableScan => Some(tableScan)
      // computed column on lookup table
      case project: LogicalProject => getTableScan(trimHep(project.getInput))
      case _ => None
    }
  }

  /** Trim out the HepRelVertex wrapper and get current relational expression. */
  def trimHep(node: RelNode): RelNode = {
    node match {
      case hepRelVertex: HepRelVertex =>
        hepRelVertex.getCurrentRel
      case _ => node
    }
  }

  def isTableSourceScan(relNode: RelNode): Boolean = {
    relNode match {
      case r: LogicalTableScan =>
        val table = r.getTable
        table match {
          case _: LegacyTableSourceTable[Any] | _: TableSourceTable => true
          case _ => false
        }
      case _: FlinkLogicalLegacyTableSourceScan | _: FlinkLogicalTableSourceScan => true
      case _ => false
    }
  }

  def isLookupTableSource(relNode: RelNode): Boolean = relNode match {
    case scan: FlinkLogicalLegacyTableSourceScan =>
      scan.tableSource.isInstanceOf[LookupableTableSource[_]]
    case scan: FlinkLogicalTableSourceScan =>
      scan.tableSource.isInstanceOf[LookupTableSource]
    case scan: LogicalTableScan =>
      scan.getTable match {
        case table: LegacyTableSourceTable[_] =>
          table.tableSource.isInstanceOf[LookupableTableSource[_]]
        case table: TableSourceTable =>
          table.tableSource.isInstanceOf[LookupTableSource]
        case _ => false
      }
    case _ => false
  }
}
