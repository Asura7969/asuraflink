package org.apache.flink.table.planner.delegation

import org.apache.calcite.rel.{BiRel, RelNode}
import org.apache.calcite.rel.`type`.RelDataTypeField
import org.apache.calcite.rel.logical.{LogicalAggregate, LogicalCorrelate, LogicalJoin, LogicalProject, LogicalTableScan, LogicalUnion, LogicalValues}
import org.apache.calcite.rex.{RexCall, RexCorrelVariable, RexDynamicParam, RexFieldAccess, RexInputRef, RexLiteral, RexLocalRef, RexOver, RexPatternFieldRef, RexRangeRef, RexSubQuery, RexTableInputRef, RexVisitor}
import org.apache.flink.configuration.ConfigOption
import org.apache.flink.configuration.ConfigOptions.key
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.catalog.{CatalogManager, ObjectIdentifier}
import org.apache.flink.table.planner.plan.nodes.calcite.LogicalSink
import org.apache.flink.table.planner.plan.schema.{FlinkPreparingTableBase, TableSourceTable}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
import scala.annotation.tailrec
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable

class LogicalNodeCollector

object LogicalNodeCollector {

  val LOG: Logger = LoggerFactory.getLogger(classOf[LogicalNodeCollector])
  val COLLECT_IMPL_KEY: String = "collect-lineage-impl"
  val COLLECT_IMPL: ConfigOption[String] = key(COLLECT_IMPL_KEY).stringType().defaultValue("log")
  def collectSqlFieldsLinage(tableConfig: TableConfig,
                             catalogManager: CatalogManager,
                             optRelNode: RelNode): Unit = {
    try {
      val fieldsLinage = collectField(tableConfig, catalogManager, optRelNode)
      printColumns(fieldsLinage, tableConfig)
    } catch {
      case e: Exception => LOG.error("", e)
    }
  }

  private def collectField(tableConfig: TableConfig,
                           catalogManager: CatalogManager,
                           optRelNode: RelNode,
                           outFields: Option[Seq[Column]] = None): Option[Seq[Column]] = {

    optRelNode match {
      case logicalSink: LogicalSink =>
        val table = logicalSink.contextResolvedTable.getIdentifier
        val fieldToColumns: Seq[Column] = logicalSink.getRowType.getFieldList.asScala.map(field => Column.from(table, field))
        collectField(tableConfig, catalogManager, optRelNode.getInput(0), Some(fieldToColumns))

      case logicalProject: LogicalProject =>
        val v = new VisitorRexNode()
        val projects = logicalProject.getProjects.asScala
        (0 until projects.size()).foreach { idx =>
          projects(idx).accept(v) match {
            case Some(i) => outFields.get(idx).setIndexes(i)
            case _ =>
          }
        }
        collectField(tableConfig, catalogManager, logicalProject.getInput(0), outFields)

      case biRel: BiRel if biRel.isInstanceOf[LogicalJoin] || biRel.isInstanceOf[LogicalCorrelate] =>
        val left = biRel.getLeft
        val splitIndex = left.getRowType.getFieldCount
        parseSource(tableConfig, catalogManager, left, outFields, 0 until splitIndex)
        val right = biRel.getRight
        parseSource(tableConfig, catalogManager, right, outFields, splitIndex until splitIndex + right.getRowType.getFieldCount)

      case union: LogicalUnion =>
        val inputs = union.getInputs.asScala
        inputs.foreach(input => collectField(tableConfig, catalogManager, input, outFields))
        outFields

      case logicalAggregate: LogicalAggregate =>
        collectField(tableConfig, catalogManager, logicalAggregate.getInput, outFields)

      case logicalTableScan: LogicalTableScan =>
        parseSource(tableConfig, catalogManager, logicalTableScan, outFields, 0 until logicalTableScan.getRowType.getFieldCount)

      case _: LogicalValues => None

      case _@unknown =>
        collectField(tableConfig, catalogManager, unknown.getInput(0), outFields)

    }
  }

  def printColumns(outFields: Option[Seq[Column]], tableConfig: TableConfig): Unit = {
    outFields match {
      case Some(value) =>
        tableConfig.getConfiguration.get[String](COLLECT_IMPL) match {
          case "log" => LOG.info(s"${value.map(_.toString).mkString("\n")}")
          case _@impl => LOG.warn(s"Unsupported collect impl: $impl")
        }
      case _ =>
    }
  }

  @tailrec
  def parseSource(tableConfig: TableConfig,
                  catalogManager: CatalogManager,
                  relNode: RelNode,
                  outFields: Option[Seq[Column]] = None,
                  startIndex: Seq[Int]): Option[Seq[Column]] = {

    def completeColumns(table: ObjectIdentifier,
                        fields: Seq[RelDataTypeField],
                        outFields: Option[Seq[Column]]): Option[Seq[Column]] = {
      outFields.get.foreach { c =>
        if (null != c.getIndexes && c.getIndexes.nonEmpty) {
          val columns = c.getIndexes.filter(startIndex.contains(_))
            .map(i => Column.from(table, fields(i - startIndex.head)))
          if (columns.nonEmpty) {
            c.up match {
              case Some(cols) => c.up = Some(cols.++(columns))
              case _ => c.up = Some(columns)
            }
          }
        }
      }
      outFields
    }

    relNode match {
      case logicalTableScan: LogicalTableScan =>
        logicalTableScan.getTable match {
          case tableSourceTable: TableSourceTable =>
            val table = tableSourceTable.contextResolvedTable.getIdentifier
            val fields = tableSourceTable.getRowType.getFieldList.asScala
            completeColumns(table, fields, outFields)

          case flinkPreparingTableBase: FlinkPreparingTableBase =>
            val names = flinkPreparingTableBase.getNames.asScala
            val table = ObjectIdentifier.of(names.head, names(1), names(2))
            val fields = flinkPreparingTableBase.getRowType.getFieldList.asScala
            completeColumns(table, fields, outFields)

          case _@unknown =>
              throw new RuntimeException(s"Unsupported source table: ${unknown.getClass.getSimpleName}")
        }
      case _@node =>
        parseSource(tableConfig, catalogManager, node.getInput(0), outFields, startIndex)
    }
  }
}

class VisitorRexNode extends RexVisitor[Option[Seq[Int]]] {
  override def visitInputRef(rexInputRef: RexInputRef): Option[Seq[Int]] = {
    Some(rexInputRef.getIndex :: Nil)
  }

  override def visitLocalRef(rexLocalRef: RexLocalRef): Option[Seq[Int]] = {
    Some(rexLocalRef.getIndex :: Nil)
  }

  override def visitLiteral(rexLiteral: RexLiteral): Option[Seq[Int]] = None

  override def visitCall(rexCall: RexCall): Option[Seq[Int]] = {
    Some(rexCall.operands.asScala.flatten(_.accept(this)).flatten)
  }

  override def visitOver(rexOver: RexOver): Option[Seq[Int]] = {
    throw new RuntimeException("Unsupported rexOver")
  }

  override def visitCorrelVariable(rexCorrelVariable: RexCorrelVariable): Option[Seq[Int]] = {
    throw new RuntimeException("Unsupported rexCorrelVariable")
  }

  override def visitDynamicParam(rexDynamicParam: RexDynamicParam): Option[Seq[Int]] = {
    throw new RuntimeException("Unsupported rexDynamicParam")
  }

  override def visitRangeRef(rexRangeRef: RexRangeRef): Option[Seq[Int]] = {
    throw new RuntimeException("Unsupported rexRangeRef")
  }

  override def visitFieldAccess(rexFieldAccess: RexFieldAccess): Option[Seq[Int]] = {
    throw new RuntimeException("Unsupported rexFieldAccess")
  }

  override def visitSubQuery(rexSubQuery: RexSubQuery): Option[Seq[Int]] = {
    throw new RuntimeException("Unsupported rexSubQuery")
  }

  override def visitTableInputRef(rexTableInputRef: RexTableInputRef): Option[Seq[Int]] = {
    throw new RuntimeException("Unsupported rexTableInputRef")
  }

  override def visitPatternFieldRef(rexPatternFieldRef: RexPatternFieldRef): Option[Seq[Int]] = {
    throw new RuntimeException("Unsupported rexPatternFieldRef")
  }
}

class Column(table: ObjectIdentifier, fieldName: String, var up: Option[Seq[Column]] = None) {
  private var indexes: Seq[Int] = _

  def setIndexes(idxes: Seq[Int]): Unit = indexes = idxes
  def getIndexes: Seq[Int] = indexes

  override def toString: String = {
    s"table: ${table.asSummaryString()}, field: $fieldName${if (up.nonEmpty) s", ${up.get.toString}" else ""}"
  }
}

object Column {

  def from(table: ObjectIdentifier, field: RelDataTypeField): Column = new Column(table, field.getName, None)
}
