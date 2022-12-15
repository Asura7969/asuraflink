package org.apache.flink.table.planner.delegation

import org.apache.calcite.plan.RelOptTable
import org.slf4j.{Logger, LoggerFactory}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeField}
import org.apache.calcite.rel.core.{Join, TableScan, Union}
import org.apache.flink.table.catalog.{CatalogBaseTable, CatalogManager, ObjectIdentifier}
import org.apache.flink.table.planner.plan.nodes.calcite.LegacySink
import org.apache.flink.table.planner.plan.nodes.common.CommonPhysicalWindowTableFunction
import org.apache.flink.table.planner.plan.nodes.physical.stream.{StreamPhysicalDataStreamScan, StreamPhysicalLegacySink, StreamPhysicalLegacyTableSourceScan, StreamPhysicalLookupJoin, StreamPhysicalRel, StreamPhysicalSink}
import org.apache.flink.table.planner.plan.schema.{DataStreamTable, FlinkPreparingTableBase, LegacyTableSourceTable, TableSourceTable}
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil.toScala

import scala.collection.JavaConversions._
import java.util
import java.util.Collections
import scala.collection.JavaConverters.{asScalaBufferConverter, mapAsScalaMapConverter}

/**
 * @author asura7969
 * @create 2022-12-12-20:15
 */
object CollectLineage {

  val LOG: Logger = LoggerFactory.getLogger(classOf[CollectLineage])
  val UNKNOWN:String = "UNKNOWN"


  def isAnonymous(v: String): Boolean = v.startsWith("*anonymous")

  def getTableName(identifier: ObjectIdentifier,
                   catalogManager: CatalogManager): TableName = {
    val tableOption = catalogManager.getTable(identifier)
    toScala(tableOption) match {
      case Some(table) =>
        TableName(identifier.asSummaryString(),
          anonymous = if (table.isAnonymous) table.toString else "",
          isTemporal = table.isTemporary,
          options = table.getTable[CatalogBaseTable].getOptions)
      case _ => TableName.unknown()
    }
  }

  def parseLookupJoin(downTable: String,
                      lookupJoin: StreamPhysicalLookupJoin,
                      catalogManager: CatalogManager): Seq[Table] = {
    // val joinType = lookupJoin.joinType
    // val info = lookupJoin.joinInfo
    // lookupJoin.allLookupKeys
    val temporalTable = lookupJoin.temporalTable
    val tableIdentifier: ObjectIdentifier = temporalTable match {
      case t: TableSourceTable => t.contextResolvedTable.getIdentifier
      case t: LegacyTableSourceTable[_] => t.tableIdentifier
    }
    val tableName = getTableName(tableIdentifier, catalogManager)
    Seq(Table(tableName, temporalTable.getRowType, Seq.empty))
    val input = lookupJoin.getInput
    parseRelNode(downTable,
      Collections.singletonList(input),
      catalogManager) ++ Seq(Table(tableName, temporalTable.getRowType, Seq.empty))
  }

  private def parseRelNode(downTable: String,
                           inputs: util.List[RelNode],
                           catalogManager: CatalogManager): Seq[Table] = {
    inputs.flatMap {
      case join: Join =>
        // val joinType = join.getJoinType
        // val condition = join.getCondition
        val left = join.getLeft
        val right = join.getRight

        val leftTable = parseRelNode(downTable, left.getInputs, catalogManager)
        val rightTable = parseRelNode(downTable, right.getInputs, catalogManager)
        leftTable.++(rightTable)

      case tableScan: TableScan =>
        tableScan.getTable match {
          case tb: FlinkPreparingTableBase =>
            val tableName: TableName = tb match {
              case tst: TableSourceTable =>
                getTableName(tst.contextResolvedTable.getIdentifier, catalogManager)

              case ltst: LegacyTableSourceTable[_] =>
                val identifier = ltst.tableIdentifier
                val schema = ltst.tableSource.getTableSchema
                schema.getFieldDataTypes
                TableName(identifier.asSummaryString(), "")

              case dsTable: DataStreamTable[_] =>
                TableName(dsTable.getNames.mkString("."), "")

              case _@x =>
                println(x)
                TableName.unknown()
            }
            Seq(Table(tableName, tb.getRowType, Seq.empty))

          case _@unknown =>
            LOG.warn(s"${unknown.getClass.getSimpleName} unsupport")
            Seq.empty
        }
      case legacySink: LegacySink =>
        // TODO
        Seq.empty

      case lookupJoin: StreamPhysicalLookupJoin =>
        parseLookupJoin(downTable, lookupJoin, catalogManager)

      case sink: StreamPhysicalSink =>
        // TODO
        Seq.empty

      case union: Union =>
        // TODO
        Seq.empty

      case commonPhysicalWindowTableFunction: CommonPhysicalWindowTableFunction =>
        // TODO
        Seq.empty

      case in =>
        parseRelNode(downTable, in.getInputs, catalogManager)
    }
  }

  def getCatalog[T](fieldName: String, catalogManager: CatalogManager):T = {
    val field = classOf[CatalogManager].getDeclaredField(fieldName)
    field.setAccessible(true)
    field.get(catalogManager).asInstanceOf[T]
  }

  def buildLineageResult(catalogManager: CatalogManager, optRelNode: RelNode): Unit = {

    if (optRelNode.isInstanceOf[StreamPhysicalRel]) {
      val rootTable = optRelNode match {
        case sps: StreamPhysicalSink =>
          val identifier = sps.contextResolvedTable.getIdentifier
          val tableName = if (isAnonymous(identifier.asSummaryString())) {
            TableName(identifier.asSummaryString(), identifier.asSummaryString())
          } else getTableName(identifier, catalogManager)

          Table(tableName, optRelNode.getRowType,
            parseRelNode(identifier.asSummaryString(), optRelNode.getInputs, catalogManager))


        case spls: StreamPhysicalLegacySink[_] =>
          // TODO
          Table(TableName.unknown(), optRelNode.getRowType, Seq())
        // source 和 join relNode不应该出现在第一个位置
//        case spds: StreamPhysicalDataStreamScan =>
//
//        case spltss: StreamPhysicalLegacyTableSourceScan =>
//
//        case lookupJoin: StreamPhysicalLookupJoin =>
//          parseLookupJoin("", lookupJoin, catalogManager)

        case _@unknown =>
          LOG.warn(s"${unknown.getClass.getSimpleName} unsupport")
          Table(TableName.unknown(), optRelNode.getRowType, Seq())
      }
      println(rootTable.toString)


    } else {
      LOG.warn(s"Only stream is supported.")
    }

  }

}

class CollectLineage

case class TableName(identifierName: String,
                     anonymous:String,
                     isTemporal:Boolean = false,
                     options: util.Map[String, String] = Collections.emptyMap()) {

  def isUnknown = identifierName.equals(CollectLineage.UNKNOWN)
  override def toString: String = {

    s"""
       |table: $identifierName
       |anonymous: $anonymous
       |isTemporal: $isTemporal
       |options: {
       |  ${options.asScala.map(each => s"${each._1} = ${each._2}").mkString("\n  ")}
       |}
       |""".stripMargin
  }
}

object TableName {
  def unknown(): TableName = {
    TableName(CollectLineage.UNKNOWN, CollectLineage.UNKNOWN)
  }
}

/**
 * 表
 * @param name 表名
 * @param rowType 列数据
 * @param upNodes 上游表信息
 */
case class Table(name:TableName,
                 rowType: RelDataType,
                 upNodes: Seq[Table]) {
  override def toString: String = {

    val fieldList: util.List[RelDataTypeField] = rowType.getFieldList
    val fields = fieldList.asScala
      .map(field => s"filed name: ${field.getName}, type:${field.getType}")
      .mkString("\n  ")
    val deadLine = if (upNodes.nonEmpty) s"\n    ${upNodes.mkString("\n    ")}" else ""

    s"""
       |$name
       |scheam: {
       |  $fields
       |}
       |
       |$deadLine
       |""".stripMargin
  }
}
