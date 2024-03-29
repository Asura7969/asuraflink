package org.apache.flink.table.planner.delegation

import org.slf4j.{Logger, LoggerFactory}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeField}
import org.apache.calcite.rel.core.{Join, TableScan, Union}
import org.apache.flink.configuration.ConfigOptions.key
import org.apache.flink.configuration.{ConfigOption, Configuration}
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.catalog.{CatalogBaseTable, CatalogManager, ObjectIdentifier, ResolvedSchema}
import org.apache.flink.table.planner.plan.nodes.calcite.LegacySink
import org.apache.flink.table.planner.plan.nodes.common.CommonPhysicalWindowTableFunction
import org.apache.flink.table.planner.plan.nodes.physical.stream.{StreamPhysicalCalc, StreamPhysicalDataStreamScan, StreamPhysicalJoin, StreamPhysicalLegacySink, StreamPhysicalLegacyTableSourceScan, StreamPhysicalLookupJoin, StreamPhysicalRel, StreamPhysicalSink, StreamPhysicalUnion}
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

  val COLLECT_IMPL_KEY: String = "collect-lineage-impl"
  val COLLECT_IMPL: ConfigOption[String] = key(COLLECT_IMPL_KEY).stringType().defaultValue("log")

  val LOG: Logger = LoggerFactory.getLogger(classOf[CollectLineage])
  val UNKNOWN:String = "UNKNOWN"


  def isAnonymous(v: String): Boolean = v.startsWith("*anonymous")

  def getTableMeta(identifier: ObjectIdentifier,
                   catalogManager: CatalogManager): TableMeta = {
    val name = identifier.asSummaryString()
    if (isAnonymous(name)) return TableMeta(name, anonymous = true)

    val tableOption = catalogManager.getTable(identifier)
    toScala(tableOption) match {
      case Some(table) =>
        TableMeta(identifier.asSummaryString(),
          anonymous = table.isAnonymous,
          isTemporal = table.isTemporary,
          schema = Some(table.getResolvedSchema),
          options = table.getTable[CatalogBaseTable].getOptions)
      case _ => TableMeta.unknown()
    }
  }

  private def parseLookupJoin(downTable: String,
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
    val meta = getTableMeta(tableIdentifier, catalogManager)
    Seq(Table(meta, temporalTable.getRowType, Seq.empty))
    val input = lookupJoin.getInput
    parseRelNode(downTable,
      Collections.singletonList(input),
      catalogManager) ++ Seq(Table(meta, temporalTable.getRowType, Seq.empty))
  }

  private def parseRelNode(downTable: String,
                           inputs: util.List[RelNode],
                           catalogManager: CatalogManager): Seq[Table] = {
    inputs.flatMap {
      case calc: StreamPhysicalCalc =>

        /**
         * TODO: 参考 {@link CommonCalc#projectionToString}
         */
        val calcProgram = calc.getProgram
        val projectList = calcProgram.getProjectList
        val inputFieldNames = calcProgram.getInputRowType.getFieldNames.toList
        val localExprs = calcProgram.getExprList.toList
        val outputFieldNames = calcProgram.getOutputRowType.getFieldNames.toList
        // calcProgram.getNamedProjects
        parseRelNode(downTable, calc.getInputs, catalogManager)

      case join: Join =>
        // val joinType = join.getJoinType
        // val condition = join.getCondition

        val left = join.getLeft
        val right = join.getRight
        val cluster = join.getCluster
        val leftTable = parseRelNode(downTable, left.getInputs, catalogManager)
        val rightTable = parseRelNode(downTable, right.getInputs, catalogManager)
        leftTable.++(rightTable)

      case tableScan: TableScan =>
        tableScan.getTable match {
          case tb: FlinkPreparingTableBase =>
            val tableName: TableMeta = tb match {
              case tst: TableSourceTable =>
                getTableMeta(tst.contextResolvedTable.getIdentifier, catalogManager)

              case ltst: LegacyTableSourceTable[_] =>
                val identifier = ltst.tableIdentifier
                TableMeta(identifier.asSummaryString())

              case dsTable: DataStreamTable[_] =>
                TableMeta(dsTable.getNames.mkString("."))

              case _@x =>
                throw new RuntimeException(s"RelNode unsupported ${x.getClass.getSimpleName}")
            }
            Seq(Table(tableName, tb.getRowType, Seq.empty))

          case _@unknown =>
            throw new RuntimeException(s"RelNode unsupported ${unknown.getClass.getSimpleName}")
        }

      case lookupJoin: StreamPhysicalLookupJoin =>
        parseLookupJoin(downTable, lookupJoin, catalogManager)

//      case union: StreamPhysicalUnion =>
//      case commonPhysicalWindowTableFunction: CommonPhysicalWindowTableFunction =>
//      case legacySink: LegacySink =>
//      case sink: StreamPhysicalSink =>

      case in =>
        parseRelNode(downTable, in.getInputs, catalogManager)
    }
  }

  def buildLineageResult(tableConfig: TableConfig,
                         catalogManager: CatalogManager,
                         optRelNode: RelNode): Unit = {

    if (optRelNode.isInstanceOf[StreamPhysicalRel]) {
      val rootTable = optRelNode match {
        case sps: StreamPhysicalSink =>
          val identifier = sps.contextResolvedTable.getIdentifier
          val name = identifier.asSummaryString()
          val tableName = getTableMeta(identifier, catalogManager)

          Table(tableName, sps.getRowType,
            parseRelNode(name, sps.getInputs, catalogManager))

        case spls: StreamPhysicalLegacySink[_] =>
          // TODO: 未测试
          Table(TableMeta(spls.sinkName), spls.deriveRowType,
            parseRelNode(spls.sinkName, spls.getInputs, catalogManager))

        // source 和 join relNode不应该出现在第一个位置
//        case spds: StreamPhysicalDataStreamScan =>
//        case spltss: StreamPhysicalLegacyTableSourceScan =>
//        case lookupJoin: StreamPhysicalLookupJoin =>
//          parseLookupJoin("", lookupJoin, catalogManager)

        case _@unknown =>
          throw new RuntimeException(s"Root RelNode unsupported ${unknown.getClass.getSimpleName}")
      }
      tableConfig.getConfiguration.get[String](COLLECT_IMPL) match {
        case "log" => LOG.info(rootTable.toString)
        case _@impl => LOG.warn(s"Unsupported collect impl: $impl")
      }

    } else LOG.warn(s"Only stream is supported.")
  }
}

class CollectLineage

case class TableMeta(identifierName: String,
                     anonymous:Boolean = false,
                     isTemporal:Boolean = false,
                     schema: Option[ResolvedSchema] = None,
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

object TableMeta {
  def unknown(): TableMeta = {
    TableMeta(CollectLineage.UNKNOWN)
  }
}

/**
 * 表
 * @param name 表名
 * @param rowType 列数据
 * @param upNodes 上游表信息
 */
case class Table(name:TableMeta,
                 rowType: RelDataType,
                 upNodes: Seq[Table]) {
  override def toString: String = {

    val fields = name.schema match {
      case Some(s) =>
        s.getColumns.asScala
          .map(c => s"filed name: ${c.getName}, type:${c.getDataType.toString}")
          .mkString("\n  ")
      case _ =>
        val fieldList = rowType.getFieldList
        fieldList.asScala
          .map(field => s"filed name: ${field.getName}, type:${field.getType}")
          .mkString("\n  ")
    }

    val deadLine = if (upNodes.nonEmpty) s"\n    ${upNodes.mkString("\n    ")}" else ""

    s"""
       |$name
       |schema: {
       |  $fields
       |}
       |$deadLine
       |""".stripMargin
  }
}
