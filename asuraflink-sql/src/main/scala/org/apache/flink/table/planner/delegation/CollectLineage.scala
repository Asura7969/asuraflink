package org.apache.flink.table.planner.delegation

import org.slf4j.{Logger, LoggerFactory}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeField}
import org.apache.calcite.rel.core.{Join, TableScan}
import org.apache.flink.table.catalog.{AbstractCatalog, Catalog, CatalogBaseTable, CatalogManager, CatalogTable, CatalogView, GenericInMemoryCatalog, ObjectIdentifier, ObjectPath, ResolvedCatalogBaseTable}
import org.apache.flink.table.planner.plan.nodes.physical.stream.{StreamPhysicalDataStreamScan, StreamPhysicalLegacySink, StreamPhysicalLegacyTableSourceScan, StreamPhysicalLookupJoin, StreamPhysicalRel, StreamPhysicalSink, StreamPhysicalSort, StreamPhysicalTableSourceScan}
import org.apache.flink.table.planner.plan.schema.{FlinkPreparingTableBase, LegacyTableSourceTable, TableSourceTable}

import scala.collection.JavaConversions._
import java.util
import scala.collection.JavaConverters.{asScalaBufferConverter, mapAsScalaMapConverter}
/**
 * @author asura7969
 * @create 2022-12-12-20:15
 */
object CollectLineage {
  val LOG: Logger = LoggerFactory.getLogger(classOf[CollectLineage])
  val UNKNOWN:String = "UNKNOWN"



  private def findFromCatalog(identifierName: String,
                              catalogManager: CatalogManager): Option[TableName] = {
    val tablePath = identifierName.substring(identifierName.indexOf(".") + 1)
    val catalogs = getCatalog[util.Map[String, Catalog]]("catalogs", catalogManager)
    catalogs.asScala.find(t => {
      t._2 match {
        case catalog: GenericInMemoryCatalog =>
          val objectPath = ObjectPath.fromString(tablePath)
          catalog.tableExists(objectPath)
      }
    }).map(_ => TableName(identifierName, ""))

  }


  private def findFromTemporary(identifierName: String,
                                catalogManager: CatalogManager): Option[TableName] = {
    val temporaryTables = getCatalog[util.Map[ObjectIdentifier, CatalogBaseTable]]("temporaryTables", catalogManager)

    temporaryTables.asScala.find(t => {
      t._2 match {
        case rcbt: ResolvedCatalogBaseTable[_] =>
          // rcbt.getOrigin.getDescription
          rcbt.getOrigin.getComment.contains(identifierName)
        case view: CatalogView =>
          LOG.warn("temporal CatalogView 解析还未开发")
          false
        case table: CatalogTable =>
          LOG.warn("temporal CatalogTable 解析还未开发")
          false
      }
    }).map(a => TableName(a._1.asSummaryString(), identifierName, isTemporal = true))
  }

  private def parseRelNode(downTable: String,
                           inputs: util.List[RelNode],
                           catalogManager: CatalogManager): Seq[Table] = {
    inputs.flatMap {
      case join: Join =>
        val joinType = join.getJoinType
        val condition = join.getCondition
        val left = join.getLeft
        val right = join.getRight

        val leftTable = parseRelNode(downTable, left.getInputs, catalogManager)
        val rightTable = parseRelNode(downTable, right.getInputs, catalogManager)
        leftTable.++(rightTable)

      case tableScan@(_: TableScan) =>
        tableScan.getTable match {
          case tb: FlinkPreparingTableBase =>
            val tableName: TableName = tb match {
              case tst: TableSourceTable =>
                val fullName = tst.contextResolvedTable.getIdentifier.asSummaryString()
                findFromTemporary(fullName, catalogManager)
                  .getOrElse(findFromCatalog(fullName, catalogManager)
                  .getOrElse(TableName.unknown()))

              case ltst: LegacyTableSourceTable[_] =>
                val identifier = ltst.tableIdentifier
                val schema = ltst.tableSource.getTableSchema
                schema.getFieldDataTypes
                TableName(identifier.asSummaryString(), "")
              case _ =>
                println(tb)
                TableName.unknown()
            }
            Seq(Table(tableName, tb.getRowType, Seq.empty))

          case _ => Seq.empty
        }
      case sptss@(_: StreamPhysicalTableSourceScan) =>
        Seq(Table(TableName(sptss.tableSource.asSummaryString(), ""), sptss.getRowType, Seq.empty))

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
      optRelNode match {
        case sps: StreamPhysicalSink =>
          val sink = sps.tableSink
          val sinkTableName = sink.asSummaryString()

          val rootTable = Table(TableName(sinkTableName, ""), optRelNode.getRowType,
            parseRelNode(sinkTableName, optRelNode.getInputs, catalogManager))

          println(rootTable.toString)
        case spds: StreamPhysicalDataStreamScan =>

        case spls: StreamPhysicalLegacySink[_] =>

        case spltss: StreamPhysicalLegacyTableSourceScan =>

        case splj: StreamPhysicalLookupJoin =>

        case spsort: StreamPhysicalSort =>
        case _@unknown =>
          LOG.error(s"${unknown.getClass.getSimpleName} unsupport")
      }


    } else {
      LOG.warn(s"Only stream is supported.")
    }

  }

}

class CollectLineage

case class TableName(identifierName: String, anonymous:String, isTemporal:Boolean = false) {
  override def toString: String = {
    s"""
       |$identifierName[$anonymous, isTemporal=$isTemporal]
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
       |  $fields $deadLine
       |""".stripMargin
  }
}
