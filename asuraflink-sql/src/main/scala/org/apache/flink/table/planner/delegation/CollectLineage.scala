package org.apache.flink.table.planner.delegation

import org.slf4j.{Logger, LoggerFactory}
import org.apache.calcite.plan.RelOptTable
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeField}
import org.apache.calcite.rel.core.{Join, TableScan}
import org.apache.calcite.rel.metadata.RelColumnOrigin
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.commons.collections.CollectionUtils
import org.apache.flink.table.planner.plan.nodes.physical.stream.{StreamPhysicalCalc, StreamPhysicalCalcBase, StreamPhysicalSink, StreamPhysicalTemporalJoin}
import org.apache.flink.table.planner.plan.schema.{FlinkPreparingTableBase, TableSourceTable}
import org.apache.flink.table.types.logical.RowType

import scala.collection.JavaConversions._
import java.util
/**
 * @author asura7969
 * @create 2022-12-12-20:15
 */
object CollectLineage {
  val LOG: Logger = LoggerFactory.getLogger(classOf[CollectLineage])


  private def parseRelNode(downTable: String, inputs: util.List[RelNode]): Unit = {
    inputs.forEach {
      case join: Join =>
        val joinType = join.getJoinType
        val condition = join.getCondition
        val left = join.getLeft
        val right = join.getRight

        parseRelNode(downTable, left.getInputs)
        parseRelNode(downTable, right.getInputs)
      case input@(tableScan: TableScan) =>
        tableScan.getTable match {
          case tb: FlinkPreparingTableBase =>
            tb match {
              case tst: TableSourceTable => {
                val manager = tst.flinkContext.getCatalogManager
                val metadataQuery = input.getCluster.getMetadataQuery()
                val origins = metadataQuery.getColumnOrigins(input, 0)
                origins.foreach(relColumnOrigin => {
                  println(relColumnOrigin.getOriginTable.getQualifiedName)
                })

                println(manager.listSchemas())
                println(input)
              }
            }


            printlnColumn(tb.toString, tb.getRowType)
        }

      case in =>
        parseRelNode(downTable, in.getInputs)
    }

  }

  /**
   * print column
   * @param tableName tableName
   * @param rowType rowType of table
   */
  private def printlnColumn(tableName: String, rowType: RelDataType): Unit = {
    LOG.info(s"******************* Target table: $tableName *********************")
    val fieldList: util.List[RelDataTypeField] = rowType.getFieldList
    fieldList.forEach(field => {
      LOG.info(s"filed name: ${field.getName}, type:${field.getType}")
    })
    LOG.info("***********************************************************************")
  }

  def buildFiledLineageResult(optRelNode: RelNode): util.ArrayList[AnyRef] = {

    // check the size of query and sink fields match
    val sink = optRelNode.asInstanceOf[StreamPhysicalSink].tableSink
    val sinkTableName = sink.asSummaryString()

    printlnColumn(sinkTableName, optRelNode.getRowType)

    parseRelNode(sinkTableName, optRelNode.getInputs)


    val rowType: RelDataType = optRelNode.getRowType

    val targetColumnList = rowType.getFieldNames
    val metadataQuery = optRelNode.getCluster.getMetadataQuery
    val resultList = new util.ArrayList[AnyRef]
    var index = 0
    while ( {
      index < targetColumnList.size
    }) {
//      val targetColumn = targetColumnList.get(index)
//      LOG.debug("**********************************************************")
//      LOG.debug("Target table: {}", sinkTable)
//      LOG.debug("Target column: {}", targetColumn)
      val relColumnOriginSet = metadataQuery.getColumnOrigins(optRelNode, index)
//      LOG.info()
      if (CollectionUtils.isNotEmpty(relColumnOriginSet)) {
//
        for (relColumnOrigin <- relColumnOriginSet) { // table
          val table: RelOptTable = relColumnOrigin.getOriginTable
          val sourceTable = String.join(DELIMITER, table.getQualifiedName)
//          // filed
          val ordinal = relColumnOrigin.getOriginColumnOrdinal
          // List<String> fieldNames = table.getRowType().getFieldNames();
//          val fieldNames = table.asInstanceOf[TableSourceTable].catalogTable.getResolvedSchema.getColumnNames
//          val sourceColumn = fieldNames.get(ordinal)
//          LOG.debug("----------------------------------------------------------")
//          LOG.debug("Source table: {}", sourceTable)
//          LOG.debug("Source column: {}", sourceColumn)
//          // add record
//          resultList.add(buildResult(sourceTable, sourceColumn, sinkTable, targetColumn))
        }
      }
//
      index += 1
    }
    resultList
  }


//  private def validateSchema(sinkTable: String, relNode: RelNode, sinkFieldList: util.List[String]): Unit = {
//    val queryFieldList = relNode.getRowType.getFieldNames
//    if (queryFieldList.size ne sinkFieldList.size) throw new Nothing(String.format("Column types of query result and sink for %s do not match.\n" + "Query schema: %s\n" + "Sink schema:  %s", sinkTable, queryFieldList, sinkFieldList))
//  }

  val DELIMITER = "."
//  private def buildResult(sourceTablePath: String, sourceColumn: String, targetTablePath: String, targetColumn: String) = {
//    val sourceItems = sourceTablePath.split("\\" + DELIMITER)
//    val targetItems = targetTablePath.split("\\" + DELIMITER)
//    Result.builder.sourceCatalog(sourceItems(0)).sourceDatabase(sourceItems(1)).sourceTable(sourceItems(2)).sourceColumn(sourceColumn).targetCatalog(targetItems(0)).targetDatabase(targetItems(1)).targetTable(targetItems(2)).targetColumn(targetColumn).build
//  }
}

class CollectLineage {

}
