//package org.apache.flink.table.planner.plan.nodes.physical.stream
//
//import java.util
//
//import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
//import org.apache.calcite.rel.{RelNode, SingleRel}
//import org.apache.calcite.rel.`type`.RelDataType
//import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
//import org.apache.flink.api.dag.Transformation
//import org.apache.flink.streaming.api.transformations.OneInputTransformation
//import org.apache.flink.table.api.DataTypes
//import org.apache.flink.table.data.RowData
//import org.apache.flink.table.planner.calcite.FlinkTypeFactory
//import org.apache.flink.table.planner.delegation.StreamPlanner
//import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, StreamExecNode}
//import org.apache.flink.table.runtime.operators.bundle.MiniBatchElementOperator
//import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
//import org.apache.flink.table.types.MiniBatchJoinType
//import org.apache.flink.table.types.logical.RowType
//
//import scala.collection.JavaConversions._
//
//class StreamExecBatchLookup(
//                             cluster: RelOptCluster,
//                             traitSet: RelTraitSet,
//                             inputRel: RelNode,
//                             outputRowType: RelDataType)
//  extends SingleRel(cluster, traitSet, inputRel)
//    with StreamPhysicalRel with StreamExecNode[RowData] {
//  /**
//   * Whether the [[StreamPhysicalRel]] requires rowtime watermark in processing logic.
//   */
//  override def requireWatermark: Boolean = false
//
//  override def getInputNodes: util.List[ExecNode[StreamPlanner, _]] = {
//    getInputs.map(_.asInstanceOf[ExecNode[StreamPlanner, _]])
//  }
//
//  override def replaceInputNode(ordinalInParent: Int,
//                                newInputNode: ExecNode[StreamPlanner, _]): Unit = {
//    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
//  }
//
//  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
//    new StreamExecBatchLookup(cluster, traitSet, inputs.get(0), outputRowType)
//  }
//
//  override protected def translateToPlanInternal(planner: StreamPlanner): Transformation[RowData] = {
//
//    val input = getInputNodes.get(0).translateToPlan(planner)
//      .asInstanceOf[Transformation[RowData]]
//
//    val inRowType = FlinkTypeFactory.toLogicalRowType(getInput.getRowType)
//    val outRowType = FlinkTypeFactory.toLogicalRowType(outputRowType)
//
//    val tableConfig = planner.getTableConfig
//    val interval = tableConfig.getConfiguration.getLong(
//      "lookup.table.exec.interval.ms", 10000)
//    val maxCount = tableConfig.getConfiguration.getLong(
//      "lookup.table.exec.mini-batch.size", 1000)
//
//    val operator = new MiniBatchElementOperator(interval, maxCount)
//
//    val returnType: InternalTypeInfo[RowData] = InternalTypeInfo.of(
//      RowType.of(
//        DataTypes.RAW(TypeInformation.of(
//          new TypeHint[MiniBatchJoinType] {})).getLogicalType))
//
//    val transformation = new OneInputTransformation(
//      input,
//      getRelDetailedDescription,
//      operator,
//      returnType,
//      input.getParallelism)
//
//    if (inputsContainSingleton()) {
//      transformation.setParallelism(1)
//      transformation.setMaxParallelism(1)
//    }
//
//    transformation
//  }
//}
