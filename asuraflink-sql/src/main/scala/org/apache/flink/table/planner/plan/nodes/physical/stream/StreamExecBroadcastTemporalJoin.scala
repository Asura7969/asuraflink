package org.apache.flink.table.planner.plan.nodes.physical.stream

import java.util
import java.util.{Collections, List}

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.{Join, JoinRelType}
import org.apache.calcite.rex.RexNode
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeHint, TypeInformation, Types}
import org.apache.flink.api.dag.Transformation
import org.apache.flink.api.java.typeutils.{ListTypeInfo, MapTypeInfo, ResultTypeQueryable}
import org.apache.flink.streaming.api.scala.KeyedStream
import org.apache.flink.streaming.api.transformations.{BroadcastStateTransformation, TwoInputTransformation}
import org.apache.flink.table.api.TableException
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.delegation.StreamPlanner
import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.planner.plan.nodes.common.CommonPhysicalJoin
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, StreamExecNode}
import org.apache.flink.table.planner.plan.utils.{RelExplainUtil, TemporalJoinUtil}
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.types.logical.RowType

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class StreamExecBroadcastTemporalJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    leftRel: RelNode,
    rightRel: RelNode,
    condition: RexNode,
    joinType: JoinRelType)
  extends CommonPhysicalJoin(cluster, traitSet, leftRel, rightRel, condition, joinType)
    with StreamPhysicalRel
    with StreamExecNode[RowData] {

  override def requireWatermark: Boolean = TemporalJoinUtil.isRowTimeJoin(cluster.getRexBuilder, getJoinInfo)

  override protected def translateToPlanInternal(planner: StreamPlanner): Transformation[RowData] = {
    validateKeyTypes()

    val returnType = FlinkTypeFactory.toLogicalRowType(getRowType)

    val joinTranslator = StreamExecTemporalJoinToCoProcessTranslator.create(
      this.toString,
      planner.getTableConfig,
      returnType,
      leftRel,
      rightRel,
      getJoinInfo,
      cluster.getRexBuilder)

    val joinOperator = joinTranslator.getJoinOperator(joinType, returnType.getFieldNames)
    val leftKeySelector = joinTranslator.getLeftKeySelector
    val rightKeySelector = joinTranslator.getRightKeySelector

    val leftTransform = getInputNodes.get(0).translateToPlan(planner)
      .asInstanceOf[Transformation[RowData]]
//    val rightTransform = getInputNodes.get(1).translateToPlan(planner)
//      .asInstanceOf[Transformation[RowData]]

    val (rightTransform, broadcast) = getInputNodes.get(1) match {
      case e:StreamExecExchange =>
        if (e.getTraitSet.contains(FlinkRelDistribution.BROADCAST_DISTRIBUTED)) {
          (e.getInputNodes.get(0).translateToPlan(planner)
            .asInstanceOf[Transformation[RowData]], true)
        } else {
          noBroadcast(e, planner)
        }
      case _ =>
        noBroadcast(getInputNodes.get(1), planner)
    }

    if (!broadcast) {
      val ret = new TwoInputTransformation[RowData, RowData, RowData](
        leftTransform,
        rightTransform,
        getRelDetailedDescription,
        joinOperator,
        InternalTypeInfo.of(returnType),
        leftTransform.getParallelism)

      if (inputsContainSingleton()) {
        ret.setParallelism(1)
        ret.setMaxParallelism(1)
      }

      // set KeyType and Selector for state
      ret.setStateKeySelectors(leftKeySelector, rightKeySelector)
      ret.setStateKeyType(leftKeySelector.asInstanceOf[ResultTypeQueryable[_]].getProducedType)
      ret

    } else {
      val inputTypeInfo = InternalTypeInfo.of(rightRel.getRowType.asInstanceOf[RowType])
      val rowListTypeInfo = new ListTypeInfo[RowData](inputTypeInfo)

      val temporalBroadcastStateDescriptor =
        new MapStateDescriptor[java.lang.Long, util.List[RowData]](
          "temporalBroadcastState", Types.LONG, rowListTypeInfo)
//    if (leftTransform.isInstanceOf[KeyedStream]) {
//
//    }
      // TODO: 生成 BroadcastStateTransformation
      new BroadcastStateTransformation[RowData, RowData, RowData](
        "test-broadcast",
        leftTransform,
        rightTransform,
        // todo
        null,
        Collections.singletonList(temporalBroadcastStateDescriptor),
        InternalTypeInfo.of(returnType),
        leftTransform.getParallelism)
    }





  }

  private def noBroadcast(inputNode: ExecNode[StreamPlanner, _],
                          planner: StreamPlanner): (Transformation[RowData], Boolean) = {
    (inputNode.translateToPlan(planner).asInstanceOf[Transformation[RowData]], false)
  }

  private def validateKeyTypes(): Unit = {
    // at least one equality expression
    val leftFields = left.getRowType.getFieldList
    val rightFields = right.getRowType.getFieldList

    getJoinInfo.pairs().toList.foreach(pair => {
      val leftKeyType = leftFields.get(pair.source).getType.getSqlTypeName
      val rightKeyType = rightFields.get(pair.target).getType.getSqlTypeName
      // check if keys are compatible
      if (leftKeyType != rightKeyType) {
        throw new TableException(
          "Equality join predicate on incompatible types.\n" +
            s"\tLeft: $left,\n" +
            s"\tRight: $right,\n" +
            s"\tCondition: (${RelExplainUtil.expressionToString(
              getCondition, inputRowType, getExpressionString)})"
        )
      }
    })
  }

  /**
   * Returns a list of this node's input nodes. If there are no inputs,
   * returns an empty list, not null.
   *
   * @return List of this node's input nodes
   */
  override def getInputNodes: util.List[ExecNode[StreamPlanner, _]] = {
    getInputs.map(_.asInstanceOf[ExecNode[StreamPlanner, _]])
  }

  /**
   * Replaces the <code>ordinalInParent</code><sup>th</sup> input.
   * You must override this method if you override [[getInputNodes]].
   *
   * @param ordinalInParent Position of the child input, 0 is the first
   * @param newInputNode    New node that should be put at position ordinalInParent
   */
  override def replaceInputNode(ordinalInParent: Int, newInputNode: ExecNode[StreamPlanner, _]): Unit =
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])

  override def copy(traitSet: RelTraitSet, conditionExpr: RexNode, left: RelNode, right: RelNode, joinType: JoinRelType, semiJoinDone: Boolean): Join = {
    new StreamExecBroadcastTemporalJoin(
      cluster,
      traitSet,
      left,
      right,
      conditionExpr,
      joinType)
  }
}
