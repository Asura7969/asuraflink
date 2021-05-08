

![flink broadcast维表.png](http://ww1.sinaimg.cn/large/b3b57085gy1gq9mpof6pxj20tw0dk0ua.jpg)


Temporal table function join(LATERAL TemporalTableFunction(o.proctime)) 仅支持 Inner Join,仅支持单一列为主键(primary key)
Temporal table join(FOR SYSTEM_TIME AS OF) 仅支持 Inner Join 和 Left Join, 支持任意列为主键(primary key)




## StreamExecTemporalJoinRule
```scala
package org.apache.flink.table.planner.plan.rules.physical.stream

import java.util

import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical._
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecTemporalJoin
import org.apache.flink.table.planner.plan.utils.{FlinkRelOptUtil, IntervalJoinUtil, TemporalJoinUtil}
import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.JoinRelType
import org.apache.flink.util.Preconditions.checkState

/**
 * Rule that matches a temporal join node and converts it to [[StreamExecTemporalJoin]],
 * the temporal join node is a [[FlinkLogicalJoin]] which contains [[TemporalJoinCondition]].
 */
class StreamExecTemporalJoinRule
  extends RelOptRule(
    operand(classOf[FlinkLogicalJoin],
      operand(classOf[FlinkLogicalRel], any()),
      operand(classOf[FlinkLogicalRel], any())),
    "StreamExecTemporalJoinRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join = call.rel[FlinkLogicalJoin](0)
    if (!TemporalJoinUtil.containsTemporalJoinCondition(join.getCondition)) {
      return false
    }

    //INITIAL_TEMPORAL_JOIN_CONDITION should not appear in physical phase.
    checkState(!TemporalJoinUtil.containsInitialTemporalJoinCondition(join.getCondition))

    matchesTemporalTableJoin(join) || matchesTemporalTableFunctionJoin(join)
  }

  private def matchesTemporalTableJoin(join: FlinkLogicalJoin): Boolean = {
    val supportedJoinTypes = Seq(JoinRelType.INNER, JoinRelType.LEFT)
    supportedJoinTypes.contains(join.getJoinType)
  }

  private def matchesTemporalTableFunctionJoin(join: FlinkLogicalJoin): Boolean = {
    val joinInfo = join.analyzeCondition
    val tableConfig = FlinkRelOptUtil.getTableConfigFromContext(join)
    val (windowBounds, _) = IntervalJoinUtil.extractWindowBoundsFromPredicate(
      joinInfo.getRemaining(join.getCluster.getRexBuilder),
      join.getLeft.getRowType.getFieldCount,
      join.getRowType,
      join.getCluster.getRexBuilder,
      tableConfig)
    windowBounds.isEmpty && join.getJoinType == JoinRelType.INNER
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val join = call.rel[FlinkLogicalJoin](0)
    val left = call.rel[FlinkLogicalRel](1)
    val right = call.rel[FlinkLogicalRel](2)

    val newRight = right match {
      case snapshot: FlinkLogicalSnapshot =>
        snapshot.getInput
      case rel: FlinkLogicalRel => rel
    }

    def toHashTraitByColumns(
        columns: util.Collection[_ <: Number],
        inputTraitSets: RelTraitSet, isLeft: Boolean) = {
      val distribution = if (columns.size() == 0) {
        FlinkRelDistribution.SINGLETON
      }
      else {
        FlinkRelDistribution.hash(columns)
      }
//      else if (isLeft) {
//        FlinkRelDistribution.hash(columns)
//      } else {
//        FlinkRelDistribution.BROADCAST_DISTRIBUTED
//      }
      inputTraitSets.
        replace(FlinkConventions.STREAM_PHYSICAL).
        replace(distribution)
    }

    val joinInfo = join.analyzeCondition
    val (leftRequiredTrait, rightRequiredTrait) = (
      toHashTraitByColumns(joinInfo.leftKeys, left.getTraitSet, isLeft = true),
      toHashTraitByColumns(joinInfo.rightKeys, newRight.getTraitSet, isLeft = false))

    val convLeft: RelNode = RelOptRule.convert(left, leftRequiredTrait)
    val convRight: RelNode = RelOptRule.convert(newRight, rightRequiredTrait)
    val providedTraitSet: RelTraitSet = join.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)

    val temporalJoin = new StreamExecTemporalJoin(
      join.getCluster,
      providedTraitSet,
      convLeft,
      convRight,
      join.getCondition,
      join.getJoinType)

    call.transformTo(temporalJoin)
  }
}

object StreamExecTemporalJoinRule {
  val INSTANCE: RelOptRule = new StreamExecTemporalJoinRule
}
```

### StreamExecTemporalJoin
```scala
package org.apache.flink.table.planner.plan.nodes.physical.stream

import org.apache.flink.api.dag.Transformation
import org.apache.flink.api.java.typeutils.{ListTypeInfo, ResultTypeQueryable}
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator
import org.apache.flink.streaming.api.transformations.{BroadcastStateTransformation, TwoInputTransformation}
import org.apache.flink.table.api.{TableConfig, TableException, ValidationException}
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, ExprCodeGenerator, FunctionCodeGenerator}
import org.apache.flink.table.planner.delegation.StreamPlanner
import org.apache.flink.table.planner.plan.nodes.common.CommonPhysicalJoin
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, StreamExecNode}
import org.apache.flink.table.planner.plan.utils.TemporalJoinUtil.{TEMPORAL_JOIN_CONDITION, TEMPORAL_JOIN_CONDITION_PRIMARY_KEY}
import org.apache.flink.table.planner.plan.utils.{KeySelectorUtil, RelExplainUtil, TemporalJoinUtil}
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector
import org.apache.flink.table.runtime.operators.join.temporal.{TemporalBroadcastProcessFunction, TemporalProcessTimeJoinOperator, TemporalRowTimeJoinOperator}
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.util.Preconditions.checkState
import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.{Join, JoinInfo, JoinRelType}
import org.apache.calcite.rex._
import java.util
import java.util.Collections

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistribution

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
 * Stream physical node for temporal table join (FOR SYSTEM_TIME AS OF) and
 * temporal TableFunction join (LATERAL TemporalTableFunction(o.proctime)).
 *
 * <p>The legacy temporal table function join is the subset of temporal table join,
 * the only difference is the validation, we reuse most same logic here.
 */
class StreamExecTemporalJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    leftRel: RelNode,
    rightRel: RelNode,
    condition: RexNode,
    joinType: JoinRelType)
  extends CommonPhysicalJoin(cluster, traitSet, leftRel, rightRel, condition, joinType)
  with StreamPhysicalRel
  with StreamExecNode[RowData] {

  override def requireWatermark: Boolean = {
    TemporalJoinUtil.isRowTimeJoin(cluster.getRexBuilder, getJoinInfo)
  }

  override def copy(
      traitSet: RelTraitSet,
      conditionExpr: RexNode,
      left: RelNode,
      right: RelNode,
      joinType: JoinRelType,
      semiJoinDone: Boolean): Join = {
    new StreamExecTemporalJoin(
      cluster,
      traitSet,
      left,
      right,
      conditionExpr,
      joinType)
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getInputNodes: util.List[ExecNode[StreamPlanner, _]] = {
    getInputs.map(_.asInstanceOf[ExecNode[StreamPlanner, _]])
  }

  override def replaceInputNode(
    ordinalInParent: Int,
    newInputNode: ExecNode[StreamPlanner, _]): Unit = {
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
  }

  override protected def translateToPlanInternal(
    planner: StreamPlanner): Transformation[RowData] = {

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

    val joinOperator = joinTranslator.getJoinOperator(joinType, returnType.getFieldNames)
    val leftKeySelector = joinTranslator.getLeftKeySelector
    val rightKeySelector = joinTranslator.getRightKeySelector

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
      val rightInputType = FlinkTypeFactory.toLogicalRowType(rightRel.getRowType)
      val inputTypeInfo = InternalTypeInfo.of(rightInputType)
      val rowListTypeInfo = new ListTypeInfo[RowData](inputTypeInfo)

      val temporalBroadcastStateDescriptor =
        new MapStateDescriptor[java.lang.Long, util.List[RowData]](
          "temporalBroadcastState", Types.LONG, rowListTypeInfo)

      new BroadcastStateTransformation[RowData, RowData, RowData](
        "test-broadcast",
        leftTransform,
        new PartitionTransformation(rightTransform, new BroadcastPartitioner[RowData]),
        joinTranslator.getBroadcastFunction(joinType, temporalBroadcastStateDescriptor),
        Collections.singletonList(temporalBroadcastStateDescriptor),
        InternalTypeInfo.of(returnType),
        leftTransform.getParallelism)
    }
//    if (leftTransform.isInstanceOf[KeyedStream]) {
//
//    }
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
}

/**
  * @param rightRowTimeAttributeInputReference is defined only for event time joins.
  */
class StreamExecTemporalJoinToCoProcessTranslator private(
    textualRepresentation: String,
    config: TableConfig,
    returnType: RowType,
    leftInputType: RowType,
    rightInputType: RowType,
    joinInfo: JoinInfo,
    rexBuilder: RexBuilder,
    leftTimeAttributeInputReference: Int,
    rightRowTimeAttributeInputReference: Option[Int],
    remainingNonEquiJoinPredicates: RexNode,
    isTemporalFunctionJoin: Boolean) {

  val nonEquiJoinPredicates: Option[RexNode] = Some(remainingNonEquiJoinPredicates)

  def getLeftKeySelector: RowDataKeySelector = {
    KeySelectorUtil.getRowDataSelector(
      joinInfo.leftKeys.toIntArray,
      InternalTypeInfo.of(leftInputType)
    )
  }

  def getRightKeySelector: RowDataKeySelector = {
    KeySelectorUtil.getRowDataSelector(
      joinInfo.rightKeys.toIntArray,
      InternalTypeInfo.of(rightInputType)
    )
  }

  def getJoinOperator(
    joinType: JoinRelType,
    returnFieldNames: Seq[String]): TwoInputStreamOperator[RowData, RowData, RowData] = {

    // input must not be nullable, because the runtime join function will make sure
    // the code-generated function won't process null inputs
    val ctx = CodeGeneratorContext(config)
    val exprGenerator = new ExprCodeGenerator(ctx, nullableInput = false)
      .bindInput(leftInputType)
      .bindSecondInput(rightInputType)

    val body = if (nonEquiJoinPredicates.isEmpty) {
      // only equality condition
      "return true;"
    } else {
      val condition = exprGenerator.generateExpression(nonEquiJoinPredicates.get)
      s"""
         |${condition.code}
         |return ${condition.resultTerm};
         |""".stripMargin
    }

    val generatedJoinCondition = FunctionCodeGenerator.generateJoinCondition(
      ctx,
      "ConditionFunction",
      body)

    createJoinOperator(config, joinType, generatedJoinCondition)
  }

  def getBroadcastFunction(
    joinType: JoinRelType,
    mapStateDescriptor: MapStateDescriptor[java.lang.Long, util.List[RowData]])
  : BroadcastProcessFunction[RowData, RowData, RowData] = {
    val ctx = CodeGeneratorContext(config)
    val exprGenerator = new ExprCodeGenerator(ctx, nullableInput = false)
      .bindInput(leftInputType)
      .bindSecondInput(rightInputType)

    val body = if (nonEquiJoinPredicates.isEmpty) {
      // only equality condition
      "return true;"
    } else {
      val condition = exprGenerator.generateExpression(nonEquiJoinPredicates.get)
      s"""
         |${condition.code}
         |return ${condition.resultTerm};
         |""".stripMargin
    }

    val generatedJoinCondition = FunctionCodeGenerator.generateJoinCondition(
      ctx,
      "ConditionFunction",
      body)

    createJoinBroadcastFunc(config, joinType, generatedJoinCondition, mapStateDescriptor)

  }

  protected def createJoinBroadcastFunc(
    tableConfig: TableConfig,
    joinType: JoinRelType,
    generatedJoinCondition: GeneratedJoinCondition,
    mapStateDescriptor: MapStateDescriptor[java.lang.Long, util.List[RowData]])
  : BroadcastProcessFunction[RowData, RowData, RowData] = {
    if (isTemporalFunctionJoin) {
      if (joinType != JoinRelType.INNER) {
        throw new ValidationException(
          "Temporal table function join currently only support INNER JOIN, " +
            "but was " + joinType.toString + " JOIN.")
      }
    } else {
      if (joinType != JoinRelType.LEFT && joinType != JoinRelType.INNER) {
        throw new TableException(
          "Temporal table join currently only support INNER JOIN and LEFT JOIN, " +
            "but was " + joinType.toString + " JOIN.")
      }
    }

    val isLeftOuterJoin = joinType == JoinRelType.LEFT
    val minRetentionTime = tableConfig.getMinIdleStateRetentionTime
    val maxRetentionTime = tableConfig.getMaxIdleStateRetentionTime
    new TemporalBroadcastProcessFunction(
      generatedJoinCondition,
      InternalTypeInfo.of(rightInputType),
      mapStateDescriptor,
      isLeftOuterJoin
    )

  }

  protected def createJoinOperator(
    tableConfig: TableConfig,
    joinType: JoinRelType,
    generatedJoinCondition: GeneratedJoinCondition)
  : TwoInputStreamOperator[RowData, RowData, RowData] = {

    if (isTemporalFunctionJoin) {
      if (joinType != JoinRelType.INNER) {
        throw new ValidationException(
          "Temporal table function join currently only support INNER JOIN, " +
            "but was " + joinType.toString + " JOIN.")
      }
    } else {
      if (joinType != JoinRelType.LEFT && joinType != JoinRelType.INNER) {
        throw new TableException(
          "Temporal table join currently only support INNER JOIN and LEFT JOIN, " +
            "but was " + joinType.toString + " JOIN.")
      }
    }

    val isLeftOuterJoin = joinType == JoinRelType.LEFT
    val minRetentionTime = tableConfig.getMinIdleStateRetentionTime
    val maxRetentionTime = tableConfig.getMaxIdleStateRetentionTime
    if (rightRowTimeAttributeInputReference.isDefined) {
      new TemporalRowTimeJoinOperator(
        InternalTypeInfo.of(leftInputType),
        InternalTypeInfo.of(rightInputType),
        generatedJoinCondition,
        leftTimeAttributeInputReference,
        rightRowTimeAttributeInputReference.get,
        minRetentionTime,
        maxRetentionTime,
        isLeftOuterJoin)
    } else {
      if (isTemporalFunctionJoin) {
        new TemporalProcessTimeJoinOperator(
          InternalTypeInfo.of(rightInputType),
          generatedJoinCondition,
          minRetentionTime,
          maxRetentionTime,
          isLeftOuterJoin)
      } else {
        // The exsiting TemporalProcessTimeJoinOperator has already supported temporal table join.
        // However, the semantic of this implementation is problematic, because the join processing
        // for left stream doesn't wait for the complete snapshot of temporal table, this may
        // mislead users in production environment. See FLINK-19830 for more details.
        throw new TableException("Processing-time temporal join is not supported yet.")
      }
    }
  }
}

object StreamExecTemporalJoinToCoProcessTranslator {
  def create(
    textualRepresentation: String,
    config: TableConfig,
    returnType: RowType,
    leftInput: RelNode,
    rightInput: RelNode,
    joinInfo: JoinInfo,
    rexBuilder: RexBuilder): StreamExecTemporalJoinToCoProcessTranslator = {

    val leftType = FlinkTypeFactory.toLogicalRowType(leftInput.getRowType)
    val rightType = FlinkTypeFactory.toLogicalRowType(rightInput.getRowType)
    val isTemporalFunctionJoin = TemporalJoinUtil.isTemporalFunctionJoin(rexBuilder, joinInfo)

    checkState(
      !joinInfo.isEqui,
      "Missing %s in temporal join condition",
      TEMPORAL_JOIN_CONDITION)

    val temporalJoinConditionExtractor = new TemporalJoinConditionExtractor(
      textualRepresentation,
      leftType.getFieldCount,
      joinInfo,
      rexBuilder,
      isTemporalFunctionJoin)

    val nonEquiJoinRex: RexNode = joinInfo.getRemaining(rexBuilder)
    val remainingNonEquiJoinPredicates = temporalJoinConditionExtractor.apply(nonEquiJoinRex)

    val (leftTimeAttributeInputRef, rightRowTimeAttributeInputRef: Option[Int]) =
      if (TemporalJoinUtil.isRowTimeJoin(rexBuilder, joinInfo)) {
        checkState(temporalJoinConditionExtractor.leftTimeAttribute.isDefined &&
          temporalJoinConditionExtractor.rightPrimaryKey.isDefined,
          "Missing %s in Event-Time temporal join condition", TEMPORAL_JOIN_CONDITION)

        val leftTimeAttributeInputRef = TemporalJoinUtil.extractInputRef(
          temporalJoinConditionExtractor.leftTimeAttribute.get, textualRepresentation)
        val rightTimeAttributeInputRef = TemporalJoinUtil.extractInputRef(
          temporalJoinConditionExtractor.rightTimeAttribute.get, textualRepresentation)
        val rightInputRef = rightTimeAttributeInputRef - leftType.getFieldCount

        (leftTimeAttributeInputRef, Some(rightInputRef))
      } else {
        val leftTimeAttributeInputRef = TemporalJoinUtil.extractInputRef(
          temporalJoinConditionExtractor.leftTimeAttribute.get, textualRepresentation)
        // right time attribute defined in temporal join condition iff in Event time join
        (leftTimeAttributeInputRef, None)
      }

    new StreamExecTemporalJoinToCoProcessTranslator(
      textualRepresentation,
      config,
      returnType,
      leftType,
      rightType,
      joinInfo,
      rexBuilder,
      leftTimeAttributeInputRef,
      rightRowTimeAttributeInputRef,
      remainingNonEquiJoinPredicates,
      isTemporalFunctionJoin)
  }

  private class TemporalJoinConditionExtractor(
    textualRepresentation: String,
    rightKeysStartingOffset: Int,
    joinInfo: JoinInfo,
    rexBuilder: RexBuilder,
    isTemporalFunctionJoin: Boolean) extends RexShuttle {

    var leftTimeAttribute: Option[RexNode] = None

    var rightTimeAttribute: Option[RexNode] = None

    var rightPrimaryKey: Option[Array[RexNode]] = None

    override def visitCall(call: RexCall): RexNode = {
      if (call.getOperator != TEMPORAL_JOIN_CONDITION) {
        return super.visitCall(call)
      }

      // at most one temporal function in a temporal join node
      if (isTemporalFunctionJoin) {
        checkState(
          leftTimeAttribute.isEmpty
            && rightPrimaryKey.isEmpty
            && rightTimeAttribute.isEmpty,
          "Multiple %s temporal functions in [%s]",
          TEMPORAL_JOIN_CONDITION,
          textualRepresentation)
      }

      if (TemporalJoinUtil.isRowTimeTemporalTableJoinCon(call) ||
        TemporalJoinUtil.isRowTimeTemporalFunctionJoinCon(call)) {
        leftTimeAttribute = Some(call.getOperands.get(0))
        rightTimeAttribute = Some(call.getOperands.get(1))
        rightPrimaryKey = Some(extractPrimaryKeyArray(call.getOperands.get(2)))
      } else {
        leftTimeAttribute = Some(call.getOperands.get(0))
        rightPrimaryKey = Some(extractPrimaryKeyArray(call.getOperands.get(1)))
      }

      // the condition of temporal function comes from WHERE clause,
      // so it's not been validated in logical plan
      if (isTemporalFunctionJoin) {
        TemporalJoinUtil.validateTemporalFunctionCondition(
          call,
          leftTimeAttribute.get,
          rightTimeAttribute,
          rightPrimaryKey,
          rightKeysStartingOffset,
          joinInfo,
          "Temporal Table Function")
      }

      rexBuilder.makeLiteral(true)
    }

    private def extractPrimaryKeyArray(rightPrimaryKey: RexNode): Array[RexNode] = {
      if (!rightPrimaryKey.isInstanceOf[RexCall] ||
        rightPrimaryKey.asInstanceOf[RexCall].getOperator != TEMPORAL_JOIN_CONDITION_PRIMARY_KEY) {
        throw new ValidationException(
          s"No primary key [${rightPrimaryKey.asInstanceOf[RexCall]}] " +
            s"defined in versioned table of Event-time temporal table join")
       }
      rightPrimaryKey.asInstanceOf[RexCall].getOperands.asScala.toArray
     }
  }
}
```


### add TemporalBroadcastProcessFunction (org.apache.flink.table.runtime.operators.join.temporal.BaseTwoInputStreamOperatorWithStateRetention)
```java
package org.apache.flink.table.runtime.operators.join.temporal;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class TemporalBroadcastProcessFunction extends BroadcastProcessFunction<RowData, RowData, RowData> {

    private final GeneratedJoinCondition generatedJoinCondition;
    private final InternalTypeInfo<RowData> rightType;
    private final MapStateDescriptor<Long, List<RowData>> temporalBroadcastStateDescriptor;
    private final boolean isLeftOuterJoin;

    private transient JoinCondition joinCondition;
    private transient JoinedRowData outRow;
    private transient GenericRowData rightNullRow;

    public TemporalBroadcastProcessFunction(
            GeneratedJoinCondition generatedJoinCondition,
            InternalTypeInfo<RowData> rightType,
            MapStateDescriptor<Long, List<RowData>> temporalBroadcastStateDescriptor,
            boolean isLeftOuterJoin) {
        this.generatedJoinCondition = generatedJoinCondition;
        this.rightType = rightType;
        this.temporalBroadcastStateDescriptor = temporalBroadcastStateDescriptor;
        this.isLeftOuterJoin = isLeftOuterJoin;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.joinCondition =
                generatedJoinCondition.newInstance(getRuntimeContext().getUserCodeClassLoader());
        this.outRow = new JoinedRowData();
        this.rightNullRow = new GenericRowData(rightType.toRowSize());
    }

    @Override
    public void processElement(
            RowData leftSideRow,
            ReadOnlyContext ctx,
            Collector<RowData> out) throws Exception {

        ReadOnlyBroadcastState<Long, List<RowData>> broadcastState = ctx.getBroadcastState(
                temporalBroadcastStateDescriptor);
        long currentProcessingTime = ctx.currentProcessingTime();
        List<RowData> temporalRows = broadcastState.get(currentProcessingTime);
        boolean joinSuccess = false;
        if (Objects.nonNull(temporalRows)) {
            for (RowData rightSideRow : temporalRows) {
                if (joinCondition.apply(leftSideRow, rightSideRow)) {
                    joinSuccess = true;
                    collectJoinedRow(leftSideRow, rightSideRow, out);
                }
            }
            if (!joinSuccess && isLeftOuterJoin) {
                collectJoinedRow(leftSideRow, rightNullRow, out);
            }
        } else {
            if (isLeftOuterJoin) {
                collectJoinedRow(leftSideRow, rightNullRow, out);
            }
        }

    }

    @Override
    public void processBroadcastElement(
            RowData value,
            Context ctx,
            Collector<RowData> out) throws Exception {
        long currentProcessingTime = ctx.currentProcessingTime();
        BroadcastState<Long, List<RowData>> broadcastState = ctx.getBroadcastState(
                temporalBroadcastStateDescriptor);
        List<RowData> rowDataList = broadcastState.get(currentProcessingTime);
        if (Objects.isNull(rowDataList)) {
            rowDataList = new ArrayList<>();
        }
        rowDataList.add(value);
        broadcastState.put(currentProcessingTime, rowDataList);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    private void collectJoinedRow(RowData leftRow, RowData rightRow, Collector<RowData> collector) {
        outRow.setRowKind(leftRow.getRowKind());
        outRow.replace(leftRow, rightRow);
        collector.collect(outRow);
    }

}

```


TemporalJoinITCase.testEventTimeTemporalJoin
```scala
val sqlQuery = " SELECT o.order_id, o.currency, o.amount, o.order_time, r.rate, r.currency_time " +
      " FROM orders_rowtime AS o JOIN versioned_currency_with_single_key " +
      " FOR SYSTEM_TIME AS OF o.order_time as r " +
      " ON o.currency = r.currency"

    tEnv.toRetractStream[Row](tEnv.sqlQuery(sqlQuery))
    println(env.getExecutionPlan)
```