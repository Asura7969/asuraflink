# Mini-Batch 维表 Join

## 背景
主要针对一些 I/O 请求比较高，系统又支持 batch 请求的能力，比如说 RPC、HBase、Redis 等。以往的方式都是逐条的请求，且 Async I/O 只能解决 I/O 延迟的问题,并不能解决访问量的问题。通过实现 Mini-Batch 版本的维表算子，大量降低维表关联访问外部存储次数

## DAG

在 Lookup join 的 operator 前加一个攒批的operator

加规则前
![minibatch-src.png](http://ww1.sinaimg.cn/large/b3b57085gy1gqj5dubohbj21yc082jui.jpg)

加规则后
![minibatch.png](http://ww1.sinaimg.cn/large/b3b57085gy1gqj5ep19pcj225m07cn0f.jpg)

## 实现

### Rule实现
```scala
package org.apache.flink.table.planner.plan.rules.physical.stream

import java.util.Collections

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.physical.stream.{StreamExecBatchLookup, StreamExecCalc, StreamExecLookupJoin, StreamExecTemporalJoin}
import org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil

import scala.collection.JavaConversions._

class StreamExecTemporalBatchJoinRule
  extends RelOptRule(
    operand(classOf[StreamExecLookupJoin],
      operand(classOf[StreamExecCalc], any())),
    "StreamExecTemporalBatchJoinRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val rel = call.rel[StreamExecLookupJoin](0)

    val config = FlinkRelOptUtil.getTableConfigFromContext(rel)
    // 开启优化规则的检验条件
    val batchJoinEnable = config.getConfiguration.getBoolean("temporal.table.exec.mini-batch.enabled", false);
    val miniBatchEnabled = config.getConfiguration.getBoolean(
      ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED)
    miniBatchEnabled && batchJoinEnable
  }

  def getInputs(parent: RelNode): Seq[RelNode] = {
    parent.getInputs.map(_.asInstanceOf[HepRelVertex].getCurrentRel)
  }

  /**
   * pre_Operator  -->  lookup
   * pre_Operator  --> batchNode --> lookup
   * @param call
   */
  override def onMatch(call: RelOptRuleCall): Unit = {
    val rel = call.rel[StreamExecLookupJoin](0)
    val inputNode = rel.getInput
    // 在lookup operator前添加 攒批算子
    val batchNode = new StreamExecBatchLookup(
      inputNode.getCluster,
      inputNode.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL),
      inputNode,
      inputNode.getRowType)

    val newRel = rel.copy(rel.getTraitSet, Collections.singletonList(batchNode))

    call.transformTo(newRel)
  }
}


object StreamExecTemporalBatchJoinRule {
  val INSTANCE: RelOptRule = new StreamExecTemporalBatchJoinRule
}
```

### StreamExecNode 实现
```scala
package org.apache.flink.table.planner.plan.nodes.physical.stream

import java.util

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.{RelNode, SingleRel}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.api.dag.Transformation
import org.apache.flink.streaming.api.transformations.OneInputTransformation
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.delegation.StreamPlanner
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, StreamExecNode}
import org.apache.flink.table.runtime.operators.bundle.MiniBatchElementOperator
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.types.MiniBatchJoinType
import org.apache.flink.table.types.logical.RowType

import scala.collection.JavaConversions._

class StreamExecBatchLookup(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    outputRowType: RelDataType)
  extends SingleRel(cluster, traitSet, inputRel)
  with StreamPhysicalRel with StreamExecNode[RowData] {
  /**
   * Whether the [[StreamPhysicalRel]] requires rowtime watermark in processing logic.
   */
  override def requireWatermark: Boolean = false

  override def getInputNodes: util.List[ExecNode[StreamPlanner, _]] = {
    getInputs.map(_.asInstanceOf[ExecNode[StreamPlanner, _]])
  }

  override def replaceInputNode(ordinalInParent: Int,
                                newInputNode: ExecNode[StreamPlanner, _]): Unit = {
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
  }

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new StreamExecBatchLookup(cluster, traitSet, inputs.get(0), outputRowType)
  }

  override protected def translateToPlanInternal(planner: StreamPlanner): Transformation[RowData] = {

    val input = getInputNodes.get(0).translateToPlan(planner)
      .asInstanceOf[Transformation[RowData]]

    val inRowType = FlinkTypeFactory.toLogicalRowType(getInput.getRowType)
    val outRowType = FlinkTypeFactory.toLogicalRowType(outputRowType)

    val tableConfig = planner.getTableConfig
    // 发送间隔以及batch size设置
    val interval = tableConfig.getConfiguration.getLong(
      "lookup.table.exec.interval.ms", 10000)
    val maxCount = tableConfig.getConfiguration.getLong(
      "lookup.table.exec.mini-batch.size", 1000)

    val operator = new MiniBatchElementOperator(interval, maxCount)

    // MiniBatchJoinType 为 自定义的 MiniBatchOperator的输出类型
    val returnType: InternalTypeInfo[RowData] = InternalTypeInfo.of(
      RowType.of(
        DataTypes.RAW(TypeInformation.of(
          new TypeHint[MiniBatchJoinType] {})).getLogicalType))

    val transformation = new OneInputTransformation(
      input,
      getRelDetailedDescription,
      operator,
      returnType,
      input.getParallelism)

    if (inputsContainSingleton()) {
      transformation.setParallelism(1)
      transformation.setMaxParallelism(1)
    }

    transformation
  }
}
```

### MiniBatch Operator

> 该实现放置位置: flink-table-runtime-blink 模块（可选）

```java
package org.apache.flink.table.runtime.operators.bundle;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRawValueData;
import org.apache.flink.table.types.MiniBatchJoinType;

import java.util.ArrayList;
import java.util.List;


public class MiniBatchElementOperator extends AbstractStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData>, ProcessingTimeCallback {

    private static final long serialVersionUID = 1L;

    private final long intervalMs;

    private final long maxCount;
    
    private transient int numOfElements = 0;

    private transient long latestRegisterTime = 0L;

    private transient List<RowData> bundle;

    public MiniBatchElementOperator(long intervalMs, long maxCount) {
        this.intervalMs = intervalMs;
        this.maxCount = maxCount;
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.bundle = new ArrayList<>();
        long now = getProcessingTimeService().getCurrentProcessingTime();
        latestRegisterTime = now + intervalMs;
        getProcessingTimeService().registerTimer(
                latestRegisterTime, this);

        // report marker metric
        getRuntimeContext()
                .getMetricGroup()
                .gauge("bundleSize", (Gauge<Integer>) () -> numOfElements);
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        numOfElements ++;
        bundle.add(element.getValue());
        if (numOfElements >= maxCount) {
            emit();
            latestRegisterTime = getProcessingTimeService().getCurrentProcessingTime() + intervalMs;
            getProcessingTimeService().registerTimer(latestRegisterTime, this);
        }
    }

    private GenericRowData toGenericRowData(MiniBatchJoinType value) {
        GenericRowData row = new GenericRowData(1);
        row.setField(0, BinaryRawValueData.fromObject(value));
        return row;
    }

    private void emit() {
        if (!bundle.isEmpty()) {
            MiniBatchJoinType outValue = new MiniBatchJoinType();
            outValue.setBatch(new ArrayList<>(bundle));
            output.collect(new StreamRecord<>(toGenericRowData(outValue)));
            bundle.clear();
            numOfElements = 0;
        }
    }

    @Override
    public void onProcessingTime(long timestamp) throws Exception {
        if(latestRegisterTime > timestamp) {
            return;
        }
        emit();
        long now = getProcessingTimeService().getCurrentProcessingTime();
        latestRegisterTime = now + intervalMs;
        getProcessingTimeService().registerTimer(latestRegisterTime, this);
    }
}
```

### MiniBatchJoinType 自定义类型

> 该实现放置位置: flink-table-common 模块（可选）

```java
package org.apache.flink.table.types;

import org.apache.flink.table.data.RowData;

import java.util.List;

public class MiniBatchJoinType {

    public List<RowData> batch;

    public List<RowData> getBatch() {
        return batch;
    }

    public void setBatch(List<RowData> batch) {
        this.batch = batch;
    }
}
```

### CodeGenerator 源码修改

#### LookupJoinCodeGenerator
```scala
  def generateLookupFunction(
      config: TableConfig,
      typeFactory: FlinkTypeFactory,
      inputType: LogicalType,
      returnType: LogicalType,
      tableReturnTypeInfo: TypeInformation[_],
      lookupKeyInOrder: Array[Int],
      // index field position -> lookup key
      allLookupFields: Map[Int, LookupKey],
      lookupFunction: TableFunction[_],
      enableObjectReuse: Boolean)
    : GeneratedFunction[FlatMapFunction[RowData, RowData]] = {

    val ctx = CodeGeneratorContext(config)
    val batchJoinEnabled = config.getConfiguration.getBoolean(
      "temporal.table.exec.mini-batch.enabled", false)

    val logicalType = RowType.of(DataTypes.RAW(TypeInformation.of(new TypeHint[MiniBatchJoinType] {})).getLogicalType)
    val (prepareCode, parameters, nullInParameters) = if (batchJoinEnabled) {
      prepareParameters(
        ctx,
        typeFactory,
        // logicalType 为自定义的batch类型
        logicalType,
        lookupKeyInOrder,
        allLookupFields,
        tableReturnTypeInfo.isInstanceOf[RowTypeInfo],
        enableObjectReuse)
    } else {
      prepareParameters(
        ctx,
        typeFactory,
        inputType,
        lookupKeyInOrder,
        allLookupFields,
        tableReturnTypeInfo.isInstanceOf[RowTypeInfo],
        enableObjectReuse)
    }

    val lookupFunctionTerm = ctx.addReusableFunction(lookupFunction)
    val setCollectorCode = tableReturnTypeInfo match {
      case rt: RowTypeInfo =>
        val converterCollector = new RowToRowDataCollector(rt)
        val term = ctx.addReusableObject(converterCollector, "collector")
        s"""
           |$term.setCollector($DEFAULT_COLLECTOR_TERM);
           |$lookupFunctionTerm.setCollector($term);
         """.stripMargin
      case _ =>
        s"$lookupFunctionTerm.setCollector($DEFAULT_COLLECTOR_TERM);"
    }

    val elseCode = if (batchJoinEnabled) {
      // TableFunction 的实现类额外添加方法,处理batch数据
      s"""$lookupFunctionTerm.batchEval($parameters);"""
    } else {
      s"""$lookupFunctionTerm.eval($parameters);"""
    }

    // TODO: filter all records when there is any nulls on the join key, because
    //  "IS NOT DISTINCT FROM" is not supported yet.
    val body =
      s"""
         |$prepareCode
         |$setCollectorCode
         |if ($nullInParameters) {
         |  return;
         |} else {
         |  $elseCode
         | }
      """.stripMargin

    if (batchJoinEnabled) {
      FunctionCodeGenerator.generateFunction(
        ctx,
        "LookupFunction",
        classOf[FlatMapFunction[RowData, RowData]],
        body,
        returnType,
        // logicalType 为自定义的batch类型
        logicalType)
    } else {
      FunctionCodeGenerator.generateFunction(
        ctx,
        "LookupFunction",
        classOf[FlatMapFunction[RowData, RowData]],
        body,
        returnType,
        inputType)
    }
  }

  private def prepareParameters(
      ctx: CodeGeneratorContext,
      typeFactory: FlinkTypeFactory,
      inputType: LogicalType,
      lookupKeyInOrder: Array[Int],
      allLookupFields: Map[Int, LookupKey],
      isExternalArgs: Boolean,
      fieldCopy: Boolean): (String, String, String) = {
    val batchJoinEnabled = ctx.tableConfig.getConfiguration.getBoolean(
      "temporal.table.exec.mini-batch.enabled", false)
    val inputFieldExprs = if (batchJoinEnabled) {
      val expression = generateInputAccess(
        ctx,
        inputType,
        DEFAULT_INPUT1_TERM,
        // index默认为0
        0,
        nullableInput = false,
        fieldCopy)
      Array(expression)
    } else {
      for (i <- lookupKeyInOrder) yield {
        allLookupFields.get(i) match {
          case Some(ConstantLookupKey(dataType, literal)) =>
            generateLiteral(ctx, dataType, literal.getValue3)
          case Some(FieldRefLookupKey(index)) =>
            generateInputAccess(
              ctx,
              inputType,
              DEFAULT_INPUT1_TERM,
              index,
              nullableInput = false,
              fieldCopy)
          case None =>
            throw new CodeGenException("This should never happen!")
        }
      }
    }
    val codeAndArg = inputFieldExprs
      .map { e =>
        val dataType = fromLogicalTypeToDataType(e.resultType)
        val bType = if (isExternalArgs) {
          typeTerm(dataType.getConversionClass)
        } else {
          boxedTypeTermForType(e.resultType)
        }
        val assign = if (isExternalArgs) {
          CodeGenUtils.genToExternalConverter(ctx, dataType, e.resultTerm)
        } else {
          e.resultTerm
        }
        val newTerm = newName("arg")
        val code =
          s"""
             |$bType $newTerm = null;
             |if (!${e.nullTerm}) {
             |  $newTerm = $assign;
             |}
             """.stripMargin
        (code, newTerm, e.nullTerm)
      }
    (
      codeAndArg.map(_._1).mkString("\n"),
      codeAndArg.map(_._2).mkString(", "),
      codeAndArg.map(_._3).mkString("|| "))
  }
```

### TableFunction 实现类添加batch方法
```java
    public void batchEval(Object keys) {
        BinaryRawValueData<MiniBatchJoinType> binaryRawValueData = (BinaryRawValueData<MiniBatchJoinType>) keys;
        binaryRawValueData.getJavaObject().getBatch().forEach(e -> {
            System.out.println(e.toString());
            Object[] params = new Object[e.getArity()];
            for (int i = 0; i < e.getArity(); i++) {
                // do something
            }
        });
    }
```

### 添加规则
```scala
  val PHYSICAL_REWRITE: RuleSet = RuleSets.ofList(
    //optimize agg rule
    TwoStageOptimizedAggregateRule.INSTANCE,
    // incremental agg rule
    IncrementalAggregateRule.INSTANCE,
    // minibatch join rule
    StreamExecTemporalBatchJoinRule.INSTANCE
  )
```

## 规则开启参数
```properties
temporal.table.exec.mini-batch.enabled = true
table.exec.mini-batch.enabled = true
lookup.table.exec.interval.ms = 5000
lookup.table.exec.mini-batch.size = 1000
```

