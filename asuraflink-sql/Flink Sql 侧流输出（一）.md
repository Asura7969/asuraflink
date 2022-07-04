# Flink Sql SideOutput Stream(一)

> 本章并没有实现完成, 到最后发现 sink 端不好搞，不感兴趣的可以直接看第二篇内容（本章内容中涉及到的代码与第二章大部分一样，只是处理的环节不同）

## 实现原理
![sideOutput(一).png](http://ww1.sinaimg.cn/large/003i2GtDgy1gr69krbftxj60fg0c7my802.jpg)

预想的使用示例如下:

1、定义处理逻辑
```java
public class MyProcessFunction extends ScalarFunction {

    public Atest eval(Integer id1, String id2) {
        Atest a = new Atest();
        a.setId1(id1);
        a.setId2(id2);
        return a;
    }
}

```
2、注册UDF函数
```java
tEnv.createTemporarySystemFunction("SumFunction", SumFunction.class);
```
3、创建sink表
```sql
## sideOutput 输出端
CREATE TABLE sideOutput_table(
    id1 INT,
    id2 VARCHAR
) WITH (
    'connector'='print',
    'functionName'='MyProcessFunction',
    'tagName'='tag1'
)
```
4、查询
```sql
## sideOutput_table 为表名
## SIDE_OUT_PUT 为关键字

SELECT /*+ SIDE_OUT_PUT('sideOutput_table') */ source.id2 FROM source
```

## 一、添加提示信息
```java
public abstract class FlinkHints {

    public static final String HINT_SIDE_OUT_PUT = "SIDE_OUT_PUT";

    public static List<String> getHintedSideOutput(List<RelHint> tableHints) {
        return tableHints.stream()
                .filter(hint -> hint.hintName.equalsIgnoreCase(HINT_SIDE_OUT_PUT))
                .flatMap(hint -> hint.listOptions.stream())
                .collect(Collectors.toList());
    }
}
```
```java
public abstract class FlinkHintStrategies {
    public static HintStrategyTable createHintStrategyTable() {
        return HintStrategyTable.builder()
                // Configure to always throw when we encounter any hint errors
                // (either the non-registered hint or the hint format).
                .errorHandler(Litmus.THROW)
                .hintStrategy(
                        FlinkHints.HINT_NAME_OPTIONS,
                        HintStrategy.builder(HintPredicates.TABLE_SCAN)
                                .optionChecker(
                                        (hint, errorHandler) ->
                                                errorHandler.check(
                                                        hint.kvOptions.size() > 0,
                                                        "Hint [{}] only support non empty key value options",
                                                        hint.hintName))
                                .build())
                // 添加测流校验
                .hintStrategy(
                        FlinkHints.HINT_SIDE_OUT_PUT,
                        HintStrategy.builder(HintPredicates.PROJECT)
                                .optionChecker(
                                        (hint, errorHandler) ->
                                                errorHandler.check(
                                                        hint.listOptions.size() > 0,
                                                        "Hint [{}] only support non empty list",
                                                        hint.hintName))
                                .build())
                .build();
    }
}
```
## 二、SqlToOperationConverter 添加校验
```java
    private PlannerQueryOperation toQueryOperation(FlinkPlannerImpl planner, SqlNode validated) {
        // transform to a relational tree
        RelRoot relational = planner.rel(validated);

        if (!relational.hints.isEmpty()) {
            PlannerQueryOperation queryOperation = new PlannerQueryOperation(
                    relational.project(),
                    relational.hints);
            Catalog catalog = catalogManager.getCatalog(catalogManager.getCurrentCatalog()).get();
            try {
                List<String> allTables = catalog.listTables(catalogManager.getCurrentDatabase());
                if (!allTables.containsAll(queryOperation.getSideOutputHints())) {
                    throw new RuntimeException("must register sideOutput table:"
                            + queryOperation.getSideOutputHints());
                }
                return queryOperation;
            } catch (DatabaseNotExistException e) {
                e.printStackTrace();
            }
        }

        return new PlannerQueryOperation(relational.project(), relational.hints);
    }
```

## 三、修改 PROJECT_TO_CALC
原始实现会忽略 project 中 hint 的提示信息
```scala
package org.apache.flink.table.planner.plan.rules.logical

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.rel.logical.{LogicalCalc, LogicalProject}
import org.apache.calcite.rex.RexProgram

class FlinkProjectToCalcRule extends RelOptRule(
  operand(classOf[LogicalProject], any()),
  "FlinkProjectToCalcRule") {

  override def onMatch(call: RelOptRuleCall): Unit = {
    val project: LogicalProject = call.rel(0)
    val input = project.getInput
    val program = RexProgram.create(
      input.getRowType,
      project.getProjects,
      null,
      project.getRowType,
      project.getCluster.getRexBuilder)

    val calc = if (!project.getHints.isEmpty) {
      LogicalCalc.create(input, program).withHints(project.getHints)
    } else {
      LogicalCalc.create(input, program)
    }
    call.builder()
    call.transformTo(calc)
  }
}

object FlinkProjectToCalcRule {
  val INSTANCE = new FlinkProjectToCalcRule
}
```
`LOGICAL_RULES`中添加规则
```scala
//    CoreRules.PROJECT_TO_CALC,
    FlinkProjectToCalcRule.INSTANCE,
```


## 四、重写 FlinkLogicalCalc 节点
`FlinkLogicalCalc` 实现中并未涉及到 hint信息,故需要修改,
因为涉及到`CommonCalc`的修改, 故同属子类也需要添加 **hint** 信息

```scala
package org.apache.flink.table.planner.plan.nodes.logical

import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.common.CommonCalc

import org.apache.calcite.plan._
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.Calc
import org.apache.calcite.rel.logical.LogicalCalc
import org.apache.calcite.rel.metadata.RelMdCollation
import org.apache.calcite.rel.{RelCollation, RelCollationTraitDef, RelNode}
import org.apache.calcite.rex.RexProgram

import java.util
import java.util.Collections
import java.util.function.Supplier

import org.apache.calcite.rel.hint.RelHint
import org.apache.flink.table.planner.JList

/**
  * Sub-class of [[Calc]] that is a relational expression which computes project expressions
  * and also filters in Flink.
  */
class FlinkLogicalCalc(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    calcProgram: RexProgram,
    hints: JList[RelHint])
  extends CommonCalc(cluster, traitSet, input, calcProgram, hints)
  with FlinkLogicalRel {

  def this(cluster: RelOptCluster,
           traitSet: RelTraitSet,
           input: RelNode,
           calcProgram: RexProgram) {
    this(cluster, traitSet, input, calcProgram, Collections.emptyList[RelHint]())
  }

  override def copy(traitSet: RelTraitSet, child: RelNode, program: RexProgram): Calc = {
    new FlinkLogicalCalc(cluster, traitSet, child, program, this.getHints)
  }

}

private class FlinkLogicalCalcConverter
  extends ConverterRule(
    classOf[LogicalCalc],
    Convention.NONE,
    FlinkConventions.LOGICAL,
    "FlinkLogicalCalcConverter") {

  override def convert(rel: RelNode): RelNode = {
    val calc = rel.asInstanceOf[LogicalCalc]
    val newInput = RelOptRule.convert(calc.getInput, FlinkConventions.LOGICAL)
    FlinkLogicalCalc.create(newInput, calc.getProgram, calc.getHints)
  }
}

object FlinkLogicalCalc {
  val CONVERTER: ConverterRule = new FlinkLogicalCalcConverter()

  def create(input: RelNode, calcProgram: RexProgram): FlinkLogicalCalc = {
    create(input, calcProgram, Collections.emptyList[RelHint]())
  }

  def create(input: RelNode, calcProgram: RexProgram, hints: JList[RelHint]): FlinkLogicalCalc = {
    val cluster = input.getCluster
    val mq = cluster.getMetadataQuery
    val traitSet = cluster.traitSetOf(FlinkConventions.LOGICAL).replaceIfs(
      RelCollationTraitDef.INSTANCE, new Supplier[util.List[RelCollation]]() {
        def get: util.List[RelCollation] = RelMdCollation.calc(mq, input, calcProgram)
      }).simplify()
    new FlinkLogicalCalc(cluster, traitSet, input, calcProgram, hints)
  }
}
```
修改父类`CommonCalc`

```scala

import org.apache.flink.table.planner.JList

abstract class CommonCalc(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    calcProgram: RexProgram,
    hints: JList[RelHint])
  extends Calc(cluster, traitSet, hints, input, calcProgram)
  with FlinkRelNode {

  def this(cluster: RelOptCluster,
           traitSet: RelTraitSet,
           input: RelNode,
           calcProgram: RexProgram) {
    this(cluster, traitSet, input, calcProgram, Collections.emptyList[RelHint]())
  }

  ...
}  
```


## 五、添加 StreamExecSideOutputCalc
参考`StreamExecCalc`
```scala
package org.apache.flink.table.planner.plan.nodes.physical.stream

import java.util
import java.util.{Objects, Optional}

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.Calc
import org.apache.calcite.rel.hint.RelHint
import org.apache.calcite.rex.{RexCall, RexNode, RexProgram}
import org.apache.flink.api.dag.Transformation
import org.apache.flink.streaming.api.transformations.OneInputTransformation
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.catalog.{Catalog, CatalogBaseTable, FunctionLookup, ObjectPath, UnresolvedIdentifier}
import org.apache.flink.table.catalog.exceptions.CatalogException
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.{JList, JMap}
import org.apache.flink.table.planner.calcite.{FlinkContext, FlinkTypeFactory}
import org.apache.flink.table.planner.codegen.{CalcCodeGenerator, CodeGeneratorContext}
import org.apache.flink.table.planner.delegation.StreamPlanner
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction
import org.apache.flink.table.planner.hint.FlinkHints
import org.apache.flink.table.planner.utils.ShortcutUtils
import org.apache.flink.table.runtime.operators.AbstractProcessStreamOperator
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.types.logical.LogicalType

import scala.collection.JavaConverters._

class StreamExecSideOutputCalc(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    calcProgram: RexProgram,
    outputRowType: RelDataType,
    hints: JList[RelHint])
  extends StreamExecCalcBase(cluster, traitSet, inputRel, calcProgram, outputRowType, hints) {

  type ColumnInfo = JList[(Int, String, LogicalType)]
  type TableInfo = JMap[String, ColumnInfo]
  type BSF = BridgingSqlFunction

  val sideOutputTableInfo: TableInfo = new util.HashMap[String, ColumnInfo]()
  val tableAndFunction: JMap[String, BSF] = new util.HashMap[String, BSF]()
  val tableAndRexCall: JMap[String, RexCall] = new util.HashMap[String, RexCall]()
  val tableAndTagName: JMap[String, String] = new util.HashMap[String, String]()

  def toScalaOption[T](op: Optional[T]): Option[T] = {
    if (op.isPresent) {
      Some(op.get())
    } else None
  }

  // 提取一些 side_output table的信息, codegen 时候会用到
  def generatorTableToFieldInfo(): Unit = {
    val context = this.cluster.getPlanner.getContext.unwrap(classOf[FlinkContext])
    val hintsOp = Option(getHints.asScala
      .filter(_.hintName.equals(FlinkHints.HINT_SIDE_OUT_PUT))
      .flatMap(_.listOptions.asScala))

    val flinkTypeFactory = ShortcutUtils.unwrapTypeFactory(cluster)
    val manager = context.getCatalogManager
    val functionCatalog = context.getFunctionCatalog

    val rexBuilder = input.getCluster.getRexBuilder
    val inputRowType = input.getRowType

    toScalaOption[Catalog](manager.getCatalog(manager.getCurrentCatalog)).foreach {
      case catalog: Catalog =>
        hintsOp match {
          case Some(sideOutputTables) =>
            sideOutputTables.foreach(tableName => {
              val tableFullName = s"${manager.getCurrentDatabase}.$tableName"
              val table: CatalogBaseTable = catalog.getTable(ObjectPath.fromString(tableFullName))
              val types: ColumnInfo = new util.ArrayList[(Int, String, LogicalType)]()
              val functionName = validateExits(table.getOptions.get("functionName"))
              val tagName = validateExits(table.getOptions.get("tagName"))

              tableAndTagName.put(tableName, tagName);
              val schema = table.getSchema

              schema.getTableColumns.asScala.zipWithIndex.foreach(t => {
                types.add((t._2, t._1.getName, t._1.getType.getLogicalType))
              })

              sideOutputTableInfo.put(tableName, types)

              val result = functionCatalog.lookupFunction(UnresolvedIdentifier.of(functionName))
              toScalaOption[FunctionLookup.Result](result) match {
                case Some(functionLookup) =>
                  val definition = functionLookup.getFunctionDefinition
                  val function = BridgingSqlFunction.of(
                    context,
                    flinkTypeFactory,
                    functionLookup.getFunctionIdentifier,
                    definition)

                  tableAndFunction.put(tableName, function)

                  val dataType = flinkTypeFactory.buildRelNodeRowType(schema)
                  val operands = new util.ArrayList[RexNode](rexBuilder.identityProjects(dataType))
                  val rexCall = rexBuilder.makeCall(dataType, function, operands)
                  tableAndRexCall.put(tableName, rexCall.asInstanceOf[RexCall])
                case _ =>

              }
            })
          case _ =>
            throw new ValidationException("Only support sideOutput hints ...")
        }
      case _ =>
        throw new CatalogException(s"${manager.getCurrentCatalog} is null!")
    }
  }

  override def copy(traitSet: RelTraitSet, child: RelNode, program: RexProgram): Calc = {
    new StreamExecSideOutputCalc(cluster, traitSet, child, program, outputRowType, getHints)
  }

  private def validateExits[T](v: T): T = {
    if (Objects.isNull(v)) throw new ValidationException(s"value is null: $v")
    else v
  }

  private def validatePrepareInfo(): Unit = {
    if (sideOutputTableInfo.isEmpty || tableAndFunction.isEmpty || tableAndRexCall.isEmpty) {
      throw new ValidationException(s"Must call generatorTableToFieldInfo method!")
    }
  }

  override protected def translateToPlanInternal(
      planner: StreamPlanner): Transformation[RowData] = {
    val config = planner.getTableConfig
    val inputTransform = getInputNodes.get(0).translateToPlan(planner)
      .asInstanceOf[Transformation[RowData]]
    // materialize time attributes in condition
    val condition = if (calcProgram.getCondition != null) {
      Some(calcProgram.expandLocalRef(calcProgram.getCondition))
    } else {
      None
    }

    val ctx = CodeGeneratorContext(config).setOperatorBaseClass(
      classOf[AbstractProcessStreamOperator[RowData]])
    val outputType = FlinkTypeFactory.toLogicalRowType(getRowType)
    val sideOutputCode = if (!getHints.isEmpty) {
      generatorTableToFieldInfo()
      validatePrepareInfo()
      CalcCodeGenerator.generateSideOutputCode(
        ctx,
        inputTransform,
        outputType,
        calcProgram,
        condition,
        retainHeader = true,
        sideOutputTableInfo,
        tableAndFunction,
        tableAndRexCall,
        tableAndTagName
      )
    } else ""

    val substituteStreamOperator = CalcCodeGenerator.generateCalcOperator(
      ctx,
      inputTransform,
      outputType,
      calcProgram,
      condition,
      retainHeader = true,
      sideOutputCode,
      "StreamExecCalc"
    )
    val ret = new OneInputTransformation(
      inputTransform,
      getRelDetailedDescription,
      substituteStreamOperator,
      InternalTypeInfo.of(outputType),
      inputTransform.getParallelism)

    if (inputsContainSingleton()) {
      ret.setParallelism(1)
      ret.setMaxParallelism(1)
    }
    ret
  }
}
```
`FlinkChangelogModeInferenceProgram` 添加 `StreamExecSideOutputCalc`节点的匹配(274行)
```scala
      case _: StreamExecCalc | _: StreamExecSideOutputCalc | _: StreamExecPythonCalc | _: StreamExecCorrelate |
           _: StreamExecPythonCorrelate | _: StreamExecLookupJoin | _: StreamExecExchange |
           _: StreamExecExpand | _: StreamExecMiniBatchAssigner |
           _: StreamExecWatermarkAssigner =>
```


`CalcCodeGenerator` 添加方法 `generateSideOutputCode`
```scala
import org.apache.flink.table.types.logical.{LogicalType, RowType}
import org.apache.flink.table.planner.codegen.SideOutputCodeUtils._
import org.apache.flink.table.planner.{JList, JMap}

import scala.collection.JavaConverters._

object CalcCodeGenerator {

  /**
   * 生成 SideOutput 的代码
   */
  private[flink] def generateSideOutputCode(
      ctx: CodeGeneratorContext,
      inputTransform: Transformation[RowData],
      outputType: RowType,
      calcProgram: RexProgram,
      condition: Option[RexNode],
      retainHeader: Boolean = false,
      sideOutputTableInfo: JMap[String, JList[(Int, String, LogicalType)]],
      tableAndFunction: JMap[String, BridgingSqlFunction],
      tableAndRexCall: JMap[String, RexCall],
      tableAndTagName: JMap[String, String],
      inputTerm: String = CodeGenUtils.DEFAULT_INPUT1_TERM,
      collectorTerm: String = CodeGenUtils.DEFAULT_OPERATOR_COLLECTOR_TERM): String = {

    val inputType = inputTransform.getOutputType
      .asInstanceOf[InternalTypeInfo[RowData]]
      .toRowType

    val tableAndRexCal = tableAndRexCall.asScala.map(t => (t._1, t._2)).toList.zipWithIndex

    val tableMap = addReusable(ctx, tableAndRexCal, tableAndTagName)
    produceProcessCode(ctx, inputType, collectorTerm, inputTerm, tableMap, tableAndRexCal)

  }
  
  private[flink] def generateCalcOperator(
      ctx: CodeGeneratorContext,
      inputTransform: Transformation[RowData],
      outputType: RowType,
      calcProgram: RexProgram,
      condition: Option[RexNode],
      retainHeader: Boolean = false,
      // 添加参数 sideOutputCode
      sideOutputCode: String = "",
      opName: String): CodeGenOperatorFactory[RowData] = {
        ....
    val genOperator =
      OperatorCodeGenerator.generateOneInputStreamOperator[RowData, RowData](
        ctx,
        opName,
        processCode,
        inputType,
        inputTerm = inputTerm,
        // 添加参数 sideOutputCode
        sideOutputCode = sideOutputCode,
        lazyInputUnboxingCode = true)
    ...
  }

}
```
`OperatorCodeGenerator` 添加 **sideOutputCode**
```scala
object OperatorCodeGenerator extends Logging {
  def generateOneInputStreamOperator[IN <: Any, OUT <: Any](
      ctx: CodeGeneratorContext,
      name: String,
      processCode: String,
      inputType: LogicalType,
      inputTerm: String = CodeGenUtils.DEFAULT_INPUT1_TERM,
      endInputCode: Option[String] = None,
      lazyInputUnboxingCode: Boolean = false,
      // 添加参数 sideOutputCode
      sideOutputCode: String = "",
      converter: String => String = a => a): GeneratedOperator[OneInputStreamOperator[IN, OUT]] = {
      
      ...
      @Override
      public void processElement($STREAM_RECORD $ELEMENT) throws Exception {
        $inputTypeTerm $inputTerm = ($inputTypeTerm) ${converter(s"$ELEMENT.getValue()")};
        ${ctx.reusePerRecordCode()}
        ${ctx.reuseLocalVariableCode()}
        ${if (lazyInputUnboxingCode) "" else ctx.reuseInputUnboxingCode()}
        $processCode

        $sideOutputCode
      }
  }
}
```

添加 `SideOutputCodeUtils`
主要用于添加 测流输出的代码
```scala
package org.apache.flink.table.planner.codegen

import java.util
import java.util.Collections

import org.apache.calcite.rex.{RexCall, RexCallBinding}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.functions.UserDefinedFunction
import org.apache.flink.table.planner.JMap
import org.apache.flink.table.planner.codegen.CodeGenUtils.{BINARY_RAW_VALUE, BINARY_STRING, className, typeTerm}
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction
import org.apache.flink.table.planner.functions.inference.OperatorBindingCallContext
import org.apache.flink.table.types.extraction.ExtractionUtils.primitiveToWrapper
import org.apache.flink.table.types.inference.TypeInferenceUtil
import org.apache.flink.table.types.logical.LogicalTypeRoot._
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks.{getFieldCount, getPrecision, getScale}
import org.apache.flink.table.types.logical.{DistinctType, LogicalType, RowType, TimestampKind, TimestampType}
import org.apache.flink.util.{InstantiationUtil, OutputTag}

import scala.collection.JavaConverters._

object SideOutputCodeUtils {

  def addReusable(ctx: CodeGeneratorContext,
                  tableAndRexCall: Seq[((String, RexCall), Int)],
                  tableAndTagName: JMap[String, String])
      : util.HashMap[String, (String, String)] = {

    val tableMap = new util.HashMap[String, (String, String)]()
    tableAndRexCall.foreach {
      t =>
        val tableName = t._1._1
        val returnClass = getFunctionReturnStr(t._1._2, hasSymbol = false)
        val idx = addReferences(ctx, tableName)

        val outputTagClass = className[OutputTag[_]]
        val typeInformation = className[TypeInformation[_]]
        val outputTagFieldTerm = tableAndTagName.get(tableName)
        val newOutputTag =
          s"""
             |private $outputTagClass $outputTagFieldTerm = new $outputTagClass("$outputTagFieldTerm", $typeInformation.of($returnClass.class)){};
             |""".stripMargin

        ctx.addReusableMember(newOutputTag)

        val function = t._1._2.getOperator.asInstanceOf[BridgingSqlFunction]
          .getDefinition.asInstanceOf[UserDefinedFunction]

        val index = t._2
        val functionFieldTerm = s"${CodeGenUtils.udfFieldName(function)}_$index"
        val fieldTypeTerm = function.getClass.getName
        ctx.addReusableMember(
          s"private transient $fieldTypeTerm $functionFieldTerm;")
        // TODO: 自定义udf需支持空参构造，并且不支持 open close 方法
        ctx.addReusableOpenStatement(s"$functionFieldTerm = new $fieldTypeTerm();")

        tableMap.put(tableName, (functionFieldTerm, outputTagFieldTerm))
    }

    ctx.references.clear()
    tableMap
  }

  def produceProcessCode(ctx: CodeGeneratorContext,
                         inputType: RowType,
                         collectorTerm: String,
                         inputTerm: String,
                         tableAndRef: util.HashMap[String, (String, String)],
                         tableAndRexCall: Seq[((String, RexCall), Int)]): String = {

    // 获取 table 的入参 [function.eval(?,?,...)]
    val params = inputType.getFields.asScala.zipWithIndex
      .filter {
        case (field, _) =>
          field.getType match {
            case time:TimestampType =>
              !(time.getKind.ordinal() == TimestampKind.PROCTIME.ordinal())
            case _ => true
          }
      }.map {
      case (field, index) =>
        // CodeGenUtils.rowFieldReadAccess(ctx, index, inputTerm, field.getType)
        rowFieldReadAccess(ctx, index.toString, inputTerm, field.getType)
    }.mkString(", ")

    //    val tableToParams = tableAndRexNode.asScala.map { t =>
    //      val fieldList = t._2.getType.getFieldList.asScala
    //      val params = fieldList.zipWithIndex.map {
    //        case (field, index) =>
    //          val logicalType = FlinkTypeFactory.toLogicalType(field.getType)
    //          CodeGenUtils.rowFieldReadAccess(ctx, index, inputTerm, logicalType)
    //      }.mkString(", ")
    //      (t._1, params)
    //    }.toMap

    tableAndRexCall.map {
      t =>
        val index = t._2
        val tableName = t._1._1
        val returnClass = getFunctionReturnStr(t._1._2, hasSymbol = false)
        val (functionFieldTerm, outputTagFieldTerm) = tableAndRef.get(tableName)
        val streamRecordClass = className[StreamRecord[_]]
        val newStreamRecord = s"new $streamRecordClass<$returnClass>(tmp_result$index)"
        s"""
           |$returnClass tmp_result$index = ($returnClass)$functionFieldTerm.eval($params);
           |if (java.util.Objects.nonNull(tmp_result$index)) {
           |  $collectorTerm.collect($outputTagFieldTerm, $newStreamRecord);
           |}
           |""".stripMargin
    }.mkString("\n")

  }

  def addReferences(ctx: CodeGeneratorContext, obj: Object): Int = {
    val idx = ctx.references.length
    val byteArray = InstantiationUtil.serializeObject(obj)
    val objCopy: AnyRef = InstantiationUtil.deserializeObject(
      byteArray,
      Thread.currentThread().getContextClassLoader)
    ctx.references += objCopy
    idx
  }

  def getFunctionReturnStr(call: RexCall, hasSymbol: Boolean = true): String = {
    call.getOperator match {
      case function: BridgingSqlFunction =>
        val udf = function.getDefinition.asInstanceOf[UserDefinedFunction]

        val inference = function.getTypeInference

        val callContext = new OperatorBindingCallContext(
          function.getDataTypeFactory,
          udf,
          RexCallBinding.create(
            function.getTypeFactory,
            call,
            Collections.emptyList()))

        val adaptedCallContext = TypeInferenceUtil.adaptArguments(
          inference,
          callContext,
          null)
        // val enrichedArgumentDataTypes = toScala(adaptedCallContext.getArgumentDataTypes)

        val enrichedOutputDataType = TypeInferenceUtil.inferOutputType(
          adaptedCallContext,
          inference.getOutputTypeStrategy)

        val externalResultClass = enrichedOutputDataType.getConversionClass
        val externalResultTypeTerm = typeTerm(externalResultClass)
        val externalResultClassBoxed = primitiveToWrapper(externalResultClass)
        val externalResultCasting = if (externalResultClass == externalResultClassBoxed) {
          if (hasSymbol) s"($externalResultTypeTerm)" else externalResultTypeTerm
        } else {
          // TODO: has problem ()
          s"($externalResultTypeTerm) (${typeTerm(externalResultClassBoxed)})"
        }

        externalResultCasting

      case _ =>
        throw new ValidationException(s"$call is not BridgingSqlFunction's instance")
    }

  }

  def rowFieldReadAccess(
        ctx: CodeGeneratorContext,
        indexTerm: String,
        rowTerm: String,
        t: LogicalType)
  : String = t.getTypeRoot match {
    // ordered by type root definition
    case CHAR | VARCHAR =>
      // 源码中也有该实现, 但对于 String 参数并没有调用 toString() 方法
      s"(($BINARY_STRING) $rowTerm.getString($indexTerm)).toString()"
    case BOOLEAN =>
      s"$rowTerm.getBoolean($indexTerm)"
    case BINARY | VARBINARY =>
      s"$rowTerm.getBinary($indexTerm)"
    case DECIMAL =>
      s"$rowTerm.getDecimal($indexTerm, ${getPrecision(t)}, ${getScale(t)})"
    case TINYINT =>
      s"$rowTerm.getByte($indexTerm)"
    case SMALLINT =>
      s"$rowTerm.getShort($indexTerm)"
    case INTEGER | DATE | TIME_WITHOUT_TIME_ZONE | INTERVAL_YEAR_MONTH =>
      s"$rowTerm.getInt($indexTerm)"
    case BIGINT | INTERVAL_DAY_TIME =>
      s"$rowTerm.getLong($indexTerm)"
    case FLOAT =>
      s"$rowTerm.getFloat($indexTerm)"
    case DOUBLE =>
      s"$rowTerm.getDouble($indexTerm)"
    case TIMESTAMP_WITHOUT_TIME_ZONE | TIMESTAMP_WITH_LOCAL_TIME_ZONE =>
      s"$rowTerm.getTimestamp($indexTerm, ${getPrecision(t)})"
    case TIMESTAMP_WITH_TIME_ZONE =>
      throw new UnsupportedOperationException("Unsupported type: " + t)
    case ARRAY =>
      s"$rowTerm.getArray($indexTerm)"
    case MULTISET | MAP  =>
      s"$rowTerm.getMap($indexTerm)"
    case ROW | STRUCTURED_TYPE =>
      s"$rowTerm.getRow($indexTerm, ${getFieldCount(t)})"
    case DISTINCT_TYPE =>
      rowFieldReadAccess(ctx, indexTerm, rowTerm, t.asInstanceOf[DistinctType].getSourceType)
    case RAW =>
      s"(($BINARY_RAW_VALUE) $rowTerm.getRawValue($indexTerm))"
    case NULL | SYMBOL | UNRESOLVED =>
      throw new IllegalArgumentException("Illegal type: " + t)
  }
}

```

注意事项: codegen, 避免使用泛型类
```java
// 运行时会提取不到 OutputTag 的具体类型
OutputTag<Integer> outputTag = new OutputTag<Integer>("a"){};
```

```java
Exception in thread "main" org.apache.flink.api.common.functions.InvalidTypesException: Could not determine TypeInformation for the OutputTag type. The most common reason is forgetting to make the OutputTag an anonymous inner class. It is also not possible to use generic type variables with OutputTags, such as 'Tuple2<A, B>'.
	at org.apache.flink.util.OutputTag.<init>(OutputTag.java:69)
	at com.asuraflink.sideoutput.SideOutPutCase$3$1.<init>(SideOutPutCase.java:21)
	at com.asuraflink.sideoutput.SideOutPutCase$3.<init>(SideOutPutCase.java:21)
	at com.asuraflink.sideoutput.SideOutPutCase.main(SideOutPutCase.java:19)
Caused by: org.apache.flink.api.common.functions.InvalidTypesException: The types of the interface org.apache.flink.util.OutputTag could not be inferred. Support for synthetic interfaces, lambdas, and generic or raw types is limited at this point
	at org.apache.flink.api.java.typeutils.TypeExtractor.getParameterType(TypeExtractor.java:1185)
	at org.apache.flink.api.java.typeutils.TypeExtractor.getParameterTypeFromGenericType(TypeExtractor.java:1209)
	at org.apache.flink.api.java.typeutils.TypeExtractor.getParameterType(TypeExtractor.java:1180)
	at org.apache.flink.api.java.typeutils.TypeExtractor.privateCreateTypeInfo(TypeExtractor.java:733)
	at org.apache.flink.api.java.typeutils.TypeExtractor.createTypeInfo(TypeExtractor.java:713)
	at org.apache.flink.api.java.typeutils.TypeExtractor.createTypeInfo(TypeExtractor.java:706)
	at org.apache.flink.util.OutputTag.<init>(OutputTag.java:66)
	... 3 more
```


应该指定 `OutputTag` 的 `TypeInformation`
```java
OutputTag outputTag = new OutputTag("a", TypeInformation.of(Integer.class)){};
```

## 六、添加 Sink 输出

### 未完待续 ...

# 2000 YEARS LATER ...
![捂脸表情包.jpeg](http://ww1.sinaimg.cn/large/003i2GtDgy1gr6awupeqlj60dw0dwglw02.jpg)
## 续不下去，另辟蹊径了，哈哈哈哈哈😃
