# Flink Sql SideOutput Stream(二)

使用示例如下:

1、定义处理逻辑
```java
    public static class MyProcessFunction extends ScalarFunction {

        @DataTypeHint("ROW<id1 STRING, id2 STRING> NOT NULL")
        public Row eval(String id1, String id2) {
            return Row.of(id1, id2);
        }
    }

```
2、注册UDF函数
```java
tEnv.createTemporarySystemFunction("MyProcessFunction", MyProcessFunction.class);
```
3、创建sink表
```sql
## sideOutput 输出端
CREATE TABLE sideOutput_table(
    `data` Row < id1 INT,
    id2 VARCHAR >
) WITH (
    ...
)
```
4、查询
```sql
## sideOutput_table 为表名
## functionName 为注册的处理函数
## SIDE_OUT_PUT 为关键字

"SELECT /*+ SIDE_OUT_PUT('tableName'='sideOutput_table', 'functionName'='MyProcessFunction') */ T.id2 FROM T"
```

## 一、添加提示信息
```java
public abstract class FlinkHints {
    public static final String HINT_SIDE_OUT_PUT = "SIDE_OUT_PUT";
    
    public static Map<String, String> getHintedSideOutput(List<RelHint> tableHints) {
        return tableHints.stream()
                .filter(hint -> hint.hintName.equalsIgnoreCase(HINT_SIDE_OUT_PUT))
                .findFirst()
                .map(hint -> hint.kvOptions)
                .orElse(Collections.emptyMap());
    }
}
```

```java
public abstract class FlinkHintStrategies {

    /**
     * Customize the {@link HintStrategyTable} which contains hint strategies supported by Flink.
     */
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
                .hintStrategy(
                        FlinkHints.HINT_SIDE_OUT_PUT,
                        HintStrategy.builder(HintPredicates.PROJECT)
                                .optionChecker(
                                        (hint, errorHandler) ->
                                                errorHandler.check(
                                                        hint.kvOptions.size() > 0,
                                                        "Hint [{}] only support non empty key value options",
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
                if (!allTables.contains(queryOperation.getSideOutputHints().get("tableName"))) {
                    throw new RuntimeException("must register sideOutput table:"
                            + queryOperation.getSideOutputHints().get("tableName"));
                }
                return queryOperation;
            } catch (DatabaseNotExistException e) {
                e.printStackTrace();
            }
        }

        return new PlannerQueryOperation(relational.project(), relational.hints);
    }

```

## 三、RelNodeBlock中修改
```scala
import java.util.{Collections, Optional}

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.logical.LogicalProject
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.table.catalog.{CatalogTable, ConnectorCatalogTable, FunctionLookup, ObjectPath, UnresolvedIdentifier}
import org.apache.flink.table.functions.UserDefinedFunction
import org.apache.flink.table.planner.calcite.{FlinkContext, FlinkTypeFactory}
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction
import org.apache.flink.table.planner.functions.inference.OperatorBindingCallContext
import org.apache.flink.table.planner.hint.FlinkHints
import org.apache.flink.table.planner.sinks.{DataStreamTableSink, SelectTableSinkSchemaConverter, StreamSelectTableSink}
import org.apache.flink.table.planner.utils.ShortcutUtils
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.inference.TypeInferenceUtil

import org.apache.calcite.rex.{RexBuilder, RexCall, RexCallBinding, RexNode}
import org.apache.flink.table.planner.plan.nodes.calcite.{LegacySink, LogicalLegacySink}
import org.apache.flink.table.api.{DataTypes, TableConfig, TableSchema}



  def buildRelNodeBlockPlan(
      sinkNodes: Seq[RelNode],
      planner: PlannerBase): Seq[RelNodeBlock] = {
    require(sinkNodes.nonEmpty)

    // 新添加方法, 解析 project中有关side_output 的提示信息, 最后会返回多个RelNode
    val newSinkNodes = expandProject(sinkNodes, planner)

    // expand QueryOperationCatalogViewTable in TableScan
    val shuttle = new ExpandTableScanShuttle
    val convertedRelNodes = newSinkNodes.map(_.accept(shuttle))

    if (convertedRelNodes.size == 1) {
      Seq(new RelNodeBlock(convertedRelNodes.head))
    } else {
      // merge multiple RelNode trees to RelNode dag
      val relNodeDag = reuseRelNodes(convertedRelNodes, config)
      val builder = new RelNodeBlockPlanBuilder(config)
      builder.buildRelNodeBlockPlan(relNodeDag)
    }
  }



  def expandProject(sinkNodes: Seq[RelNode],
                    planner: PlannerBase): Seq[RelNode] = {
    // 提取 提示信息 以及 project
    sinkNodes.map(node => expandHintsOfSideOutput(node, planner))
      .map(t => Seq(t._1).++(t._2)).flatMap(_.toList)

//    sinkNodes.map(expandHintsOfSideOutput)
//      .map(t => Seq().++(t._2)).flatMap(_.toList)
  }

  def expandHintsOfSideOutput(relNode: RelNode, planner: PlannerBase): (RelNode, Seq[RelNode]) = {
    import scala.collection.JavaConverters._

    relNode match {
      case project: LogicalProject =>

        val tableNames = project.getHints.asScala
          .filter(_.hintName.equals(FlinkHints.HINT_SIDE_OUT_PUT))
          .map(_.kvOptions.asScala)
        if (tableNames.nonEmpty) {
          (project.withHints(Collections.emptyList()), createSink(tableNames, project, planner))
        } else (project, Seq.empty)

      case _ =>
        if(!relNode.getInputs.isEmpty) {
          val tuples = relNode.getInputs.asScala.map(node => expandHintsOfSideOutput(node, planner))
          val newRelNode = relNode.copy(relNode.getTraitSet, new util.ArrayList[RelNode](tuples.map(_._1).asJava))
          val extractRelNodes = tuples.map(_._2).flatMap(_.toList)
          (newRelNode, extractRelNodes)
        } else (relNode, Seq.empty)

    }
  }

  def createSink(tables: Seq[mutable.Map[String, String]], project: LogicalProject, planner: PlannerBase): Seq[LogicalLegacySink] = {
    val rexBuilder = project.getCluster.getRexBuilder
    val context = project.getCluster.getPlanner.getContext.unwrap(classOf[FlinkContext])
    val manager = context.getCatalogManager
    val catalog = manager.getCatalog(manager.getCurrentCatalog)
    val functionCatalog = context.getFunctionCatalog
    val flinkTypeFactory = ShortcutUtils.unwrapTypeFactory(project.getCluster)
    val size = project.getProjects.size()

    tables.map { tableMap =>
      val tableName = tableMap("tableName")
      val functionName = tableMap("functionName")
      val tableFullName = s"${manager.getCurrentDatabase}.$tableName"
      val table = catalog.get().getTable(ObjectPath.fromString(tableFullName))
      val functionLookup = functionCatalog.lookupFunction(UnresolvedIdentifier.of(functionName))
      table match {
        case catalogTable: CatalogTable =>
          val schema = table.getSchema
          val sinkSchema = SelectTableSinkSchemaConverter.convertTimeAttributeToRegularTimestamp(
            SelectTableSinkSchemaConverter.changeDefaultConversionClass(schema))

          val op = toOutputTypeInformation(
            context, rexBuilder, schema, flinkTypeFactory, functionLookup, functionName, size)
          op match {
            case Some((typeInfo, rexNode, relDataType)) =>

              val newProjects = new util.ArrayList[RexNode]()
              newProjects.add(rexNode)

              val newProject = project.copy(
                project.getTraitSet,
                project.getInput,
                newProjects,
                project.getInput.getRowType)

              val sink = new StreamSelectTableSink(FlinkTypeFactory.toTableSchema(relDataType))
              // planner.getExecEnv.addProvider(sink.getSelectResultProvider)
              
              /**
               * TODO: 可根据 table信息实现多种 Sink
               *
               * 此处的实现会报错, 没有在 StreamSelectTableSink 中设置 jobClient
               * 可在 StreamExecutionEnvironment 中添加 List，存储 sideOutput 的 sink provider,
               * <code>
               *  public final List<Object> sinkProvider = new ArrayList<>();
               *
               *  public void addProvider(Object provider) {
               *    sinkProvider.add(provider);
               *  }
               * </code>
               * 然后在 TableEnvironmentImpl 中获取 provider
               * <code>
               *   execEnv.getSinkProvide().forEach(provide -> ((SelectResultProvider)provide).setJobClient(jobClient));
               * </code>
               */
              LogicalLegacySink.create(newProject, sink, "collect", ConnectorCatalogTable.sink(sink, false))
          }
      }
    }
  }

  def toOutputTypeInformation(
      context: FlinkContext,
      rexBuilder: RexBuilder,
      schema: TableSchema,
      flinkTypeFactory: FlinkTypeFactory,
      result: Optional[FunctionLookup.Result],
      functionName: String,
      index: Int): Option[(TypeInformation[_], RexNode, RelDataType)] = {
    if (result.isPresent) {
      val functionLookup = result.get()
      val definition = functionLookup.getFunctionDefinition
      val function = BridgingSqlFunction.of(
        context,
        flinkTypeFactory,
        functionLookup.getFunctionIdentifier,
        definition)

      val dataType = flinkTypeFactory.buildRelNodeRowType(schema)
      val operands = new util.ArrayList[RexNode](rexBuilder.identityProjects(dataType))
      val rexCall = rexBuilder.makeCall(dataType, function, operands)

      val udf = function.getDefinition.asInstanceOf[UserDefinedFunction]

      val inference = function.getTypeInference

      val callContext = new OperatorBindingCallContext(
        function.getDataTypeFactory,
        udf,
        RexCallBinding.create(
          function.getTypeFactory,
          rexCall.asInstanceOf[RexCall],
          Collections.emptyList()))

      val adaptedCallContext = TypeInferenceUtil.adaptArguments(
        inference,
        callContext,
        null)

      val functionReturnType: DataType = TypeInferenceUtil.inferOutputType(
        adaptedCallContext,
        inference.getOutputTypeStrategy)

      val relDataType = flinkTypeFactory.createFieldTypeFromLogicalType(
        DataTypes.ROW(DataTypes.FIELD(s"EXPR$index", functionReturnType)).getLogicalType)

      val clazz = TypeInferenceUtil.inferOutputType(
        adaptedCallContext,
        inference.getOutputTypeStrategy).getConversionClass
      Some((TypeExtractor.createTypeInfo(clazz), rexCall, relDataType))

    } else {
      println(s"Not found table: $functionName")
      None
    }
  }

```

## 四、修改 PlannerBase 
```scala
  @VisibleForTesting
  private[flink] def optimize(relNodes: Seq[RelNode]): Seq[RelNode] = {
    val optimizedRelNodes = getOptimizer.optimize(relNodes)
    // 因为optimize会返回多个 relNode, 所以以下断言需注释掉
//    require(optimizedRelNodes.size == relNodes.size)
    optimizedRelNodes
  }

```

![sideoutput(二).png](http://ww1.sinaimg.cn/large/003i2GtDgy1gr6d6xb7f9j61i10cigog02.jpg)

## 注意事项
* 本章内容提供实现思路
* 本章内容没有应用生产实践
* 本章实现属于半成品