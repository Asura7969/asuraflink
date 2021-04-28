# Flink Sql解析

![Flink-sql.png](http://ww1.sinaimg.cn/large/b3b57085gy1gpy52z5xqtj20jl0c5jty.jpg)


## Flink sql执行过程
![flink-sql执行计划.png](http://ww1.sinaimg.cn/large/b3b57085gy1gpy54rdyzgj21120djgmo.jpg)


### SQL -> SqlNode
```java
    @Override
    public List<Operation> parse(String statement) {
        CalciteParser parser = calciteParserSupplier.get();
        FlinkPlannerImpl planner = validatorSupplier.get();
        // parse the sql query
        // 最终由 FlinkSqlParserImpl 执行解析
        SqlNode parsed = parser.parse(statement);
        
        // SqlNode 会封装成 Operation
        Operation operation =
                SqlToOperationConverter.convert(planner, catalogManager, parsed)
                        .orElseThrow(() -> new TableException("Unsupported query: " + statement));
        return Collections.singletonList(operation);
    }
```


```scala
    override def translate(
          modifyOperations: util.List[ModifyOperation]): util.List[Transformation[_]] = {
        if (modifyOperations.isEmpty) {
          return List.empty[Transformation[_]]
        }
        // 将 ModifyOperation 转换为 calcite 关系表达式
        val relNodes = modifyOperations.map(translateToRel)
        // 优化 Plan
        val optimizedRelNodes = optimize(relNodes)
        // 转换为 ExecNode
        val execNodes = translateToExecNodePlan(optimizedRelNodes)
        // 将 ExecNode 转为 transformation
        translateToPlan(execNodes)
    }
```
### Operation -> RelNode
> 代码较多,不粘贴了,详情见 PlannerBase.translateToRel 方法

此方法主要用于将不同的Operation转为 **Calcite** 的 **RelNode**

例如 TableEnvironment 的 `sqlUpdate(sql)` 或 `sqlQuery(sql)` 都将调用到此方法

如果是 **QueryOperation**，则对应 **SelectSinkOperation** 的转换规则

### RelNode(Logical Node) -> Optimized
用到两种优化器对 `Logical Node` 进行优化,**HepPlanner** 和 **VolcanoPlanner**, Flink中应用到到的Rule如下:

> 本章只看 FlinkStreamProgram, 并只截取部分代码,查看完整代码可移步官方源码
```scala
    // VolcanoPlanner FlinkVolcanoProgramBuilder
    // optimize the logical plan
    chainedProgram.addLast(
      LOGICAL,
      FlinkVolcanoProgramBuilder.newBuilder
        .add(FlinkStreamRuleSets.LOGICAL_OPT_RULES)
        .setRequiredOutputTraits(Array(FlinkConventions.LOGICAL))
        .build())
    
    // optimize the physical plan
    chainedProgram.addLast(
      PHYSICAL,
      FlinkVolcanoProgramBuilder.newBuilder
        .add(FlinkStreamRuleSets.PHYSICAL_OPT_RULES)
        .setRequiredOutputTraits(Array(FlinkConventions.STREAM_PHYSICAL))
        .build())

    // HepPlanner, FlinkHepRuleSetProgramBuilder
    // logical rewrite
    chainedProgram.addLast(
      LOGICAL_REWRITE,
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(FlinkStreamRuleSets.LOGICAL_REWRITE)
        .build())
```


### Optimized Node -> PhysicalNode

最后会对优化后的 `Optimize Node` 转换为  `physical Node`, 并对 `physical Node` 再次优化
```scala
    // optimize the physical plan
    chainedProgram.addLast(
      PHYSICAL,
      FlinkVolcanoProgramBuilder.newBuilder
        .add(FlinkStreamRuleSets.PHYSICAL_OPT_RULES)
        .setRequiredOutputTraits(Array(FlinkConventions.STREAM_PHYSICAL))
        .build())
    // physical rewrite
    chainedProgram.addLast(
      PHYSICAL_REWRITE,
      FlinkGroupProgramBuilder.newBuilder[StreamOptimizeContext]
        // add a HEP program for watermark transpose rules to make this optimization deterministic
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
            .add(FlinkStreamRuleSets.WATERMARK_TRANSPOSE_RULES)
            .build(), "watermark transpose")
        .addProgram(new FlinkChangelogModeInferenceProgram,
          "Changelog mode inference")
        .addProgram(new FlinkMiniBatchIntervalTraitInitProgram,
          "Initialization for mini-batch interval inference")
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
            .setHepMatchOrder(HepMatchOrder.TOP_DOWN)
            .add(FlinkStreamRuleSets.MINI_BATCH_RULES)
            .build(), "mini-batch interval rules")
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
            .add(FlinkStreamRuleSets.PHYSICAL_REWRITE)
            .build(), "physical rewrite")
        .build())
```


### PhysicalNode -> ExecNode

```scala
  /**
    * Converts [[FlinkPhysicalRel]] DAG to [[ExecNode]] DAG, and tries to reuse duplicate sub-plans.
    */
  @VisibleForTesting
  private[flink] def translateToExecNodePlan(
      optimizedRelNodes: Seq[RelNode]): util.List[ExecNode[_, _]] = {
    require(optimizedRelNodes.forall(_.isInstanceOf[FlinkPhysicalRel]))
    // Rewrite same rel object to different rel objects
    // in order to get the correct dag (dag reuse is based on object not digest)
    val shuttle = new SameRelObjectShuttle()
    val relsWithoutSameObj = optimizedRelNodes.map(_.accept(shuttle))
    // reuse subplan
    val reusedPlan = SubplanReuser.reuseDuplicatedSubplan(relsWithoutSameObj, config)
    // convert FlinkPhysicalRel DAG to ExecNode DAG
    reusedPlan.map(_.asInstanceOf[ExecNode[_, _]])
  }
```


### ExecNode -> Transformation
最后会调用node的**translateToPlan**方法转换为 `Transformation` 算子
