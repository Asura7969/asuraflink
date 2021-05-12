
## MiniBatchIntervalInferRule


#### 水印产生:
按时间语义主要分为两种`ProcTimeMiniBatchAssignerOperator` 和 `RowTimeMiniBatchAssginerOperator`, 按时间间隔(table.exec.mini-batch.allow-latency)发送watermark给下游,下游依据watermark 批量查询state

#### 攒批处理:
StreamExecGroupAggregate # translateToPlanInternal

若开启minbatch,会构造 `KeyedMapBundleOperator`

如下伪代码:`AbstractMapBundleOperator` 为 `KeyedMapBundleOperator` 父类
```java
AbstractMapBundleOperator
    // 暂存数据buffer
    Map<K, V> bundle;
    // 触发器, 达到数据量触发function.finishBundle, 并重新计数
    BundleTrigger<IN> bundleTrigger;
    // 数据计算逻辑
    MapBundleFunction<K, V, IN, OUT> function;
        addInput(...);
        finishBundle(...);

    public void processElement(StreamRecord<IN> element)
        function.addInput(...)
        // 放入缓存
        bundle.put
        // 计数
        bundleTrigger.onElement

    public void finishBundle()
        // 发送至下游
        function.finishBundle(...)
        // 重置计数器
        bundleTrigger.reset

    public void processWatermark(Watermark mark)
        finishBundle()
```

其中, function（参考:`MiniBatchGroupAggFunction`） 实现类中的聚合变量(genAggsHandler:Accumulate)为 CodeGenerate 动态生成, 其超类为:`AggsHandleFunctionBase` 
