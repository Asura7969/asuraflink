#### 描述

![flink延迟join.png](http://ww1.sinaimg.cn/large/b3b57085gy1gpbjvs162ej20wq0glgni.jpg)

#### 实现方式
* 1、继承 TableFunction
* 2、实现 DelayAttributes 接口
    * default DelayStrategy getDelayStrategy()
      获取延迟策略
    * default boolean collectIsNotNull (Object rightOutput)
      判断查询的外部资源是否为null, 如果为 null, 则不调用 collect 方法输出到下游
* 3、重写 LookupJoinRunner 方法
* 4、由于不是 keyBy 的stream，不能使用flink内置的TimeService, 因此参考 **SystemProcessingTimeService** 实现了 **MyTimeService**, 用于延迟join
```java
class MyFunction extends TableFunction<Integer> implements DelayAttributes{
   public void eval(Integer leftKey) {

     // 通过leftKey 查询其他外部资源的数据
     Row right = connect.join(leftKey);

     if (collectIsNotNull(right)) {

         // 判断不为null 才最终调用 TableFunction.collect 方法
         collect(Row.of(leftKey, right));
     }
   }
}
```
[重写LookupJoinRunner](https://github.com/Asura7969/asuraflink/blob/main/asuraflink-sql/src/main/java/org/apache/flink/table/runtime/operators/join/lookup/LookupJoinRunner.java)

[DelayDynamicTableFactoryTest.testDelayLookUpJoin 测试类](https://github.com/Asura7969/asuraflink/blob/main/asuraflink-sql/src/test/java/com/asuraflink/sql/dynamic/delay/DelayDynamicTableFactoryTest.java)

[示例](https://github.com/Asura7969/asuraflink/blob/main/asuraflink-sql/src/main/java/com/asuraflink/sql/dynamic/redis/RedisRowDataLookupFunction.java)
##### 注意事项
* 只支持 processTime（暂未实现 eventTime）
* 基于状态的恢复,默认会把之前的未完成的join重新join
* 测试类并不完善(基于日志内容查看是否完全实现延迟join)