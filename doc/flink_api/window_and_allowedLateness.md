# Apache Flink的时间类型

* ProcessingTime：数据流入到具体算子某个算子时候相应的系统时间
* IngestionTime：数据进入Flink框架的时间
* EventTime：事件在设备上产生时候携带的

# Watermark

为了处理 EventTime 窗口计算提出的一种机制，本质上也是一种时间戳； Apache Flink 框架保证 Watermark 单调递增，算子接受到一个 Watermark 时候，框架知道不会再有任何小于该 Watermark 的时间戳的数据元素的到来，所以 Watermark 可以看做是告诉 Apache Flink 框架数据流已经处理到什么位置（时间维度）的方式。

## Watermark产生方式
* Punctuated - 数据流中每一个递增的 EventTime 都会产生一个 Watermark。
* Periodic - 周期性的（一定时间间隔或者达到一定的记录数）产生一个 Watermark

```
                            TimestampAssigner
                            m:extractTimestamp()
                                    |
                                    |
                 +------------------+------------------+
                 |                                     |
                 |                                     |
  AssignerWithPeriodicWatermarks       AssignerWithPunctuatedWatermarks
  m:getCurrentWatermark()              m:checkAndGetNextWatermark()
         
```
从接口第一可以看出，Watermark可以在Event(Element)中提取 EventTime，进而定义一定的计算逻辑产生 Watermark 的时间戳。
```java
DataStream<T>.assignTimestampsAndWatermarks(AssignerWithPeriodicWatermarks<T> timestampAndWatermarkAssigner)

DataStream<T>.assignTimestampsAndWatermarks(AssignerWithPunctuatedWatermarks<T> timestampAndWatermarkAssigner)
```
# Window

![Window](http://img3.tbcdn.cn/5476e8b07b923/TB1bwsTJVXXXXaBaXXXXXXXXXXX)

```java
KeyedStream<T, KEY>.window(w extends WindowAssigner)
```
**Time Window**

* Tumbling Time Window
> 统计每一分钟中用户购买的商品的总数
```scala
// Stream of (userId, buyCnt)
val buyCnts: DataStream[(Int, Int)] = ...

val tumblingCnts: DataStream[(Int, Int)] = buyCnts
  // key stream by userId
  .keyBy(0) 
  // tumbling time window of 1 minute length
  .timeWindow(Time.minutes(1))
  // compute sum over buyCnt
  .sum(1)
```
* Sliding Time Window

> 每30秒计算一次最近一分钟用户购买的商品总数   
```scala

val slidingCnts: DataStream[(Int, Int)] = buyCnts
  .keyBy(0) 
  // sliding time window of 1 minute length and 30 secs trigger interval
  .timeWindow(Time.minutes(1), Time.seconds(30))
  .sum(1)
```

**Count Window**

Count Window 是根据元素个数对数据流进行分组的

* Tumbling Count Window
> 我们想要每100个用户购买行为事件统计购买总数，那么每当窗口中填满100个元素了，就会对窗口进行计算
```scala
// Stream of (userId, buyCnts)
val buyCnts: DataStream[(Int, Int)] = ...

val tumblingCnts: DataStream[(Int, Int)] = buyCnts
  // key stream by sensorId
  .keyBy(0)
  // tumbling count window of 100 elements size
  .countWindow(100)
  // compute the buyCnt sum 
  .sum(1)
```

* Sliding Count Window
> 计算每10个元素计算一次最近100个元素的总和
```scala
val slidingCnts: DataStream[(Int, Int)] = vehicleCnts
  .keyBy(0)
  // sliding count window of 100 elements size and 10 elements trigger interval
  .countWindow(100, 10)
  .sum(1)
```

**Session Window**

将事件聚合到会话窗口中（一段用户持续活跃的周期），由非活跃的间隙分隔开
```scala
// Stream of (userId, buyCnts)
val buyCnts: DataStream[(Int, Int)] = ...
  
val sessionCnts: DataStream[(Int, Int)] = vehicleCnts
  .keyBy(0)
  // session window based on a 30 seconds session gap interval 
  .window(ProcessingTimeSessionWindows.withGap(Time.seconds(30)))
  .sum(1)
```

## WindowAssigner
用来决定某个元素被分配到哪个/哪些窗口中去。
## Trigger
触发器。决定了一个窗口何时能够被计算或清除，每个窗口都会拥有一个自己的Trigger。
## Evictor
可以译为“驱逐者”。在Trigger触发之后，在窗口被处理之前，Evictor（如果有Evictor的话）会用来剔除窗口中不需要的元素，相当于一个filter

## Window的实现机制

![Window实现机制](http://img3.tbcdn.cn/5476e8b07b923/TB1swNgKXXXXXc4XpXXXXXXXXXX)

# AllowedLateness
对延迟数据再提供一个宽容的时间
> 水位线表明着早于它的事件不应该再出现,接收到水位线以前的的消息是不可避免的，这就是所谓的迟到事件



    http://wuchong.me/blog/2016/05/25/flink-internals-window-mechanism/
    http://zhuanlan.51cto.com/art/201810/584645.htm
    http://www.whitewood.me/2018/06/01/Flink-Watermark-%E6%9C%BA%E5%88%B6%E6%B5%85%E6%9E%90/