# Flink Sql-Increment Window

**场景描述**：希望能够绘制一天内的 pv/uv 曲线，即在一天内或一个大的窗口内，输出多次结果，而非等窗口结束之后统一输出一次结果。

使用示例如下:

```sql
SELECT 
    INCREMENT_START(ts, INTERVAL '10' SECOND, INTERVAL '1' MINUTE),
    username,
    COUNT(click_url)
FROM 
    user_clicks
GROUP BY 
    INCREMENT(ts, INTERVAL '10' SECOND, INTERVAL '1' MINUTE),username
```

底层逻辑

![increment2.png](http://ww1.sinaimg.cn/large/003i2GtDgy1grd8gviyadj60je08eq3c02.jpg)


## 添加 INCREMENT 窗口集合函数解析规则
### 修改 *flink-sql-parser* 模块
```java
public class FlinkSqlOperatorTable {

    // -----------------------------------------------------------------------------
    // Window SQL functions
    // -----------------------------------------------------------------------------

    /** We need custom group auxiliary functions in order to support nested windows. */
    public static final SqlGroupedWindowFunction INCREMENT_OLD =
            new SqlGroupedWindowFunction(
                    "$INCREMENT",
                    SqlKind.OTHER_FUNCTION,
                    null,
                    OperandTypes.or(
                            OperandTypes.DATETIME_INTERVAL_INTERVAL, OperandTypes.DATETIME_INTERVAL_INTERVAL_TIME)) {
                @Override
                public List<SqlGroupedWindowFunction> getAuxiliaryFunctions() {
                    return Collections.singletonList(INCREMENT_START);
                }
            };

    public static final SqlGroupedWindowFunction INCREMENT_START =
            INCREMENT_OLD.auxiliary("INCREMENT_START", SqlKind.OTHER_FUNCTION);
}
```
![increment1.png](http://ww1.sinaimg.cn/large/003i2GtDgy1grd8h3i7yfj60el0nwjtd02.jpg)

### 修改 Paraser.tdd 文件
```
"org.apache.flink.sql.parser.func.FlinkSqlOperatorTable"


  # List of methods for parsing builtin function calls.
  # Return type of method implementation should be "SqlNode".
  # Example: DateFunctionCall().
  builtinFunctionCallMethods: [
    "GroupByNewWindowingCall()"
  ]
```
> 导入规则信息, GroupByNewWindowingCall 为函数名, 可自定义

### 修改 parserImpls.ftl 文件
```
SqlCall GroupByNewWindowingCall():
{
    final Span s;
    final List<SqlNode> args;
    final SqlOperator op;
}
{
    (
        <INCREMENT>
        {
            s = span();
            op = FlinkSqlOperatorTable.INCREMENT_OLD;
        }
    )
   args = UnquantifiedFunctionParameterList(ExprContext.ACCEPT_SUB_QUERY) {
        return op.createCall(s.end(this), args);
   }
}
```

### 编译
```shell
mvn clean compile
```

### org.apache.flink.table.catalog.BasicOperatorTable
```scala
  BasicOperatorTable.INCREMENT,
  BasicOperatorTable.INCREMENT_START

  val INCREMENT: SqlGroupedWindowFunction = new SqlGroupedWindowFunction(
    "$INCREMENT",
    SqlKind.OTHER_FUNCTION,
    null,
    OperandTypes.or(OperandTypes.DATETIME_INTERVAL_INTERVAL, OperandTypes.DATETIME_INTERVAL_INTERVAL_TIME)) {
    override def getAuxiliaryFunctions: JList[SqlGroupedWindowFunction] =
      Seq(INCREMENT_START)
  }

  val INCREMENT_START: SqlGroupedWindowFunction = new SqlGroupedWindowFunction(
    "INCREMENT_START",
    SqlKind.OTHER_FUNCTION,
    INCREMENT,
    OperandTypes.or(OperandTypes.DATETIME_INTERVAL, OperandTypes.DATETIME_INTERVAL_TIME))
```
### 添加规则信息
> INCREMENT_START、INCREMENT
* org.apache.flink.table.plan.rules.common.LogicalWindowAggregateRule
* org.apache.flink.table.plan.rules.common.WindowPropertiesBaseRule

#### org.apache.flink.table.planner.plan.logical.LogicalWindow
```scala
// ------------------------------------------------------------------------------------------------
// Increment group windows
// ------------------------------------------------------------------------------------------------

case class IncrementGroupWindow(
    alias: PlannerWindowReference,
    timeField: FieldReferenceExpression,
    size: ValueLiteralExpression,
    interval: ValueLiteralExpression)
  extends LogicalWindow(
    alias,
    timeField) {

  override def toString: String = s"IncrementGroupWindow($alias, $timeField, $size, $interval)"
}
```

#### org.apache.flink.table.runtime.operators.window.WindowOperatorBuilder
```java

    public WindowOperatorBuilder withIncrement(Duration interval, long windowSize, TimeDomain timeDomain) {
        checkNotNull(windowAssigner);
        checkArgument(windowAssigner instanceof InternalTimeWindowAssigner);
        InternalTimeWindowAssigner timeWindowAssigner = (InternalTimeWindowAssigner) windowAssigner;
        switch (timeDomain) {
            case EVENT_TIME:
                this.windowAssigner = (WindowAssigner<?>) timeWindowAssigner.withEventTime();
                break;
            case PROCESSING_TIME:
                this.windowAssigner = (WindowAssigner<?>) timeWindowAssigner.withProcessingTime();
                break;
        }
        if (trigger == null) {
            this.trigger = IncrementalTriggers.of(interval.toMillis(), windowSize, timeDomain);
        }
        return this;
    }

    public WindowOperatorBuilder withRowtimeIndex(int rowtimeIndex) {
        this.rowtimeIndex = rowtimeIndex;
        return this;
    }

```

#### org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecGroupWindowAggregateBase
```scala
  override def requireWatermark: Boolean = window match {
    ...
    case IncrementGroupWindow(_, timeField, size, _)
      if isRowtimeAttribute(timeField) && hasTimeIntervalType(size) => true
    ...
  }


  private def createWindowOperator(
      config: TableConfig,
      aggsHandler: GeneratedClass[_],
      recordEqualiser: GeneratedRecordEqualiser,
      accTypes: Array[LogicalType],
      windowPropertyTypes: Array[LogicalType],
      aggValueTypes: Array[LogicalType],
      inputFields: Seq[LogicalType],
      timeIdx: Int): WindowOperator[_, _] = {
    ...
    val newBuilder = window match {
      ...
      case IncrementGroupWindow(_, timeField, size, interval)
        if isProctimeAttribute(timeField) && hasTimeIntervalType(size) =>
        builder.tumble(toDuration(size))
          .withIncrement(toDuration(interval), toDuration(size).toMillis, PROCESSING_TIME)

      case IncrementGroupWindow(_, timeField, size, interval)
        if isRowtimeAttribute(timeField) && hasTimeIntervalType(size) =>
        builder.tumble(toDuration(size))
          .withIncrement(toDuration(interval), toDuration(size).toMillis, EVENT_TIME)
          .withRowtimeIndex(timeIdx)
      ...
    }
    ...
  }
```
#### org.apache.flink.table.planner.plan.rules.logical.LogicalWindowAggregateRuleBase
```scala
      ...
      FlinkSqlOperatorTable.INCREMENT_OLD => true
      ...

      ...
      case FlinkSqlOperatorTable.INCREMENT_OLD =>
        if (call.getOperands.size() == 3) {
          true
        } else {
          throw new TableException("INCREMENT window with alignment is not supported yet.")
        }
      ...


      case FlinkSqlOperatorTable.INCREMENT_OLD =>
        val (interval, size) = (getOperandAsLong(windowExpr, 1), getOperandAsLong(windowExpr, 2))
        IncrementGroupWindow(
          windowRef,
          timeField,
          intervalOfMillis(size),
          intervalOfMillis(interval))

```

### 添加自定义 trigger
> org.apache.flink.table.runtime.operators.window.triggers.IncrementalTriggers

> 此处省略一万字，可参考 org.apache.flink.table.runtime.operators.window.triggers 下的实现类
