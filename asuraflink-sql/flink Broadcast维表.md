

![flink broadcast维表.png](http://ww1.sinaimg.cn/large/b3b57085gy1gq9mpof6pxj20tw0dk0ua.jpg)


Temporal table function join(LATERAL TemporalTableFunction(o.proctime)) 仅支持 Inner Join,仅支持单一列为主键(primary key)
Temporal table join(FOR SYSTEM_TIME AS OF) 仅支持 Inner Join 和 Left Join, 支持任意列为主键(primary key)