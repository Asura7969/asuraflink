
# 背景

之前看到一篇文章,Flink Sql在字节的优化,其中一项是维表join（lookup join）时候通过keyby方式,提高维表查询的命中率。看下自己能不能实现类似功能,总耗时2周(其实上班时间并没有时间研究,就周末和下班时间,大概实际花了4天)

[Flink SQL 在字节跳动的优化与实践](https://segmentfault.com/a/1190000039084980)

先看下实现后的效果图:
![flink-sql-lookupJoinNoKeyBy.png](http://ww1.sinaimg.cn/large/b3b57085gy1gpr4yvibdpj21o407w76t.jpg)

![flink-sql-lookupJoinWithKeyBy.png](http://ww1.sinaimg.cn/large/b3b57085gy1gpr4z7a5dyj21mz089q5h.jpg)


# 实现思路
## Flink sql 整个的执行流程梳理

![PlannerBase.png](http://ww1.sinaimg.cn/large/b3b57085gy1gprqev1t36j20vz0hgdib.jpg)

![1-1.png](http://ww1.sinaimg.cn/large/b3b57085gy1gpr54gtooij20kp0az764.jpg)

## 在哪实现(where)?
实现的地方很多,图1-1 中最引人注意的是 `TEMPORAL_JOIN_REWRITE`, 但后来实现过程中由于对 `calcite API` 不熟悉,实在无奈,只能另辟蹊径了。

最后看到 `PHYSICAL_RWRITE`, 先大致看了下这个规则下**Rule**的实现过程, 主要就是对已生成的 **physical node** 进行重写。

## 怎么实现(how)?
接下来就是依葫芦画瓢了

[KeyByLookupRule 实现](https://github.com/Asura7969/asuraflink/blob/main/asuraflink-sql/src/main/scala/com/asuraflink/sql/rule/KeyByLookupRule.scala)

由于实现过程需要 `temporalTable` 和 `calcOnTemporalTable`, 而 **CommonLookupJoin** 中并没有获取这两个对象的方法,因此只能自己手动添加了
![CommonLookupJoin add method.png](http://ww1.sinaimg.cn/large/b3b57085gy1gpr5a1fx8ej213j0n5grh.jpg)

## 如何校验?
本次校验就仅对 **JdbcLookupTableITCase** 该测试类测试,查看其执行计划
![flink join keyby.png](http://ww1.sinaimg.cn/large/b3b57085gy1gprpv96eutj20u00m70vf.jpg)

结合flink动态添加规则

[AddMyLookupRule](https://github.com/Asura7969/asuraflink/blob/main/asuraflink-sql/src/main/java/com/asuraflink/sql/rule/AddMyLookupRule.java)

# 注意事项
* 本次实践基于flink1.12.0
* 该实现不一定是最优的
* 如有不足,还请大佬指出