# Flink sql

flink版本 : 1.12.0

相关组件环境安装：https://github.com/Asura7969/asuraflink/tree/main/doc/docker

## Flink 扩展功能

### Flink 自定义动态表(Redis)

#### 描述
```sql
CREATE TABLE redis_table(
    column_name1 VARCHAR,
    column_name2 VARCHAR,
    ...
) WITH (
    'connector' = 'redis',
    'mode' = 'single',
    'single.host' = 'localhost',
    'single.port' = '6379',
    'db-num' = '0',
    'command' = 'HSCAN',
    'additional-key' = 'test_hash_key',
    'match-key' = 'keys*',
)

```
实现**LookupTableSource** 与 **ScanTableSource**

[RedisDynamicTableFactory](https://github.com/Asura7969/asuraflink/tree/main/asuraflink-sql/src/main/java/com/asuraflink/sql/dynamic/redis)

#### 底层逻辑调用实现

![lookup.png](http://ww1.sinaimg.cn/large/b3b57085gy1gojtjlvkvmj20pv0h3q3v.jpg)

