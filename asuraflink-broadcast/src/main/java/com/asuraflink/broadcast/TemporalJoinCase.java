package com.asuraflink.broadcast;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class TemporalJoinCase extends MyDerby {

    public static void main(String[] args) throws Exception {
        before();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);


        Table order = tEnv.fromDataStream(env.addSource(new KeyByBroadcastStream.ContinueSource()),
                $("id"),
                $("productId"),
                $("name"),
                $("proctime").proctime());

        tEnv.createTemporaryView("order_view", order);


        String cacheConfig = ", 'lookup.cache.max-rows'='4', 'lookup.cache.ttl'='10000'";
        tEnv.executeSql(
                String.format(
                        "create table lookup ("
                                + "  id INT,"
                                + "  name VARCHAR"
                                + ") with("
                                + "  'connector'='jdbc',"
                                + "  'url'='" + DB_URL + "',"
                                + "  'table-name'='" + LOOKUP_TABLE + "',"
                                + "  'lookup.max-retries' = '0'"
                                + "  %s)",
                        ""));
//        CloseableIterator<Row> collect = tEnv.executeSql("select * from lookup").collect();
//        List<String> result =
//                CollectionUtil.iteratorToList(collect).stream()
//                        .map(Row::toString)
//                        .sorted()
//                        .collect(Collectors.toList());
//        System.out.println(result);
        String sqlQuery =
                "SELECT order_view.id, order_view.name as order_name, L.name as product_name FROM order_view "
                        + "JOIN lookup for system_time as of order_view.proctime AS L "
                        + "ON order_view.productId = L.id";


        tEnv.toAppendStream(tEnv.sqlQuery(sqlQuery), Result.class);

        System.out.println(env.getExecutionPlan());
//        tEnv.executeSql(sqlQuery).collect();

        after();

    }

    public static class Result {
        public int id;
        public String order_name;
        public String product_name;
    }
}
