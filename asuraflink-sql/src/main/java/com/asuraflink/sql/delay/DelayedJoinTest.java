package com.asuraflink.sql.delay;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author asura7969
 * @create 2021-04-10-17:30
 */
public class DelayedJoinTest {

    public static final String DELAY_JOIN_KEY = "flink-delay-join";
    public static final String INPUT_TABLE = "redisDynamicTableSource";
    public static final String HGET = "HGET";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStreamSource<Tuple2<String, String>> continueSource = env.addSource(new ContinueSource());
        Table t = tEnv.fromDataStream(continueSource,
                $("id1"),
                $("id2"),
                $("proctime").proctime());

        tEnv.createTemporaryView("T", t);

        tEnv.executeSql(
                "CREATE TABLE " + INPUT_TABLE + " (\n" +
                        "  cityId STRING,\n" +
                        "  cityName STRING\n" +
                        ") WITH (\n" +
                        "  'connector' = 'redis',\n" +
                        "  'mode' = 'single',\n" +
                        "  'single.host' = 'localhost',\n" +
                        "  'single.port' = '6379',\n" +
                        "  'db-num' = '0',\n" +
                        "  'command' = '" + HGET + "',\n" +
                        "  'connection.timeout-ms' = '5000',\n" +
                        "  'additional-key' = '"+ DELAY_JOIN_KEY +"'" +
//                        "  'match-key' = '*-*'\n" +
                        ")"
        );

        String sqlQuery =
                "SELECT source.id1, source.id2, L.cityId FROM T AS source "
                        + "LEFT JOIN " + INPUT_TABLE + " for system_time as of source.proctime AS L "
                        + "ON source.id1 = L.cityId";
        tEnv.executeSql(sqlQuery).print();

//        Iterator<Row> collected =
//                tEnv.executeSql(sqlQuery)
//                        .collect();
//
//        List<String> result =
//                CollectionUtil.iteratorToList(collected).stream()
//                        .map(Row::toString)
//                        .sorted()
//                        .collect(Collectors.toList());
//        System.out.println(result);

//        tEnv.execute("");
    }

    public static class ContinueSource implements SourceFunction<Tuple2<String, String>> {
        private volatile boolean isRunning = true;
        private int count = 0;
        @Override
        public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
            while (isRunning && count < Integer.MAX_VALUE) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(new Tuple2<>(count + "-" + count, "" + count));
                    count++;
                    Thread.sleep(2000);
                }
            }

        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
