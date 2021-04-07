package com.asuraflink.sql.dynamic.delay;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.table.api.Expressions.$;
import static org.junit.Assert.assertEquals;

/**
 * @author asura7969
 * @create 2021-04-04-9:21
 */
public class DelayDynamicTableFactoryTest {

    public static final String DELAY_JOIN_KEY = "flink-delay-join";
    public static final String INPUT_TABLE = "redisDynamicTableSource";
    public static final String HGET = "HGET";
    public static final String HSCAN = "HSCAN";

    /**
     * 该示例通过自定义source, 持续输出, 看日志输出内容, 后续会完善该示例
     * @throws Exception
     */
    @Test
    public void testDelayLookUpJoin () throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
//        new Thread(this::generateDelayRedisSource, "redisSource").start();
//        Thread.sleep(1000);
        useDynamicTableFactory(env, tEnv);

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
                        + "JOIN " + INPUT_TABLE + " for system_time as of source.proctime AS L "
                        + "ON source.id1 = L.cityId";

        Iterator<Row> collected =
                tEnv.executeSql(sqlQuery)
                        .collect();

        List<String> result =
                CollectionUtil.iteratorToList(collected).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());
        System.out.println(result);

//
//        List<String> expected =
//                Stream.of(
//                        "0-0,0,0", "1-1,1,1", "2-2,2,2",
//                        "3-3,3,3", "4-4,4,4", "5-5,5,5", "6-6,6,6")
//                        .sorted()
//                        .collect(Collectors.toList());
//        assertEquals(expected, result);

    }

    private void useDynamicTableFactory(
            StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        DataStreamSource<Tuple2<String, String>> continueSource = env.addSource(new ContinueSource());
        Table t = tEnv.fromDataStream(continueSource,
                $("id1"),
                $("id2"),
                $("proctime").proctime());

        tEnv.createTemporaryView("T", t);
    }

    // 延迟产生 redis join 维表数据
    private void generateDelayRedisSource () {
        Jedis jedis = new Jedis("localhost", 6379);
        jedis.select(0);
        jedis.del(DELAY_JOIN_KEY);

        AtomicBoolean stop = new AtomicBoolean(false);
        AtomicInteger index = new AtomicInteger(0);

        ScheduledExecutorService scheduledService = Executors.newSingleThreadScheduledExecutor();
        scheduledService.scheduleWithFixedDelay(() -> {
            int andIncrement = index.getAndIncrement();
            if (andIncrement <= 6) {
                jedis.hset(DELAY_JOIN_KEY, andIncrement + "-" + andIncrement, String.valueOf(andIncrement));
                System.out.println("产生 redis 维表数据: " + andIncrement);
            } else {
                stop.compareAndSet(false, true);
            }

        }, 0, 4L, TimeUnit.SECONDS);

        while (!stop.get()) {

        }
        scheduledService.shutdown();

    }

    private class ContinueSource implements SourceFunction<Tuple2<String, String>> {
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
