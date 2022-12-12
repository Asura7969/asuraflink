package com.asuraflink.sql.dynamic.redis;

import com.asuraflink.sql.dynamic.redis.config.RedisOptions;
import com.asuraflink.sql.dynamic.redis.config.RedisReadOptions;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.table.api.Expressions.$;
import static org.junit.Assert.assertEquals;

public class RedisDynamicTableFactoryTest {

    public static final String INPUT_TABLE = "redisDynamicTableSource";
    public static final String HSCAN = "HSCAN";
    public static final String HGET = "HGET";

    private Map<String, String> getAllOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "redis");
        options.put("mode", "single");
        options.put("single.host", "localhost");
        options.put("single.port", "6379");
        options.put("db-num", "0");
        options.put("command", "HGET");
        options.put("additional-key", "aaa");
        return options;
    }

    @Test
    public void testRedisReadProperties() {
        Map<String, String> properties = getAllOptions();
        properties.put("scan-count", "2");
        properties.put("match-key", "100");

        RedisOptions options = new RedisOptions.Builder()
                .setHost("localhost")
                .setPort(6379)
                .setDatabase(0)
                .setCommand("HGET")
                .setAdditionalKey("")
                .build();

        RedisReadOptions readOptions = new RedisReadOptions.Builder()
                .setScanCount(2)
                .setMatchKey("*")
                .build();
    }

    @Test
    public void testScanProject() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings envSettings =
                EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, envSettings);

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
                        "  'command' = '" + HSCAN + "',\n" +
                        "  'connection.timeout-ms' = '5000',\n" +
                        "  'additional-key' = 'redis-flink-sql',\n" +
                        "  'match-key' = '1*'\n" +
                        ")"
        );

        Iterator<Row> collected =
                tEnv.executeSql("SELECT cityId,cityName FROM " + INPUT_TABLE)
                        .collect();
        List<String> result =
                CollectionUtil.iteratorToList(collected).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());

        List<String> expected =
                Stream.of(
                        "1,changzhou", "10,jilin", "11,wuxi",
                        "12,jiangyin", "13,anhui", "14,xinjiang", "15,wulumuqi")
                        .sorted()
                        .collect(Collectors.toList());
        assertEquals(expected, result);
    }


    private void useRedisDynamicTableFactory(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        Table t =
                tEnv.fromDataStream(
                        env.fromCollection(
                                Arrays.asList(
                                        new Tuple2<>("1", "1"),
                                        new Tuple2<>("1", "1"),
                                        new Tuple2<>("3", "3"),
                                        new Tuple2<>("5", "5"),
                                        new Tuple2<>("5", "5"),
                                        new Tuple2<>("8", "8"))),
                        $("id1"),
                        $("id2"),
                        $("proctime").proctime());

        tEnv.createTemporaryView("T", t);
    }
    @Test
    public void testLookupProject() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings envSettings =
                EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, envSettings);

        // create T table
        useRedisDynamicTableFactory(env, tEnv);
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
                        "  'additional-key' = 'redis-flink-sql'\n" +
                        ")"
        );

        String sqlQuery =
                "SELECT source.id1, source.id2, L.cityId, L.cityName FROM T AS source "
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
        List<String> expected =
                Stream.of(
                        "1,1,1,changzhou", "1,1,1,changzhou", "3,3,3,shanghai",
                        "5,5,5,shenzhen", "5,5,5,shenzhen", "8,8,8,hefei")
                        .sorted()
                        .collect(Collectors.toList());
        assertEquals(expected, result);
    }

    @Test
    public void testRedisHscan() {
        Jedis jedis = new Jedis("localhost", 6379);
        jedis.select(0);
        ScanParams params = new ScanParams().match("1*").count(10);

        String cursor = ScanParams.SCAN_POINTER_START;
        ScanResult<Map.Entry<String, String>> hscan = null;
        do{
            hscan = jedis.hscan("redis-flink-sql", cursor, params);
            if (!hscan.isCompleteIteration()) {
                cursor = hscan.getCursor();
            }
            hscan.getResult().forEach(e -> {
                System.out.println("key : " + e.getKey() + "  value : " + e.getValue());
            });
        } while (!hscan.isCompleteIteration());
    }


}
