package com.asuraflink.sql.dynamic.redis;

import com.asuraflink.sql.dynamic.redis.config.RedisOptions;
import com.asuraflink.sql.dynamic.redis.config.RedisReadOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class RedisDynamicTableFactoryTest {

    public static final String INPUT_TABLE = "redisDynamicTableSource";

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
    public void testProject() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings envSettings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
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
                        "  'command' = 'HSCAN',\n" +
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
