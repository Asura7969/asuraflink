package com.asuraflink.sql.dynamic.redis.test;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class RedisSourceTest {

    public static Map<String, String> cityInfo = new HashMap<>();
    static {
        cityInfo.put("1", "漳州市");
        cityInfo.put("2", "常德市");
        cityInfo.put("3", "桂林市");
        cityInfo.put("4", "九江市");
        cityInfo.put("5", "惠州市");
        cityInfo.put("6", "芜湖市");
        cityInfo.put("7", "无锡市");
        cityInfo.put("8", "常州市");
    }

    public static void main(String[] args) throws Exception {

        source();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings envSettings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, envSettings);


        String redisDDL =
                "CREATE TABLE redis_test_city_info (\n" +
                "  cityId STRING,\n" +
                "  cityName STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'redis',\n" +
                "  'mode' = 'single',\n" +
                "  'single.host' = 'localhost',\n" +
                "  'single.port' = '6379',\n" +
                "  'db-num' = '0',\n" +
                "  'command' = 'HGET',\n" +
                "  'connection.timeout-ms' = '5000',\n" +
                "  'connection.test-while-idle' = 'true',\n" +
                "  'additional-key' = 'rtdw_dim:test_city_info'\n" +
//                "  'lookup.cache.max-rows' = '1000',\n" +
//                "  'lookup.cache.ttl-sec' = '600'\n" +
                ")";

        tEnv.sqlUpdate(redisDDL);

        String printSink =
                "CREATE TABLE print_redis_test_dim_join (\n" +
                "  tss TIMESTAMP(3),\n" +
                "  cityId BIGINT,\n" +
                "  cityName STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ")";


        tEnv.executeSql(printSink);


        String kafka =
                "CREATE TABLE kafka_order_done_log (\n" +
                        "    cityId BIGINT,\n" +
                        "    orderType BIGINT,\n" +
                        "    logTime BIGINT,\n" +  // 13位时间戳
                        "    procTime AS PROCTIME(),\n" +  // 13位时间戳
                        "    tss AS TO_TIMESTAMP(FROM_UNIXTIME(logTime / 1000, 'yyyy-MM-dd HH:mm:ss'))\n" +
                        // 注：简化逻辑,不使用 watermark
                        // 在ts上定义5 秒延迟的 watermark
//                "    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND" +
                        ") WITH (\n" +
                        "    'connector.type' = 'kafka',\n" +
                        "    'connector.version' = 'universal',\n" +
                        "    'connector.topic' = 'city_metadata',\n" +
                        "    'connector.startup-mode' = 'latest-offset',\n" +
                        "    'connector.properties.0.key' = 'zookeeper.connect',\n" +
                        "    'connector.properties.0.value' = 'localhost:2181',\n" +
                        "    'connector.properties.1.key' = 'bootstrap.servers',\n" +
                        "    'connector.properties.1.value' = 'localhost:9092',\n" +
                        "    'connector.properties.2.key' = 'group.id',\n" +
                        "    'connector.properties.2.value' = 'testGroup',\n" +
                        "    'update-mode' = 'append',\n" +
                        "    'format.type' = 'json',\n" +
                        "    'format.derive-schema' = 'true'\n" +
                        ")";
        tEnv.sqlUpdate(kafka);

        String query =
                "INSERT INTO print_redis_test_dim_join\n" +
                "SELECT a.tss, a.cityId, b.cityName\n" +
                "FROM kafka_order_done_log AS a\n" +
                "LEFT JOIN redis_test_city_info FOR SYSTEM_TIME AS OF a.procTime AS b ON CAST(a.cityId AS STRING) = b.cityId\n" +
                "WHERE a.orderType = 12";

        tEnv.executeSql(query).print();
//        tEnv.executeSql("select * from kafka_order_done_log").print();

//        tEnv.executeSql("select * from print_redis_test_dim_join").print();

        tEnv.execute("redis source");
    }

    public static void source() {
        new Thread(() -> {
            Jedis jedis = new Jedis("localhost", 6379);
            cityInfo.forEach((k,v) -> {
                jedis.hset("rtdw_dim:test_city_info", k, v);
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
        }).start();

        new Thread(() -> {
            Properties props = new Properties();
            props.put("bootstrap.servers", "localhost:9092");
            props.put("acks", "all");
            props.put("retries", 0);
//        props.put("batch.size", 16384);
            props.put("key.serializer", StringSerializer.class.getName());
            props.put("value.serializer", StringSerializer.class.getName());
            KafkaProducer<String, String> producer = new KafkaProducer<>(props);

            for (int i = 1; i < 9; i++) {

                JSONObject json = new JSONObject();
                json.put("cityId",i);
                json.put("orderType",12);
                json.put("logTime",System.currentTimeMillis());

                producer.send(new ProducerRecord<String, String>("city_metadata", "", json.toJSONString()));
                if(i == 8) {
                    i = 0;
                }
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        }).start();
    }

    class City {
        public String cityId;
        public String cityName;

        public String getCityId() {
            return cityId;
        }

        public void setCityId(String cityId) {
            this.cityId = cityId;
        }

        public String getCityName() {
            return cityName;
        }

        public void setCityName(String cityName) {
            this.cityName = cityName;
        }
    }
}
