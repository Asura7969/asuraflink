package com.asuraflink.produce.delay;

import com.alibaba.fastjson.JSONObject;
import io.vertx.core.*;
import io.vertx.redis.client.*;
import io.vertx.redis.client.impl.RedisClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import redis.clients.jedis.Jedis;

import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class OrderAndPaySource {

    private KafkaProducer<String, String> producer;
    private Jedis jedis;
    private String topic;
    private ScheduledExecutorService scheduled;

    public OrderAndPaySource(KafkaProducer<String, String> producer, Jedis jedis, String topic) {
        this.producer = producer;
        this.jedis = jedis;
        this.topic = topic;
        this.scheduled = Executors.newSingleThreadScheduledExecutor();
    }

    public static Map<Integer, String> orderMetaData = new HashMap<>();
    static {
        orderMetaData.put(1,"手表");
        orderMetaData.put(2,"床");
        orderMetaData.put(3,"牙刷");
        orderMetaData.put(4,"书");
        orderMetaData.put(5,"手机");
        orderMetaData.put(6,"电脑");
        orderMetaData.put(7,"衣服");
        orderMetaData.put(8,"鞋子");
        orderMetaData.put(9,"耳机");
        orderMetaData.put(10,"毛巾");
    }

    public void generator(long time) {
        for (int i = 1; i <= 10; i++) {
            sleep(time);
            String productName = orderMetaData.get(i);
            JSONObject orderJson = new JSONObject();
            orderJson.put("user_id", i + "");
            orderJson.put("product_name", productName);
            orderJson.put("amount", i * 100);
            long currTime = System.currentTimeMillis();
            orderJson.put("logTime", currTime);
            orderJson.put("order_id", i + "");

            scheduled.schedule(new ScheduledTask(i), 10, TimeUnit.SECONDS);
            producer.send(new ProducerRecord<>(topic, "", orderJson.toJSONString()));
            System.out.println(orderJson.toJSONString());
            // 重新遍历循环
            if (i == 10) {
                i = 0;
            }
        }
    }

    class ScheduledTask implements Runnable {
        private int payId;

        public ScheduledTask(int payId) {
            this.payId = payId;
        }

        @Override
        public void run() {
            JSONObject payJson = new JSONObject();
            payJson.put("user_id",payId);
            payJson.put("amount", payId * 5);
            payJson.put("order_id",payId);
            payJson.put("pay_id", payId + 1);
            payJson.put("ttl", System.currentTimeMillis());
            jedis.set((payId + 1) + "", payJson.toJSONString());
            jedis.expire((payId + 1) + "", 5);
            System.out.println(payJson.toJSONString());
        }
    }

    public void run() {
        generator(2000);
    }

    private void sleep(long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static KafkaProducer<String, String> createKafkaProduce() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
//        props.put("batch.size", 16384);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    /**
     * 数据产生规则：
     * order：
     *      每 2s 产生一个订单, 每产生一个订单后 10s 产生对应得支付订单
     *      支付id = 订单id + 1
     *
     * 注：支付数据的过期时间设置为 5s
     *
     * @param args
     */
    public static void main(String[] args) throws InterruptedException {

//        Jedis jedis = new Jedis("localhost", 6379);
////        jedis.auth("");
//        jedis.select(0);
//        OrderAndPaySource source = new OrderAndPaySource(createKafkaProduce(), jedis, "order-stream");
////        source.run();



        RedisOptions config = new RedisOptions();
        // redis://[username:password@][host][:port][/[database]
        String connectionString = String.format("redis://%s:%s@%s:%d/%d",
                "","", "localhost",
                6379,0);
        System.out.println(connectionString);
        config.setConnectionString(connectionString);

        VertxOptions vo = new VertxOptions();
        vo.setEventLoopPoolSize(1);
        vo.setWorkerPoolSize(2);

        Vertx vertx = Vertx.vertx(vo);

        RedisClient redisClient = new RedisClient(vertx, config);

        Redis connect = redisClient.send(Request.cmd(Command.GET).arg("1"), new Handler<AsyncResult<Response>>() {
            @Override
            public void handle(AsyncResult<Response> event) {
                System.out.println(event.result().toString());
            }
        });

//                .onComplete(res -> {
//                    if (res.succeeded()) {
//                        System.out.println("成功");
//                        res.result().attributes().forEach((k, v) -> {
//                            System.out.println(k + " : " + v);
//                        });
//                    } else {
//                        throw new RuntimeException(res.cause().getMessage());
//                    }
//                });
//        Thread.sleep(10000);

//        while (!redis.isComplete()) {
//
//        }
//        redisClient.close();
//
//        vertx.close();
    }
}
