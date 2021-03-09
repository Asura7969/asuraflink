package com.asuraflink.sql.user.delay.func;

import com.asuraflink.sql.user.delay.model.Order;
import com.asuraflink.sql.user.delay.model.Pay;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 延迟消息
 */
public class SourceGenerator implements Serializable {
    public final LinkedBlockingQueue<Pay> queue = new LinkedBlockingQueue<>();
    public final LinkedBlockingQueue<Order> orderQueue = new LinkedBlockingQueue<>();


    public static List<Order> orderMetaData = new ArrayList<>();
    static {
        orderMetaData.add(new Order("手表", 1));
        orderMetaData.add(new Order("床", 2));
        orderMetaData.add(new Order("牙刷", 3));
        orderMetaData.add(new Order("书", 4));
        orderMetaData.add(new Order("手机", 5));
        orderMetaData.add(new Order("电脑", 6));
        orderMetaData.add(new Order("衣服", 7));
        orderMetaData.add(new Order("鞋子", 8));
        orderMetaData.add(new Order("耳机", 9));
        orderMetaData.add(new Order("毛巾", 10));
    }

    public static List<Pay> payMetaData = new ArrayList<>();
    static {
        payMetaData.add(new Pay(1));
        payMetaData.add(new Pay(2));
        payMetaData.add(new Pay(3));
        payMetaData.add(new Pay(4));
        payMetaData.add(new Pay(5));
        payMetaData.add(new Pay(6));
        payMetaData.add(new Pay(7));
        payMetaData.add(new Pay(8));
        payMetaData.add(new Pay(9));
        payMetaData.add(new Pay(10));
    }

    private Random r = new Random();

    public void generatorOrder(long time) {
        for (int i = 0; i < 10; i++) {
            sleep(time);
            Order orderMeta = orderMetaData.get(i);
            long currTime = System.currentTimeMillis();
            Order order = new Order(Long.parseLong(i + ""), orderMeta.getProduct(), i, new Timestamp(currTime), orderMeta.getOrderId());
            orderQueue.add(order);
            if (i == 9) {
                i = -1;
            }
        }

    }

    public void generatorPay(long time) {
        for (int i = 0; i < 10; i++) {
            sleep(time);
            Order orderMeta = orderMetaData.get(i);
            long currTime = System.currentTimeMillis();
            Pay payMeta = payMetaData.get(i);
            Pay pay = new Pay(Long.parseLong(i + ""), i * 100, orderMeta.getOrderId(), payMeta.getPayId(), new Timestamp(currTime));
            queue.add(pay);
            if (i == 9) {
                i = -1;
            }
        }
    }

    private void sleep(long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private String generateTimestamp(long time) {
        Date date = new Date(time);
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        df.setTimeZone(new SimpleTimeZone(0, "GMT"));
        return df.format(date);
    }

    public static void main(String[] args) {
        System.out.println(Instant.now().toEpochMilli());
    }
}
