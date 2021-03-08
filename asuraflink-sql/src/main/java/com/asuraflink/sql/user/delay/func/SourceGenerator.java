package com.asuraflink.sql.user.delay.func;

import com.asuraflink.sql.user.delay.model.Order;
import com.asuraflink.sql.user.delay.model.Pay;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * 延迟消息
 */
public class SourceGenerator implements Serializable {
    public final LinkedBlockingDeque<Pay> queue = new LinkedBlockingDeque<>();
    public final LinkedBlockingDeque<Order> orderQueue = new LinkedBlockingDeque<>();


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
        for (int i = 0; i < 9; i++) {
            sleep(time);
            Order orderMeta = orderMetaData.get(i);
            long currTime = System.currentTimeMillis();
            Order order = new Order(Long.parseLong(i + ""), orderMeta.getProduct(), i, currTime, orderMeta.getOrderId());
            orderQueue.addLast(order);
        }

    }

    public void generatorPay(long time) {
        for (int i = 0; i < 9; i++) {
            sleep(time);
            Order orderMeta = orderMetaData.get(i);
            long currTime = System.currentTimeMillis();
            long payTime = currTime + 3000;
            Pay payMeta = payMetaData.get(i);
            Pay pay = new Pay(Long.parseLong(i + ""), i * 100, orderMeta.getOrderId(), payMeta.getPayId(), payTime);
            queue.add(pay);
        }
    }

    private void sleep(long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
