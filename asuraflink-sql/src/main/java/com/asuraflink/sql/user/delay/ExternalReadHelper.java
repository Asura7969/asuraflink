package com.asuraflink.sql.user.delay;

import com.alibaba.fastjson.JSON;
import com.asuraflink.sql.user.delay.func.ReadHelper;
import com.asuraflink.sql.user.delay.model.Pay;
import redis.clients.jedis.Jedis;

public class ExternalReadHelper extends ReadHelper<Long, Pay> {

    private Jedis jedis;

    @Override
    public void open() {
        jedis = new Jedis("localhost", 6379);
        System.out.println("Open external resource!");
    }

    @Override
    public void close() {
        jedis.close();
        System.out.println("Close external resource!");
    }

    @Override
    public Pay eval(Long orderId) {
        String s = jedis.get(orderId.toString());
        if (null == s) {
            return null;
        }
        return JSON.parseObject(s, Pay.class);
    }

    @Override
    public Pay eval(Long... values) {
        return null;
    }
}
