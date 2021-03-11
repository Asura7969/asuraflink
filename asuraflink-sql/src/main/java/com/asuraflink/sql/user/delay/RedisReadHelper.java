package com.asuraflink.sql.user.delay;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.asuraflink.sql.user.delay.func.ReadHelper;
import com.asuraflink.sql.user.delay.model.Pay;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import redis.clients.jedis.Jedis;

import java.util.Map;

public class RedisReadHelper extends ReadHelper<String, MapData> {

    private transient Jedis jedis;

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
    public MapData eval(String orderId) {
        String value = jedis.get(orderId);
        if (null == value) {
            return null;
        }
        Map map = JSONObject.parseObject(value).toJavaObject(Map.class);
        return new GenericMapData(map);
    }

    @Override
    public MapData eval(String... values) {
        return null;
    }
}
