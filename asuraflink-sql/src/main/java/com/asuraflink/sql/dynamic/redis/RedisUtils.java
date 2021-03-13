package com.asuraflink.sql.dynamic.redis;

import com.asuraflink.sql.dynamic.redis.config.RedisOptions;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.calcite.shaded.com.google.common.base.Preconditions;
import redis.clients.jedis.JedisPool;

/**
 * @author asura7969
 * @create 2021-03-13-10:00
 */
public class RedisUtils {

    public static RedisSingle buildJedisPool(RedisOptions redisOptions) {
        Preconditions.checkNotNull(redisOptions, "No options supplied");
        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        poolConfig.setMaxIdle(redisOptions.getConnectionMaxIdle());
        poolConfig.setMinIdle(redisOptions.getConnectionMinIdle());
        poolConfig.setMaxTotal(redisOptions.getConnectionMaxTotal());

        JedisPool jedisPool = new JedisPool(poolConfig, redisOptions.getHost(),
                redisOptions.getPort(), redisOptions.getConnectionTimeout(), redisOptions.getPassword(),
                redisOptions.getDatabase());
        return new RedisSingle(jedisPool);
    }
}
