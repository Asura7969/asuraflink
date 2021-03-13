package com.asuraflink.sql.dynamic.redis;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

@Slf4j
public class RedisSingle {
    private JedisPool jedisPool;

    public RedisSingle(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    private Jedis getInstance() {
        return jedisPool.getResource();
    }

    private void releaseInstance(final Jedis jedis) {
        if (jedis == null) {
            return;
        }
        try {
            jedis.close();
        } catch (Exception e) {
            log.error("Failed to close (return) instance to pool", e);
        }
    }

    public void open() throws Exception {
        getInstance().echo("Test");
    }

    public String get(final String key) {
        Jedis jedis = null;
        try {
            jedis = getInstance();
            return jedis.get(key);
        } catch (Exception e) {
            log.error("Cannot receive Redis message with command HGET to key {} error message {}",
                    key, e.getMessage());
            throw e;
        } finally {
            releaseInstance(jedis);
        }
    }

    public String hget(final String key, final String hashField) {
        Jedis jedis = null;
        try {
            jedis = getInstance();
            return jedis.hget(key, hashField);
        } catch (Exception e) {
            log.error("Cannot receive Redis message with command HGET to key {} and hashField {} error message {}", key, hashField, e.getMessage());
            throw e;
        } finally {
            releaseInstance(jedis);
        }
    }

    public void hset(final String key, final String hashField, final String value) {
        Jedis jedis = null;
        try {
            jedis = getInstance();
            jedis.hset(key, hashField, value);
        } catch (Exception e) {
            log.error("Cannot send Redis message with command HSET to key {} and hashField {} error message {}", key, hashField, e.getMessage());
            throw e;
        } finally {
            releaseInstance(jedis);
        }
    }

    public void rpush(final String listName, final String value) {
        Jedis jedis = null;
        try {
            jedis = getInstance();
            jedis.rpush(listName, value);
        } catch (Exception e) {
            log.error("Cannot send Redis message with command RPUSH to list {} error message {}", listName, e.getMessage());
            throw e;
        } finally {
            releaseInstance(jedis);
        }
    }

    public void lpush(String listName, String value) {
        Jedis jedis = null;
        try {
            jedis = getInstance();
            jedis.lpush(listName, value);
        } catch (Exception e) {
            log.error("Cannot send Redis message with command LUSH to list {} error message {}", listName, e.getMessage());
            throw e;
        } finally {
            releaseInstance(jedis);
        }
    }

    public void sadd(final String setName, final String value) {
        Jedis jedis = null;
        try {
            jedis = getInstance();
            jedis.sadd(setName, value);
        } catch (Exception e) {
            log.error("Cannot send Redis message with command RPUSH to set {} error message {}", setName, e.getMessage());
            throw e;
        } finally {
            releaseInstance(jedis);
        }
    }

    public void publish(final String channelName, final String message) {
        Jedis jedis = null;
        try {
            jedis = getInstance();
            jedis.publish(channelName, message);
        } catch (Exception e) {
            log.error("Cannot send Redis message with command PUBLISH to channel {} error message {}", channelName, e.getMessage());
            throw e;
        } finally {
            releaseInstance(jedis);
        }
    }

    public void set(final String key, final String value) {
        Jedis jedis = null;
        try {
            jedis = getInstance();
            jedis.set(key, value);
        } catch (Exception e) {
            log.error("Cannot send Redis message with command SET to key {} error message {}", key, e.getMessage());
            throw e;
        } finally {
            releaseInstance(jedis);
        }
    }

    public void pfadd(final String key, final String element) {
        Jedis jedis = null;
        try {
            jedis = getInstance();
            jedis.pfadd(key, element);
        } catch (Exception e) {
            log.error("Cannot send Redis message with command PFADD to key {} error message {}", key, e.getMessage());
            throw e;
        } finally {
            releaseInstance(jedis);
        }
    }

    public void zadd(final String key, final String score, final String element) {
        Jedis jedis = null;
        try {
            jedis = getInstance();
            jedis.zadd(key, Double.valueOf(score), element);
        } catch (Exception e) {
            log.error("Cannot send Redis message with command ZADD to set {} error message {}", key, e.getMessage());
            throw e;
        } finally {
            releaseInstance(jedis);
        }
    }

    public void zrem(final String key, final String element) {
        Jedis jedis = null;
        try {
            jedis = getInstance();
            jedis.zrem(key, element);
        } catch (Exception e) {
            log.error("Cannot send Redis message with command ZREM to set {} error message {}", key, e.getMessage());
            throw e;
        } finally {
            releaseInstance(jedis);
        }
    }

    public ScanResult<String> scan(String cursor, ScanParams scanParams) {
        Jedis jedis = null;
        try {
            jedis = getInstance();
            return jedis.scan(cursor, scanParams);
        } catch (Exception e) {
            log.error("Cannot send Redis message with command scan error message {}", e.getMessage());
            throw e;
        } finally {
            releaseInstance(jedis);
        }
    }

    public ScanResult<Map.Entry<String, String>> hscan(String additionalKey, String cursor, ScanParams scanParams) {
        Jedis jedis = null;
        try {
            jedis = getInstance();
            return jedis.hscan(additionalKey, cursor, scanParams);
        } catch (Exception e) {
            log.error("Cannot send Redis message with command hscan error message {}", e.getMessage());
            throw e;
        } finally {
            releaseInstance(jedis);
        }
    }

    public void close() throws IOException {
        if (this.jedisPool != null) {
            this.jedisPool.close();
        }
    }
}
