package com.asuraflink.sql.dynamic.redis.config;

import java.io.Serializable;

/**
 * @author asura7969
 * @create 2021-03-13-0:21
 */
public class RedisReadOptions implements Serializable {

    private final int scanCount;
    private final String matchKey;

    public RedisReadOptions(int scanCount, String matchKey) {
        this.scanCount = scanCount;
        this.matchKey = matchKey;
    }

    public int getScanCount() {
        return scanCount;
    }

    public String getMatchKey() {
        return matchKey;
    }

    public static class Builder {
        private int scanCount;
        private String matchKey;

        public Builder setScanCount(int scanCount) {
            this.scanCount = scanCount;
            return this;
        }

        public Builder setMatchKey(String matchKey) {
            this.matchKey = matchKey;
            return this;
        }

        public RedisReadOptions build() {
            return new RedisReadOptions(scanCount, matchKey);
        }
    }
}
