package com.asuraflink.sql.dynamic.redis.config;

import java.io.Serializable;

/**
 * @author asura7969
 * @create 2021-03-13-0:17
 */
public class RedisOptions implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String host;
    private final int port;
    private String password;
    private final int database;
    private final String command;
    private final String additionalKey;
    private final int connectionTimeout;
    private final int connectionMaxTotal;
    private final int connectionMaxIdle;
    private final int connectionMinIdle;

    public RedisOptions(String host, int port, String password, int database, String command, String additionalKey,
                        int connectionTimeout, int connectionMaxTotal, int connectionMaxIdle, int connectionMinIdle) {
        this.host = host;
        this.port = port;
        this.password = password;
        this.database = database;
        this.command = command;
        this.additionalKey = additionalKey;
        this.connectionTimeout = connectionTimeout;
        this.connectionMaxTotal = connectionMaxTotal;
        this.connectionMaxIdle = connectionMaxIdle;
        this.connectionMinIdle = connectionMinIdle;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getDatabase() {
        return database;
    }

    public String getPassword() {
        return password;
    }

    public String getCommand() {
        return command;
    }

    public String getAdditionalKey() {
        return additionalKey;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public int getConnectionMaxTotal() {
        return connectionMaxTotal;
    }

    public int getConnectionMaxIdle() {
        return connectionMaxIdle;
    }

    public int getConnectionMinIdle() {
        return connectionMinIdle;
    }

    public static class Builder {
        private String host;
        private int port;
        private String password;
        private int database;
        private String command;
        private String additionalKey;
        private int connectionTimeout;
        private int connectionMaxTotal;
        private int connectionMaxIdle;
        private int connectionMinIdle;

        public Builder setHost(String host) {
            this.host = host;
            return this;
        }

        public Builder setPort(int port) {
            this.port = port;
            return this;
        }

        public Builder setDatabase(int database) {
            this.database = database;
            return this;
        }

        public Builder setCommand(String command) {
            this.command = command;
            return this;
        }

        public Builder setAdditionalKey(String additionalKey) {
            this.additionalKey = additionalKey;
            return this;
        }

        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        public Builder setConnectionTimeout(int connectionTimeout) {
            this.connectionTimeout = connectionTimeout;
            return this;
        }

        public Builder setConnectionMaxTotal(int connectionMaxTotal) {
            this.connectionMaxTotal = connectionMaxTotal;
            return this;
        }

        public Builder setConnectionMaxIdle(int connectionMaxIdle) {
            this.connectionMaxIdle = connectionMaxIdle;
            return this;
        }

        public Builder setConnectionMinIdle(int connectionMinIdle) {
            this.connectionMinIdle = connectionMinIdle;
            return this;
        }

        public RedisOptions build() {
            return new RedisOptions(host, port, password, database, command, additionalKey,
                    connectionTimeout, connectionMaxTotal, connectionMaxIdle, connectionMinIdle);
        }
    }

}
