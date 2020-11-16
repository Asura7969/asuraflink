package com.asuraflink.common.async.config;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDBCDimensionSourceConfig extends DimensionSourceConfig{

    private static final Logger LOGGER = LoggerFactory.getLogger(JDBCDimensionSourceConfig.class);

    private String url;
    private String userName;
    private String password;
    private String driverClass;


    private int maxPoolSize = 20;

    public JDBCDimensionSourceConfig(@NotNull String url,
                                     @NotNull String userName,
                                     @NotNull String password,
                                     @NotNull String driverClass) {
        this.url = url;
        this.userName = userName;
        this.password = password;
        this.driverClass = driverClass;
    }

    public String getUrl() {
        return url;
    }

    public JDBCDimensionSourceConfig setUrl(String url) {
        this.url = url;
        return this;
    }

    public String getUserName() {
        return userName;
    }

    public JDBCDimensionSourceConfig setUserName(String userName) {
        this.userName = userName;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public JDBCDimensionSourceConfig setPassword(String password) {
        this.password = password;
        return this;
    }

    public String getDriverClass() {
        return driverClass;
    }

    public JDBCDimensionSourceConfig setDriverClass(String driverClass) {
        this.driverClass = driverClass;
        return this;
    }

    public int getMaxPoolSize() {
        return maxPoolSize;
    }

    public JDBCDimensionSourceConfig setMaxPoolSize(int maxPoolSize) {
        if(maxPoolSize == 0){
            throw new IllegalArgumentException("maxPoolSize is zero");
        }
        this.maxPoolSize = maxPoolSize;
        return this;
    }
}
