package com.asuraflink.common.async.config;

/**
 * 维度表的配置信息
 */
abstract public class DimensionSourceConfig {

    // 是否使用缓存
    private boolean hasCache = true;

    public boolean isHasCache() {
        return hasCache;
    }

    public DimensionSourceConfig setHasCache(boolean hasCache) {
        this.hasCache = hasCache;
        return this;
    }
}
