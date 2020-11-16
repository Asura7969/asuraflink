package com.asuraflink.common.async;

import org.apache.flink.configuration.Configuration;

public interface DimensionSource {

    void init(Configuration parameters);

    void shutdown();
}
