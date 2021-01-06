package com.asuraflink.project.userPurchaseBehaviorTracker.schema;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.asuraflink.project.userPurchaseBehaviorTracker.model.Config;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class ConfigDeserializationSchema implements DeserializationSchema<Config> {
    @Override
    public Config deserialize(byte[] bytes) throws IOException {
        return JSON.parseObject(new String(bytes), new TypeReference<Config>() {});
    }

    @Override
    public boolean isEndOfStream(Config config) {
        return false;
    }

    @Override
    public TypeInformation<Config> getProducedType() {
        return TypeInformation.of(new TypeHint<Config>() {});
    }
}
