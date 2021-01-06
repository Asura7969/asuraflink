package com.asuraflink.project.userPurchaseBehaviorTracker.schema;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.asuraflink.project.userPurchaseBehaviorTracker.model.UserEvent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class UserEventDeserializationSchema implements DeserializationSchema<UserEvent> {


    @Override
    public UserEvent deserialize(byte[] bytes) throws IOException {
        return JSON.parseObject(new String(bytes), new TypeReference<UserEvent>() {});
    }

    @Override
    public boolean isEndOfStream(UserEvent nextElement) {
        return false;
    }

    @Override
    public TypeInformation<UserEvent> getProducedType() {
        return TypeInformation.of(new TypeHint<UserEvent>() {});
    }
}
