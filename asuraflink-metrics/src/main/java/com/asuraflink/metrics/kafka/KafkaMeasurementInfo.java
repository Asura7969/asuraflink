package com.asuraflink.metrics.kafka;

import java.util.Map;

final class KafkaMeasurementInfo {

    private final String name;
    private final Map<String, String> tag;

    KafkaMeasurementInfo(String name, Map<String, String> tags) {
        this.name = name;
        this.tag = tags;
    }

    String getName() {
        return name;
    }

    Map<String, String> getTags() {
        return tag;
    }
}
