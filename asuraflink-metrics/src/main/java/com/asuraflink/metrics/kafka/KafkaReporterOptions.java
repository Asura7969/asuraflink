package com.asuraflink.metrics.kafka;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.metrics.MetricConfig;

public class KafkaReporterOptions {

    private final static String SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";

    public static final ConfigOption<String> HOST = ConfigOptions
            .key("host")
            .noDefaultValue()
            .withDescription("the Kafka server host");

    public static final ConfigOption<String> TOPIC = ConfigOptions
            .key("topic")
            .noDefaultValue()
            .withDescription("the Kafka server topic");

    public static final ConfigOption<String> KEY_SERIALIZER = ConfigOptions
            .key("key-serializer")
            .defaultValue(SERIALIZER)
            .withDescription("the Kafka key serializer");

    public static final ConfigOption<String> VALUE_SERIALIZER = ConfigOptions
            .key("value-serializer")
            .defaultValue(SERIALIZER)
            .withDescription("the Kafka value serializer");

    static String getString(MetricConfig config, ConfigOption<String> key) {
        return config.getString(key.key(), key.defaultValue());
    }

    static int getInteger(MetricConfig config, ConfigOption<Integer> key) {
        return config.getInteger(key.key(), key.defaultValue());
    }
}
