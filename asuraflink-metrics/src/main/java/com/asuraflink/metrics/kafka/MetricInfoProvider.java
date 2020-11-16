package com.asuraflink.metrics.kafka;

import org.apache.flink.metrics.MetricGroup;

interface MetricInfoProvider<MetricInfo> {

	MetricInfo getMetricInfo(String metricName, MetricGroup group);
}