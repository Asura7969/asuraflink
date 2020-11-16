package com.asuraflink.metrics.kafka;

import org.apache.flink.metrics.*;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

abstract class AbstractReporter<MetricInfo> implements MetricReporter {
	protected final Logger log = LoggerFactory.getLogger(getClass());

	protected final Map<Gauge<?>, MetricInfo> gauges = new HashMap<>();
	protected final Map<Counter, MetricInfo> counters = new HashMap<>();
	protected final Map<Histogram, MetricInfo> histograms = new HashMap<>();
	protected final Map<Meter, MetricInfo> meters = new HashMap<>();
	protected final MetricInfoProvider<MetricInfo> metricInfoProvider;

	protected AbstractReporter(MetricInfoProvider<MetricInfo> metricInfoProvider) {
		this.metricInfoProvider = metricInfoProvider;
	}

	@Override
	public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
		final MetricInfo metricInfo = metricInfoProvider.getMetricInfo(metricName, group);
		synchronized (this) {
			if (metric instanceof Counter) {
				counters.put((Counter) metric, metricInfo);
			} else if (metric instanceof Gauge) {
				gauges.put((Gauge<?>) metric, metricInfo);
			} else if (metric instanceof Histogram) {
				histograms.put((Histogram) metric, metricInfo);
			} else if (metric instanceof Meter) {
				meters.put((Meter) metric, metricInfo);
			} else {
				log.warn("Cannot add unknown metric type {}. This indicates that the reporter " +
					"does not support this metric type.", metric.getClass().getName());
			}
		}
	}

	@Override
	public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
		synchronized (this) {
			if (metric instanceof Counter) {
				counters.remove(metric);
			} else if (metric instanceof Gauge) {
				gauges.remove(metric);
			} else if (metric instanceof Histogram) {
				histograms.remove(metric);
			} else if (metric instanceof Meter) {
				meters.remove(metric);
			} else {
				log.warn("Cannot remove unknown metric type {}. This indicates that the reporter " +
					"does not support this metric type.", metric.getClass().getName());
			}
		}
	}
}