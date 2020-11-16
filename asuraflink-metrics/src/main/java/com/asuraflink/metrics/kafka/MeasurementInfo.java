package com.asuraflink.metrics.kafka;

import java.util.Map;

final class MeasurementInfo {
	private final String name;
	private final Map<String, String> tags;

	MeasurementInfo(String name, Map<String, String> tags) {
		this.name = name;
		this.tags = tags;
	}

	String getName() {
		return name;
	}

	Map<String, String> getTags() {
		return tags;
	}
}