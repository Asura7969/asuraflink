package com.asuraflink.metrics.kafka;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.metrics.*;

import java.time.Instant;
import java.util.Map;

public class MetricMapper {
    static String map(KafkaMeasurementInfo info, Instant timestamp, Gauge<?> gauge) {
        JSONObject jsonBuilder = builder(info, timestamp);
        Object value = gauge.getValue();
        jsonBuilder.put("value",String.valueOf(value));
        return jsonBuilder.toJSONString();
    }

    static String map(KafkaMeasurementInfo info, Instant timestamp, Counter counter) {
        JSONObject jsonBuilder = builder(info, timestamp);
        jsonBuilder.put("count", counter.getCount());
        return jsonBuilder.toJSONString();
    }

    static String map(KafkaMeasurementInfo info, Instant timestamp, Histogram histogram) {
        HistogramStatistics statistics = histogram.getStatistics();
        JSONObject jsonBuilder = builder(info, timestamp);
        jsonBuilder.put("count", statistics.size());
        jsonBuilder.put("min", statistics.getMin());
        jsonBuilder.put("max", statistics.getMax());
        jsonBuilder.put("mean", statistics.getMean());
        jsonBuilder.put("stddev", statistics.getStdDev());
        jsonBuilder.put("p50", statistics.getQuantile(.50));
        jsonBuilder.put("p75", statistics.getQuantile(.75));
        jsonBuilder.put("p95", statistics.getQuantile(.95));
        jsonBuilder.put("p98", statistics.getQuantile(.98));
        jsonBuilder.put("p99", statistics.getQuantile(.99));
        jsonBuilder.put("p999", statistics.getQuantile(.999));
        return jsonBuilder.toJSONString();
    }

    static String map(KafkaMeasurementInfo info, Instant timestamp, Meter meter) {
        JSONObject jsonBuilder = builder(info, timestamp);
        jsonBuilder.put("count", meter.getCount());
        jsonBuilder.put("rate", meter.getRate());
        return jsonBuilder.toJSONString();
    }

    private static JSONObject builder(KafkaMeasurementInfo info, Instant timestamp) {
        return MetricInfoFactory.getBuilder(info.getName(),timestamp.toEpochMilli(),info.getTags());
    }

    private static class MetricInfoFactory{
        private static JSONObject getBuilder(String name, long timestamp, Map<String, String> tags){
            JSONObject json = new JSONObject();
            json.put("name",name);
            json.put("timestamp",timestamp);
            JSONArray tagsArray = new JSONArray();
            JSONObject tag = new JSONObject();
            for (Map.Entry<String, String> entry : tags.entrySet()) {
                tag.put(entry.getKey(),entry.getValue());
            }
            tagsArray.add(tag);
            json.put("tags",tagsArray);
            return json;
        }
    }
}
