package com.asuraflink.metrics.kafka;

import org.apache.flink.metrics.*;
import org.apache.flink.metrics.reporter.Scheduled;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.asuraflink.metrics.kafka.KafkaReporterOptions.*;

public class KafkaMetricReporter extends AbstractReporter<KafkaMeasurementInfo> implements Scheduled {

    private String SERVER;
    private String TOPIC_NAME;
    private String KEY_SERIALIZER_CLASS;
    private String VALUE_SERIALIZER_CLASS;
    private Producer<String, String> KAFKA_SERVER;

    public KafkaMetricReporter() {
        super(new MeasurementInfoProvider());
    }

    @Override
    public void open(MetricConfig metricConfig) {
        String host = getString(metricConfig, HOST);
        String topic = getString(metricConfig, TOPIC);
        String key_serializer = getString(metricConfig, KEY_SERIALIZER);
        String value_serializer = getString(metricConfig, VALUE_SERIALIZER);
        if(!isValidHost(host) || !isValidHost(topic)){
            throw new IllegalArgumentException("Invalid host/topic configuration. Host: " + host + " Topic: " + topic);
        }
        this.SERVER = host;
        this.TOPIC_NAME = topic;
        this.KEY_SERIALIZER_CLASS = key_serializer;
        this.VALUE_SERIALIZER_CLASS = value_serializer;
        this.KAFKA_SERVER = initKafkaProducer(this.SERVER,this.KEY_SERIALIZER_CLASS,this.VALUE_SERIALIZER_CLASS);
    }

    @Override
    public void close() {
        if(null != this.KAFKA_SERVER){
            this.KAFKA_SERVER.close();
            this.KAFKA_SERVER = null;
        }
    }

    @Override
    public void report() {
        List<String> builder = buildMsg();
        if(builder.size() > 0){
            builder.forEach(this::write);
        }
    }

    private List<String> buildMsg(){
        List<String> metrics = new ArrayList<>();

        Instant timestamp = Instant.now();
        for (Map.Entry<Gauge<?>, KafkaMeasurementInfo> entry : gauges.entrySet()) {
            metrics.add(MetricMapper.map(entry.getValue(), timestamp, entry.getKey()));
        }
        for (Map.Entry<Counter, KafkaMeasurementInfo> entry : counters.entrySet()) {
            metrics.add(MetricMapper.map(entry.getValue(), timestamp, entry.getKey()));
        }
        for (Map.Entry<Histogram, KafkaMeasurementInfo> entry : histograms.entrySet()) {
            metrics.add(MetricMapper.map(entry.getValue(), timestamp, entry.getKey()));
        }
        for (Map.Entry<Meter, KafkaMeasurementInfo> entry : meters.entrySet()) {
            metrics.add(MetricMapper.map(entry.getValue(), timestamp, entry.getKey()));
        }
        return metrics;
    }

    private void write(String msg){
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(this.TOPIC_NAME,null,msg);
        this.KAFKA_SERVER.send(producerRecord);
    }

    private static Producer<String, String> initKafkaProducer(String kafkaAddress, String keySerializer, String valueSerializer){
        Properties kafkaProducerConfig = new Properties();
        kafkaProducerConfig.setProperty("bootstrap.servers", kafkaAddress);
        kafkaProducerConfig.setProperty("key.serializer", keySerializer);
        kafkaProducerConfig.setProperty("value.serializer", valueSerializer);
        return new KafkaProducer<>(kafkaProducerConfig);
    }

    private static boolean isValidHost(String host) {
        return host != null && !host.isEmpty();
    }

    private static boolean isValidPort(int port) {
        return 0 < port && port <= 65535;
    }
}
