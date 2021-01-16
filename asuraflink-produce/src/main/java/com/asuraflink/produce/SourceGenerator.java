package com.asuraflink.produce;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class SourceGenerator {

    private long speed = 1000; // 每秒1000条
    private KafkaProducer<String, String> producer;
    private String fileName;

    public SourceGenerator(KafkaProducer<String, String> producer, String fileName) {
        this.producer = producer;
        this.fileName = fileName;
    }

    public SourceGenerator(KafkaProducer<String, String> producer, String fileName, long speed) {
        this.producer = producer;
        this.fileName = fileName;
        this.speed = speed;
    }

    public SourceGenerator() {
    }

    public void produceData(String topic, String key){
        long delay = 1000_000 / speed; // 每条耗时多少毫秒

        try (InputStream inputStream = SourceGenerator.class.getClassLoader().getResourceAsStream(fileName)) {
            assert inputStream != null;
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            long start = System.nanoTime();
            while (reader.ready()) {
                String line = reader.readLine();
                System.out.println(line);
                producer.send(new ProducerRecord<>(topic, key, line));

                long end = System.nanoTime();
                long diff = end - start;
                while (diff < (delay * 1000)) {
                    Thread.sleep(1);
                    end = System.nanoTime();
                    diff = end - start;
                }
                start = end;
            }
            reader.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            producer.flush();
            producer.close();
        }
    }

    public static void main(String[] args) {

    }
}
