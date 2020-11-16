package com.asuraflink.sink.multithread;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.collect.Queues;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MultiThreadConsumerSink<T> extends RichSinkFunction<T> {
    // Client 线程的默认数量
    private final int DEFAULT_CLIENT_THREAD_NUM = 5;
    // 数据缓冲队列的默认容量
    private final int DEFAULT_QUEUE_CAPACITY = 5000;

    private LinkedBlockingQueue<T> bufferQueue;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // new 一个容量为 DEFAULT_CLIENT_THREAD_NUM 的线程池
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(DEFAULT_CLIENT_THREAD_NUM, DEFAULT_CLIENT_THREAD_NUM,
                0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        // new 一个容量为 DEFAULT_QUEUE_CAPACITY 的数据缓冲队列
        this.bufferQueue = Queues.newLinkedBlockingQueue(DEFAULT_QUEUE_CAPACITY);
        // 创建并开启消费者线程
        MultiThreadConsumerClient consumerClient = new MultiThreadConsumerClient(bufferQueue);
        for (int i=0; i < DEFAULT_CLIENT_THREAD_NUM; i++) {
            threadPoolExecutor.execute(consumerClient);
        }
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        // 往 bufferQueue 的队尾添加数据
        bufferQueue.put(value);
    }
}