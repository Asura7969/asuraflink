package com.asuraflink.sink.multithread;

import java.util.concurrent.LinkedBlockingQueue;

public class MultiThreadConsumerClient<T> implements Runnable {

    private LinkedBlockingQueue<T> bufferQueue;

    public MultiThreadConsumerClient(LinkedBlockingQueue<T> bufferQueue) {
        this.bufferQueue = bufferQueue;
    }

    @Override
    public void run() {
        T entity;
        while (true){
            // 从 bufferQueue 的队首消费数据
            entity = bufferQueue.poll();
            // 执行 client 消费数据的逻辑
            doSomething(entity);
        }
    }

    // client 消费数据的逻辑
    private void doSomething(T entity) {
        // client 积攒批次并调用第三方 api
    }
}