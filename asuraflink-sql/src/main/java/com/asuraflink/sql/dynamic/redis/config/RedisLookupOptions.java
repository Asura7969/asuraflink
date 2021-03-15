package com.asuraflink.sql.dynamic.redis.config;

import com.asuraflink.sql.user.delay.utils.RetryerElement;

import java.io.Serializable;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author asura7969
 * @create 2021-03-13-0:27
 */
public class RedisLookupOptions implements Serializable {

    // todo:维表延迟属性
    private static DelayQueue<DelayJoinElement<String>> delayQueue = new DelayQueue<>();

//    private static BlockingQueue<DelayJoinElement<?>> delayQueue = new LinkedBlockingQueue<>();

    public static volatile boolean running = true;

    public static int maxRetryCount = 3;

    public static void main(String[] args) throws InterruptedException {

        ExecutorService direct = org.apache.flink.runtime.concurrent.Executors.newDirectExecutorService();
        direct.execute(new Runnable() {
            @Override
            public void run() {
                while (running) {
                    try {
                        DelayJoinElement<String> element = delayQueue.take();
                        // 执行join
                        if (element.increment() <= maxRetryCount) {
                            // 没达到最大重试次数,继续添加到队列
                            System.out.println("retry task " + element.in + " retryCount:" + element.getRetryCount().get());
                            delayQueue.add(element);
                        } else {
                            // 达到最大重试次数,丢弃,metric 记录数据
                            System.out.println("abort task " + element.in + " retryCount:" + element.getRetryCount().get());
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        });




        for (int i = 0; i < 5; i++) {
            Thread.sleep(1000);
            delayQueue.add(DelayJoinElement.of(String.valueOf(i), Duration.ofSeconds(5)));
        }


    }




    public static class DelayJoinElement<T> implements Delayed{
        private AtomicInteger retryCount;
        private long nextRetryTime;
        private T in;

        public DelayJoinElement(AtomicInteger retryCount, long nextRetryTime, T in) {
            this.retryCount = retryCount;
            this.nextRetryTime = nextRetryTime;
            this.in = in;
        }

        public AtomicInteger getRetryCount() {
            return retryCount;
        }

        public T getIn() {
            return in;
        }

        public static <T> DelayJoinElement<T> of(T in, Duration duration) {
            long nextTime = duration.toMillis() + System.currentTimeMillis();
            return new DelayJoinElement<T>(new AtomicInteger(1), nextTime, in);
        }

        public int increment() {
            return this.retryCount.incrementAndGet();
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return nextRetryTime - System.currentTimeMillis();
        }

        @Override
        public int compareTo(Delayed o) {
            if (this == o) {
                return 0;
            }
            DelayJoinElement element = (DelayJoinElement)o;
            long diff = this.nextRetryTime - element.nextRetryTime;
            if (diff <= 0) {
                return -1;
            } else {
                return 1;
            }
        }
    }

}
