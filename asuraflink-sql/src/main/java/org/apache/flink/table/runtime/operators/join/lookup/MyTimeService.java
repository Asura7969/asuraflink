package org.apache.flink.table.runtime.operators.join.lookup;

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeServiceUtil;
import org.apache.flink.streaming.runtime.tasks.TimerService;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.NeverCompleteFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author asura7969
 * @create 2021-04-05-22:59
 * @see org.apache.flink.streaming.runtime.tasks.SystemProcessingTimeService
 */
public class MyTimeService implements TimerService {
    private static final Logger LOG = LoggerFactory.getLogger(MyTimeService.class);
    private static final int STATUS_ALIVE = 0;
    private static final int STATUS_QUIESCED = 1;
    private static final int STATUS_SHUTDOWN = 2;
    private final ScheduledThreadPoolExecutor timerService;

    private final AtomicInteger status;
    private final CompletableFuture<Void> quiesceCompletedFuture;
    private final Queue<Long> queue;
    private final MyTriggerable triggerTarget;

    private ScheduledFuture<?> nextTimer;

    MyTimeService (ThreadFactory threadFactory, MyTriggerable triggerTarget){
        this.status = new AtomicInteger(STATUS_ALIVE);
        this.quiesceCompletedFuture = new CompletableFuture<>();
        if (threadFactory == null) {
            this.timerService = new ScheduledTaskExecutor(1);
        } else {
            this.timerService = new ScheduledTaskExecutor(1, threadFactory);
        }
        this.timerService.setRemoveOnCancelPolicy(true);

        // make sure shutdown removes all pending tasks
        this.timerService.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        this.timerService.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        this.triggerTarget = Preconditions.checkNotNull(triggerTarget);;
        this.queue = new PriorityQueue<>();
    }

    @Override
    public boolean isTerminated() {
        return status.get() == STATUS_SHUTDOWN;
    }

    @Override
    public void shutdownService() {
        if (status.compareAndSet(STATUS_ALIVE, STATUS_SHUTDOWN)
                || status.compareAndSet(STATUS_QUIESCED, STATUS_SHUTDOWN)) {
            timerService.shutdownNow();
        }
    }

    @Override
    public boolean shutdownServiceUninterruptible(long timeoutMs) {
        final Deadline deadline = Deadline.fromNow(Duration.ofMillis(timeoutMs));

        boolean shutdownComplete = false;
        boolean receivedInterrupt = false;

        do {
            try {
                // wait for a reasonable time for all pending timer threads to finish
                shutdownComplete =
                        shutdownAndAwaitPending(
                                deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException iex) {
                receivedInterrupt = true;
                LOG.trace("Intercepted attempt to interrupt timer service shutdown.", iex);
            }
        } while (deadline.hasTimeLeft() && !shutdownComplete);

        if (receivedInterrupt) {
            Thread.currentThread().interrupt();
        }

        return shutdownComplete;
    }

    boolean shutdownAndAwaitPending(long time, TimeUnit timeUnit) throws InterruptedException {
        shutdownService();
        return timerService.awaitTermination(time, timeUnit);
    }

    @Override
    public long getCurrentProcessingTime() {
        return System.currentTimeMillis();
    }

    public void registerProcessingTimeTimer(long time) {
        Long oldTime = queue.peek();
        if (!queue.contains(time) && queue.add(time)) {
            long nextTriggerTime = oldTime != null ? oldTime : Long.MAX_VALUE;
            if (time < nextTriggerTime) {
                if (nextTimer != null) {
                    nextTimer.cancel(false);
                }
                nextTimer = registerTimer(time, this::onProcessingTime);
            }
        }
    }

    private void onProcessingTime(long time) throws Exception {
        // null out the timer in case the Triggerable calls registerProcessingTimeTimer()
        // inside the callback.
        nextTimer = null;

        Long timer;

        while ((timer = queue.peek()) != null && timer <= time) {
            queue.poll();
            triggerTarget.onProcessingTime(timer);
        }

        if (timer != null && nextTimer == null) {
            nextTimer = registerTimer(timer, this::onProcessingTime);
        }
    }

    @Override
    public ScheduledFuture<?> registerTimer(long timestamp, ProcessingTimeCallback callback) {
        long delay =
                ProcessingTimeServiceUtil.getProcessingTimeDelay(
                        timestamp, getCurrentProcessingTime());

        // we directly try to register the timer and only react to the status on exception
        // that way we save unnecessary volatile accesses for each timer
        try {
            return timerService.schedule(
                    wrapOnTimerCallback(callback, timestamp), delay, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            final int status = this.status.get();
            if (status == STATUS_QUIESCED) {
                return new NeverCompleteFuture(delay);
            } else if (status == STATUS_SHUTDOWN) {
                throw new IllegalStateException("Timer service is shut down");
            } else {
                // something else happened, so propagate the exception
                throw e;
            }
        }
    }
    private Runnable wrapOnTimerCallback(ProcessingTimeCallback callback, long timestamp) {
        return new ScheduledTask(status, callback, timestamp, 0);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(ProcessingTimeCallback callback, long initialDelay, long period) {
        return scheduleRepeatedly(callback, initialDelay, period, false);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(ProcessingTimeCallback callback, long initialDelay, long period) {
        return scheduleRepeatedly(callback, initialDelay, period, true);
    }

    private ScheduledFuture<?> scheduleRepeatedly(
            ProcessingTimeCallback callback, long initialDelay, long period, boolean fixedDelay) {
        final long nextTimestamp = getCurrentProcessingTime() + initialDelay;
        final Runnable task = wrapOnTimerCallback(callback, nextTimestamp, period);

        // we directly try to register the timer and only react to the status on exception
        // that way we save unnecessary volatile accesses for each timer
        try {
            return fixedDelay
                    ? timerService.scheduleWithFixedDelay(
                    task, initialDelay, period, TimeUnit.MILLISECONDS)
                    : timerService.scheduleAtFixedRate(
                    task, initialDelay, period, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            final int status = this.status.get();
            if (status == STATUS_QUIESCED) {
                return new NeverCompleteFuture(initialDelay);
            } else if (status == STATUS_SHUTDOWN) {
                throw new IllegalStateException("Timer service is shut down");
            } else {
                // something else happened, so propagate the exception
                throw e;
            }
        }
    }

    private Runnable wrapOnTimerCallback(
            ProcessingTimeCallback callback, long nextTimestamp, long period) {
        return new ScheduledTask(status, callback, nextTimestamp, period);
    }

    @Override
    public CompletableFuture<Void> quiesce() {
        if (status.compareAndSet(STATUS_ALIVE, STATUS_QUIESCED)) {
            timerService.shutdown();
        }

        return quiesceCompletedFuture;
    }

    private class ScheduledTaskExecutor extends ScheduledThreadPoolExecutor {

        public ScheduledTaskExecutor(int corePoolSize) {
            super(corePoolSize);
        }

        public ScheduledTaskExecutor(int corePoolSize, ThreadFactory threadFactory) {
            super(corePoolSize, threadFactory);
        }

        @Override
        protected void terminated() {
            super.terminated();
            quiesceCompletedFuture.complete(null);
        }
    }

    private static final class ScheduledTask implements Runnable {
        private final AtomicInteger serviceStatus;
        private final ProcessingTimeCallback callback;

        private long nextTimestamp;
        private final long period;

        ScheduledTask(
                AtomicInteger serviceStatus,
                ProcessingTimeCallback callback,
                long timestamp,
                long period) {
            this.serviceStatus = serviceStatus;
            this.callback = callback;
            this.nextTimestamp = timestamp;
            this.period = period;
        }

        @Override
        public void run() {
            if (serviceStatus.get() != STATUS_ALIVE) {
                return;
            }
            try {
                callback.onProcessingTime(nextTimestamp);
            } catch (Exception ex) {
                // TODO:
                ex.printStackTrace();
            }
            nextTimestamp += period;
        }
    }
}
