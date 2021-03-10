package com.asuraflink.sql.user.delay.func;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;

public class DelayOption implements Serializable {
    private final Duration delayTime;
    private final Duration intervalDuration;
    private final int maxRetryTimes;
    private final boolean ignoreExpiredData;

    public DelayOption(Duration delayTime, Duration intervalDuration, int maxRetryTimes, boolean ignoreExpiredData) {
        this.delayTime = delayTime;
        this.intervalDuration = intervalDuration;
        this.maxRetryTimes = maxRetryTimes;
        this.ignoreExpiredData = ignoreExpiredData;
    }

    public int getMaxRetryTimes() {
        return maxRetryTimes;
    }

    public Duration getDelayTime() {
        return delayTime;
    }

    public Duration getIntervalDuration() {
        return intervalDuration;
    }

    public boolean getIgnoreExpiredData() {
        return ignoreExpiredData;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DelayOption that = (DelayOption) o;
        return maxRetryTimes == that.maxRetryTimes && Objects.equals(delayTime, that.delayTime) && Objects.equals(intervalDuration, that.intervalDuration);
    }

    public static class Builder {
        private Duration delayTime = Duration.ofSeconds(5);
        private Duration intervalDuration = Duration.ofSeconds(2);
        private int maxRetryTimes = 3;
        private boolean ignoreExpiredData = true;

        public Builder setDelayTime(Duration delayTime) {
            this.delayTime = delayTime;
            return this;
        }

        public Builder setIntervalDuration(Duration intervalDuration) {
            this.intervalDuration = intervalDuration;
            return this;
        }

        public Builder setMaxRetryTimes(int maxRetryTimes) {
            this.maxRetryTimes = maxRetryTimes;
            return this;
        }

        public Builder setIgnoreExpiredData(boolean ignoreExpiredData) {
            this.ignoreExpiredData = ignoreExpiredData;
            return this;
        }

        public DelayOption build() {
            return new DelayOption(delayTime, intervalDuration, maxRetryTimes, ignoreExpiredData);
        }
    }
}
