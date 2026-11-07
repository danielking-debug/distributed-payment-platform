package com.daniel.distributed.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Externalized exponential backoff for the Kafka listener {@code DefaultErrorHandler}.
 * After the backoff budget is exhausted, failed records are published to {@code {topic}.DLT}.
 */
@ConfigurationProperties(prefix = "app.kafka.consumer.retry")
public class KafkaConsumerRetryProperties {

    /** Delay before the first retry (milliseconds). */
    private long initialIntervalMs = 500;

    /** Multiplier applied to the interval after each failed attempt. */
    private double multiplier = 2.0;

    /**
     * Maximum total backoff time (milliseconds). When the next sleep would exceed this,
     * retries stop and the record is recovered to the dead-letter topic.
     */
    private long maxElapsedMs = 10_000;

    public long getInitialIntervalMs() {
        return initialIntervalMs;
    }

    public void setInitialIntervalMs(long initialIntervalMs) {
        this.initialIntervalMs = initialIntervalMs;
    }

    public double getMultiplier() {
        return multiplier;
    }

    public void setMultiplier(double multiplier) {
        this.multiplier = multiplier;
    }

    public long getMaxElapsedMs() {
        return maxElapsedMs;
    }

    public void setMaxElapsedMs(long maxElapsedMs) {
        this.maxElapsedMs = maxElapsedMs;
    }
}
