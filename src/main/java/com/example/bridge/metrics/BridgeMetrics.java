package com.example.bridge.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

/**
 * Centralized metrics service for the Salesforce-Kafka Bridge.
 * Provides counters and timers for event processing, checkpointing, and reconnection.
 */
@Component
public class BridgeMetrics {

    private static final String EVENTS_RECEIVED = "bridge.events.received";
    private static final String EVENTS_PUBLISHED = "bridge.events.published";
    private static final String CHECKPOINT_SUCCESS = "bridge.checkpoint.success";
    private static final String CHECKPOINT_FAILURE = "bridge.checkpoint.failure";
    private static final String RECONNECT_ATTEMPTS = "bridge.reconnect.attempts";
    private static final String BATCH_PROCESSING_TIME = "bridge.batch.processing.time";

    private static final String TAG_ORG = "org";
    private static final String TAG_TOPIC = "topic";

    private final MeterRegistry meterRegistry;
    private final ConcurrentMap<String, Counter> eventsReceivedCounters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Counter> eventsPublishedCounters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Counter> checkpointSuccessCounters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Counter> checkpointFailureCounters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Counter> reconnectAttemptCounters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Timer> batchProcessingTimers = new ConcurrentHashMap<>();

    public BridgeMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    /**
     * Increments the events received counter for the given org and topic.
     */
    public void recordEventsReceived(String org, String topic, int count) {
        getOrCreateCounter(eventsReceivedCounters, EVENTS_RECEIVED, org, topic).increment(count);
    }

    /**
     * Increments the events published counter for the given org and topic.
     */
    public void recordEventsPublished(String org, String topic, int count) {
        getOrCreateCounter(eventsPublishedCounters, EVENTS_PUBLISHED, org, topic).increment(count);
    }

    /**
     * Increments the checkpoint success counter for the given org and topic.
     */
    public void recordCheckpointSuccess(String org, String topic) {
        getOrCreateCounter(checkpointSuccessCounters, CHECKPOINT_SUCCESS, org, topic).increment();
    }

    /**
     * Increments the checkpoint failure counter for the given org and topic.
     */
    public void recordCheckpointFailure(String org, String topic) {
        getOrCreateCounter(checkpointFailureCounters, CHECKPOINT_FAILURE, org, topic).increment();
    }

    /**
     * Increments the reconnect attempts counter for the given org.
     */
    public void recordReconnectAttempt(String org) {
        reconnectAttemptCounters.computeIfAbsent(org, o ->
                Counter.builder(RECONNECT_ATTEMPTS)
                        .tag(TAG_ORG, o)
                        .register(meterRegistry)
        ).increment();
    }

    /**
     * Records the batch processing time for the given org and topic.
     */
    public <T> T recordBatchProcessingTime(String org, String topic, Supplier<T> operation) {
        return getOrCreateTimer(org, topic).record(operation);
    }

    /**
     * Records the batch processing time for the given org and topic (void operation).
     */
    public void recordBatchProcessingTime(String org, String topic, Runnable operation) {
        getOrCreateTimer(org, topic).record(operation);
    }

    private Counter getOrCreateCounter(ConcurrentMap<String, Counter> cache, String name, String org, String topic) {
        String key = org + ":" + topic;
        return cache.computeIfAbsent(key, k ->
                Counter.builder(name)
                        .tag(TAG_ORG, org)
                        .tag(TAG_TOPIC, topic)
                        .register(meterRegistry)
        );
    }

    private Timer getOrCreateTimer(String org, String topic) {
        String key = org + ":" + topic;
        return batchProcessingTimers.computeIfAbsent(key, k ->
                Timer.builder(BATCH_PROCESSING_TIME)
                        .tag(TAG_ORG, org)
                        .tag(TAG_TOPIC, topic)
                        .register(meterRegistry)
        );
    }
}
