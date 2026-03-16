package com.example.bridge.pubsub;

import com.example.bridge.model.EventBatch;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Handle for managing the lifecycle of a single Pub/Sub API subscription.
 * 
 * A Subscription represents an active gRPC streaming subscription to a
 * Salesforce Platform Event topic. It provides methods to check status,
 * stop the subscription, and register a callback for incoming event batches.
 */
public class Subscription {

    private final String org;
    private final String topic;
    private final AtomicBoolean active;
    private final Consumer<EventBatch> batchCallback;
    private final Runnable onStop;

    /**
     * Creates a new Subscription handle.
     *
     * @param org           the Salesforce org identifier
     * @param topic         the Salesforce Platform Event topic name
     * @param batchCallback callback invoked when an event batch is received
     * @param onStop        action to execute when the subscription is stopped
     */
    public Subscription(String org, String topic, Consumer<EventBatch> batchCallback, Runnable onStop) {
        this.org = Objects.requireNonNull(org, "org must not be null");
        this.topic = Objects.requireNonNull(topic, "topic must not be null");
        this.batchCallback = Objects.requireNonNull(batchCallback, "batchCallback must not be null");
        this.onStop = Objects.requireNonNull(onStop, "onStop must not be null");
        this.active = new AtomicBoolean(true);
    }

    /**
     * Returns the org identifier for this subscription.
     */
    public String getOrg() {
        return org;
    }

    /**
     * Returns the topic name for this subscription.
     */
    public String getTopic() {
        return topic;
    }

    /**
     * Returns whether this subscription is currently active.
     */
    public boolean isActive() {
        return active.get();
    }

    /**
     * Delivers an event batch to the registered callback.
     * Does nothing if the subscription is no longer active.
     *
     * @param batch the event batch to deliver
     */
    public void deliverBatch(EventBatch batch) {
        if (active.get()) {
            batchCallback.accept(batch);
        }
    }

    /**
     * Stops this subscription. Idempotent — calling multiple times has no additional effect.
     */
    public void stop() {
        if (active.compareAndSet(true, false)) {
            onStop.run();
        }
    }

    /**
     * Returns a unique key for this subscription (org + topic).
     */
    public String getKey() {
        return org + ":" + topic;
    }

    @Override
    public String toString() {
        return "Subscription{org='" + org + "', topic='" + topic + "', active=" + active.get() + "}";
    }
}
