package com.example.bridge.model;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Represents a batch of Platform Events received from a single Salesforce subscription.
 */
public class EventBatch {

    private final String org;
    private final String salesforceTopic;
    private final List<PlatformEvent> events;
    private final ReplayId replayId;
    private final Instant receivedAt;

    public EventBatch(String org, String salesforceTopic, List<PlatformEvent> events,
                      ReplayId replayId, Instant receivedAt) {
        this.org = Objects.requireNonNull(org, "org must not be null");
        this.salesforceTopic = Objects.requireNonNull(salesforceTopic, "salesforceTopic must not be null");
        this.events = Collections.unmodifiableList(Objects.requireNonNull(events, "events must not be null"));
        this.replayId = Objects.requireNonNull(replayId, "replayId must not be null");
        this.receivedAt = Objects.requireNonNull(receivedAt, "receivedAt must not be null");
    }

    public String getOrg() {
        return org;
    }

    public String getSalesforceTopic() {
        return salesforceTopic;
    }

    public List<PlatformEvent> getEvents() {
        return events;
    }

    public ReplayId getReplayId() {
        return replayId;
    }

    public Instant getReceivedAt() {
        return receivedAt;
    }

    public boolean isEmpty() {
        return events.isEmpty();
    }

    @Override
    public String toString() {
        return "EventBatch{org='" + org + "', topic='" + salesforceTopic +
                "', eventCount=" + events.size() + ", replayId=" + replayId + "}";
    }
}
