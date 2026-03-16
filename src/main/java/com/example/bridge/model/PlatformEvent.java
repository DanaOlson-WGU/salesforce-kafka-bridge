package com.example.bridge.model;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Objects;

/**
 * Represents a single Salesforce Platform Event received from the Pub/Sub API.
 */
public class PlatformEvent {

    private final String eventType;
    private final JsonNode payload;
    private final ReplayId replayId;

    public PlatformEvent(String eventType, JsonNode payload, ReplayId replayId) {
        this.eventType = Objects.requireNonNull(eventType, "eventType must not be null");
        this.payload = Objects.requireNonNull(payload, "payload must not be null");
        this.replayId = Objects.requireNonNull(replayId, "replayId must not be null");
    }

    public String getEventType() {
        return eventType;
    }

    public JsonNode getPayload() {
        return payload;
    }

    public ReplayId getReplayId() {
        return replayId;
    }

    @Override
    public String toString() {
        return "PlatformEvent{eventType='" + eventType + "', replayId=" + replayId + "}";
    }
}
