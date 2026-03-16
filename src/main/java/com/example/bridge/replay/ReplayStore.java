package com.example.bridge.replay;

import com.example.bridge.model.ConnectionStatus;
import com.example.bridge.model.ReplayId;

import java.util.Optional;

/**
 * Interface for persisting and retrieving Salesforce replay IDs.
 */
public interface ReplayStore {

    /**
     * Retrieves the last checkpointed replay ID for an org and topic.
     *
     * @param org the Salesforce org identifier
     * @param topic the Salesforce topic name
     * @return Optional containing replay ID, or empty if none exists
     */
    Optional<ReplayId> getLastReplayId(String org, String topic);

    /**
     * Atomically updates the replay ID for an org and topic.
     * Creates a new entry if none exists, updates existing entry otherwise.
     *
     * @param org the Salesforce org identifier
     * @param topic the Salesforce topic name
     * @param replayId the replay ID to persist
     * @throws CheckpointException if database write fails
     */
    void checkpoint(String org, String topic, ReplayId replayId) throws CheckpointException;

    /**
     * Returns database connection status for health checks.
     *
     * @return ConnectionStatus indicating database health
     */
    ConnectionStatus getDatabaseStatus();
}
