package com.example.bridge.pubsub;

import com.example.bridge.model.ConnectionStatus;
import com.example.bridge.model.ReplayId;

/**
 * Interface for managing gRPC connections to the Salesforce Pub/Sub API
 * and handling event stream subscriptions.
 * 
 * Responsibilities:
 * - Establish and maintain gRPC connections per org
 * - Subscribe to configured Salesforce topics with replay ID positioning
 * - Handle connection failures with exponential backoff
 * - Process incoming event batches including empty batches
 * - Coordinate with CircuitBreaker for failure handling
 */
public interface PubSubConnector {

    /**
     * Establishes subscription for a given org and topic.
     * 
     * @param org The Salesforce org identifier (e.g., "srm", "acad")
     * @param topic The Salesforce Platform Event topic name
     * @param replayId The replay ID to start from, or null for earliest
     * @return Subscription handle for lifecycle management
     */
    Subscription subscribe(String org, String topic, ReplayId replayId);

    /**
     * Stops subscription for a given org and topic.
     * Called on checkpoint failures or circuit breaker open.
     * 
     * @param org The Salesforce org identifier
     * @param topic The Salesforce Platform Event topic name
     */
    void stopSubscription(String org, String topic);

    /**
     * Attempts to reconnect with exponential backoff.
     * 
     * @param org The Salesforce org identifier
     * @return true if reconnection successful, false otherwise
     */
    boolean reconnect(String org);

    /**
     * Returns current connection status for health checks.
     * 
     * @param org The Salesforce org identifier
     * @return ConnectionStatus indicating the health of the connection
     */
    ConnectionStatus getStatus(String org);
}
