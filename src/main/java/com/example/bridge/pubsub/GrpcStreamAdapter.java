package com.example.bridge.pubsub;

import com.example.bridge.model.EventBatch;
import com.example.bridge.model.ReplayId;

import java.util.function.Consumer;

/**
 * Abstraction layer over the actual Salesforce gRPC Pub/Sub API streaming calls.
 * 
 * This interface decouples the {@link GrpcPubSubConnector} from the Salesforce
 * proto-generated gRPC stubs, making the connector testable without requiring
 * the actual Salesforce proto definitions. When the proto files are added to the
 * project, a concrete implementation of this interface will bridge to the
 * generated gRPC client.
 */
public interface GrpcStreamAdapter {

    /**
     * Opens a gRPC streaming subscription to a Salesforce Platform Event topic.
     *
     * @param org           the Salesforce org identifier
     * @param topic         the Salesforce Platform Event topic name
     * @param replayId      the replay ID to start from, or null for earliest
     * @param accessToken   the OAuth access token for authentication
     * @param instanceUrl   the Salesforce instance URL
     * @param batchCallback callback invoked when an event batch is received
     * @param errorCallback callback invoked when a streaming error occurs
     * @return a StreamHandle for managing the stream lifecycle
     */
    StreamHandle subscribe(String org, String topic, ReplayId replayId,
                           String accessToken, String instanceUrl,
                           Consumer<EventBatch> batchCallback,
                           Consumer<Throwable> errorCallback);

    /**
     * Checks whether the gRPC channel for the given org is connected and healthy.
     *
     * @param org the Salesforce org identifier
     * @return true if the channel is connected
     */
    boolean isConnected(String org);

    /**
     * Creates or retrieves the gRPC ManagedChannel for the given org.
     * Each org gets its own isolated channel.
     *
     * @param org       the Salesforce org identifier
     * @param pubsubUrl the Pub/Sub API endpoint URL (host:port)
     */
    void createChannel(String org, String pubsubUrl);

    /**
     * Shuts down the gRPC channel for the given org.
     *
     * @param org the Salesforce org identifier
     */
    void shutdownChannel(String org);

    /**
     * Handle for managing an active gRPC stream.
     */
    interface StreamHandle {

        /**
         * Cancels the active stream.
         */
        void cancel();

        /**
         * Returns whether the stream is still active.
         */
        boolean isActive();
    }
}
