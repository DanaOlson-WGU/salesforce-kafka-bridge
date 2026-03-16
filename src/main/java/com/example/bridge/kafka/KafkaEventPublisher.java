package com.example.bridge.kafka;

import com.example.bridge.model.ConnectionStatus;
import com.example.bridge.model.EventBatch;
import com.example.bridge.model.PublishResult;

/**
 * Interface for publishing Salesforce Platform Event batches to Kafka.
 */
public interface KafkaEventPublisher {

    /**
     * Publishes a batch of events to Kafka. On success, checkpoints the replay ID.
     * On failure, skips the checkpoint and returns a failure result.
     *
     * @param batch the event batch with org, topic, and events
     * @return PublishResult indicating success or failure
     */
    PublishResult publishBatch(EventBatch batch);

    /**
     * Returns current Kafka connection status for health checks.
     */
    ConnectionStatus getKafkaStatus();
}
