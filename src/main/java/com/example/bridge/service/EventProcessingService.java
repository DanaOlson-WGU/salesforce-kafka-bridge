package com.example.bridge.service;

import com.example.bridge.config.OrgConfig;
import com.example.bridge.config.SalesforceProperties;
import com.example.bridge.kafka.KafkaEventPublisher;
import com.example.bridge.metrics.BridgeMetrics;
import com.example.bridge.model.EventBatch;
import com.example.bridge.model.PublishResult;
import com.example.bridge.model.ReplayId;
import com.example.bridge.pubsub.GrpcPubSubConnector;
import com.example.bridge.pubsub.Subscription;
import com.example.bridge.replay.CheckpointException;
import com.example.bridge.replay.ReplayStore;
import com.example.bridge.resilience.CircuitBreaker;
import com.example.bridge.resilience.CircuitOpenException;
import com.example.bridge.resilience.CircuitState;
import com.example.bridge.routing.TopicRouter;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Coordinates the event processing pipeline: PubSubConnector → TopicRouter → KafkaEventPublisher.
 * 
 * Responsibilities:
 * - Start all org subscriptions at application startup
 * - Process event batches: receive → check circuit → route → publish → checkpoint
 * - Handle empty batches by checkpointing replay ID without publishing
 * - Implement fail-fast behavior: on checkpoint failure, stop subscription and open circuit
 * 
 * Requirements: 1.4, 2.1, 2.4, 3.4, 4.1, 4.2
 */
@Service
public class EventProcessingService {

    private static final Logger log = LoggerFactory.getLogger(EventProcessingService.class);

    private final SalesforceProperties salesforceProperties;
    private final GrpcPubSubConnector pubSubConnector;
    private final TopicRouter topicRouter;
    private final KafkaEventPublisher kafkaEventPublisher;
    private final CircuitBreaker circuitBreaker;
    private final ReplayStore replayStore;
    private final BridgeMetrics bridgeMetrics;

    private final Map<String, Subscription> activeSubscriptions = new ConcurrentHashMap<>();

    public EventProcessingService(
            SalesforceProperties salesforceProperties,
            GrpcPubSubConnector pubSubConnector,
            TopicRouter topicRouter,
            KafkaEventPublisher kafkaEventPublisher,
            CircuitBreaker circuitBreaker,
            ReplayStore replayStore,
            BridgeMetrics bridgeMetrics) {
        this.salesforceProperties = salesforceProperties;
        this.pubSubConnector = pubSubConnector;
        this.topicRouter = topicRouter;
        this.kafkaEventPublisher = kafkaEventPublisher;
        this.circuitBreaker = circuitBreaker;
        this.replayStore = replayStore;
        this.bridgeMetrics = bridgeMetrics;
    }

    /**
     * Starts subscriptions for all enabled orgs and their configured topics at startup.
     */
    @PostConstruct
    public void start() {
        log.info("Starting EventProcessingService");
        pubSubConnector.initialize();

        for (Map.Entry<String, OrgConfig> entry : salesforceProperties.getOrgs().entrySet()) {
            String orgId = entry.getKey();
            OrgConfig orgConfig = entry.getValue();

            if (!orgConfig.isEnabled()) {
                log.info("Org '{}' is disabled, skipping subscription", orgId);
                continue;
            }

            for (String topic : orgConfig.getTopics()) {
                subscribeToTopic(orgId, topic);
            }
        }

        log.info("EventProcessingService started with {} active subscriptions", activeSubscriptions.size());
    }

    /**
     * Shuts down all subscriptions on application shutdown.
     */
    @PreDestroy
    public void shutdown() {
        log.info("Shutting down EventProcessingService");
        pubSubConnector.shutdown();
        activeSubscriptions.clear();
    }

    /**
     * Subscribes to a single Salesforce topic for the given org.
     */
    void subscribeToTopic(String org, String topic) {
        Optional<ReplayId> lastReplayId = replayStore.getLastReplayId(org, topic);
        ReplayId replayId = lastReplayId.orElse(null);

        log.info("Subscribing to org='{}', topic='{}', replayId={}", org, topic, replayId);
        Subscription subscription = pubSubConnector.subscribe(org, topic, replayId);
        activeSubscriptions.put(subscriptionKey(org, topic), subscription);
    }

    /**
     * Processes an incoming event batch through the pipeline.
     * This is the batch callback invoked by the PubSubConnector.
     * 
     * Flow: receive batch → check circuit → route → publish → checkpoint
     */
    public void processBatch(EventBatch batch) {
        String org = batch.getOrg();
        String topic = batch.getSalesforceTopic();

        log.debug("Processing batch: org='{}', topic='{}', events={}", org, topic, batch.getEvents().size());

        // Record events received
        if (!batch.isEmpty()) {
            bridgeMetrics.recordEventsReceived(org, topic, batch.getEvents().size());
        }

        // Check circuit breaker state
        CircuitState state = circuitBreaker.getState(org);
        if (state == CircuitState.OPEN) {
            log.warn("Circuit breaker OPEN for org='{}'. Dropping batch for topic='{}'", org, topic);
            return;
        }

        bridgeMetrics.recordBatchProcessingTime(org, topic, () -> {
            try {
                if (batch.isEmpty()) {
                    handleEmptyBatch(batch);
                } else {
                    handleEventBatch(batch);
                }
            } catch (CircuitOpenException e) {
                log.warn("Circuit opened during processing for org='{}', topic='{}'", org, topic);
            } catch (Exception e) {
                log.error("Unexpected error processing batch for org='{}', topic='{}'", org, topic, e);
                circuitBreaker.recordFailure(org, e);
            }
        });
    }

    /**
     * Handles an empty batch by checkpointing the replay ID without publishing.
     * Requirement 4.1: Empty batches advance the replay ID.
     */
    private void handleEmptyBatch(EventBatch batch) {
        log.debug("Empty batch for org='{}', topic='{}'. Checkpointing replay ID.",
                batch.getOrg(), batch.getSalesforceTopic());
        try {
            replayStore.checkpoint(batch.getOrg(), batch.getSalesforceTopic(), batch.getReplayId());
            circuitBreaker.recordSuccess(batch.getOrg());
        } catch (CheckpointException e) {
            handleCheckpointFailure(batch.getOrg(), batch.getSalesforceTopic(), e);
        }
    }

    /**
     * Handles a non-empty event batch: route → publish → checkpoint.
     */
    private void handleEventBatch(EventBatch batch) {
        String org = batch.getOrg();
        String topic = batch.getSalesforceTopic();

        // Check if topic has a Kafka mapping
        Optional<String> kafkaTopic = topicRouter.getKafkaTopic(org, topic);
        if (kafkaTopic.isEmpty()) {
            log.warn("No Kafka mapping for org='{}', topic='{}'. Discarding {} events without checkpointing.",
                    org, topic, batch.getEvents().size());
            return;
        }

        // Publish batch to Kafka (publisher handles checkpointing internally)
        PublishResult result = kafkaEventPublisher.publishBatch(batch);

        if (result.isSuccess()) {
            log.debug("Successfully published {} events for org='{}', topic='{}'",
                    result.getPublishedCount(), org, topic);
            circuitBreaker.recordSuccess(org);
        } else {
            Exception error = result.getError().orElse(new RuntimeException("Unknown publish failure"));
            log.error("Failed to publish batch for org='{}', topic='{}'", org, topic, error);

            // Check if the failure was a checkpoint failure (fail-fast)
            if (error instanceof CheckpointException) {
                handleCheckpointFailure(org, topic, (CheckpointException) error);
            } else {
                circuitBreaker.recordFailure(org, error);
            }
        }
    }

    /**
     * Fail-fast behavior on checkpoint failure:
     * 1. Stop the subscription for the affected org/topic
     * 2. Open the circuit breaker
     * 3. Log ERROR with context
     * 
     * Requirements: 3.4, 3.5, 4.2
     */
    private void handleCheckpointFailure(String org, String topic, CheckpointException error) {
        log.error("Checkpoint failure for org='{}', topic='{}'. Stopping subscription and opening circuit.",
                org, topic, error);

        // Stop the subscription immediately
        pubSubConnector.stopSubscription(org, topic);
        activeSubscriptions.remove(subscriptionKey(org, topic));

        // Record failure to open circuit breaker
        circuitBreaker.recordFailure(org, error);
    }

    /**
     * Returns the number of active subscriptions.
     */
    public int getActiveSubscriptionCount() {
        return (int) activeSubscriptions.values().stream()
                .filter(Subscription::isActive)
                .count();
    }

    /**
     * Returns whether a subscription is active for the given org and topic.
     */
    public boolean isSubscriptionActive(String org, String topic) {
        Subscription sub = activeSubscriptions.get(subscriptionKey(org, topic));
        return sub != null && sub.isActive();
    }

    private static String subscriptionKey(String org, String topic) {
        return org + ":" + topic;
    }
}
