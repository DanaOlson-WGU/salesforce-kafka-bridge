package com.example.bridge.pubsub;

import com.example.bridge.config.OrgConfig;
import com.example.bridge.config.RetryConfig;
import com.example.bridge.config.SalesforceProperties;
import com.example.bridge.model.ConnectionStatus;
import com.example.bridge.model.EventBatch;
import com.example.bridge.model.ReplayId;
import com.example.bridge.replay.ReplayStore;
import com.example.bridge.resilience.CircuitBreaker;
import com.example.bridge.resilience.CircuitOpenException;
import com.example.bridge.resilience.CircuitState;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * gRPC-based implementation of {@link PubSubConnector} that manages Salesforce
 * Pub/Sub API subscriptions.
 *
 * <p>Key behaviors:
 * <ul>
 *   <li>Maintains separate gRPC channels per org for connection isolation</li>
 *   <li>Subscribes to configured topics with replay ID from ReplayStore (or earliest if none)</li>
 *   <li>Handles gRPC status codes with exponential backoff retry</li>
 *   <li>Coordinates with CircuitBreaker for failure handling</li>
 *   <li>Processes incoming event batches including empty batches</li>
 * </ul>
 */
public class GrpcPubSubConnector implements PubSubConnector {

    private static final Logger log = LoggerFactory.getLogger(GrpcPubSubConnector.class);

    private final SalesforceProperties salesforceProperties;
    private final SalesforceAuthService authService;
    private final ReplayStore replayStore;
    private final CircuitBreaker circuitBreaker;
    private final GrpcStreamAdapter streamAdapter;
    private final Consumer<EventBatch> batchProcessor;

    private final Map<String, Subscription> activeSubscriptions = new ConcurrentHashMap<>();
    private final Map<String, GrpcStreamAdapter.StreamHandle> activeStreams = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> retryCounters = new ConcurrentHashMap<>();

    public GrpcPubSubConnector(SalesforceProperties salesforceProperties,
                               SalesforceAuthService authService,
                               ReplayStore replayStore,
                               CircuitBreaker circuitBreaker,
                               GrpcStreamAdapter streamAdapter,
                               Consumer<EventBatch> batchProcessor) {
        this.salesforceProperties = salesforceProperties;
        this.authService = authService;
        this.replayStore = replayStore;
        this.circuitBreaker = circuitBreaker;
        this.streamAdapter = streamAdapter;
        this.batchProcessor = batchProcessor;
    }

    /**
     * Initializes gRPC channels for all configured orgs at startup.
     */
    public void initialize() {
        for (Map.Entry<String, OrgConfig> entry : salesforceProperties.getOrgs().entrySet()) {
            String orgId = entry.getKey();
            OrgConfig orgConfig = entry.getValue();
            if (orgConfig.isEnabled()) {
                log.info("Creating gRPC channel for org '{}'", orgId);
                streamAdapter.createChannel(orgId, orgConfig.getPubsubUrl());
            }
        }
    }

    @Override
    public Subscription subscribe(String org, String topic, ReplayId replayId) {
        String key = subscriptionKey(org, topic);
        log.info("Subscribing to org='{}', topic='{}', replayId={}", org, topic, replayId);

        // Resolve replay ID: use provided, or look up from store, or use null (earliest)
        ReplayId effectiveReplayId = resolveReplayId(org, topic, replayId);

        Subscription subscription = new Subscription(org, topic, batchProcessor, () -> cancelStream(key));
        activeSubscriptions.put(key, subscription);
        retryCounters.put(key, new AtomicInteger(0));

        startStream(org, topic, effectiveReplayId, subscription);

        return subscription;
    }

    @Override
    public void stopSubscription(String org, String topic) {
        String key = subscriptionKey(org, topic);
        log.info("Stopping subscription for org='{}', topic='{}'", org, topic);

        Subscription subscription = activeSubscriptions.remove(key);
        if (subscription != null) {
            subscription.stop();
        }
        cancelStream(key);
        retryCounters.remove(key);
    }

    @Override
    public boolean reconnect(String org) {
        OrgConfig orgConfig = salesforceProperties.getOrgs().get(org);
        if (orgConfig == null) {
            log.error("Cannot reconnect: no configuration found for org '{}'", org);
            return false;
        }

        log.info("Attempting reconnection for org '{}'", org);

        try {
            // Refresh the auth token
            authService.invalidateToken(org);
            authService.getAccessToken(org);

            // Recreate the gRPC channel
            streamAdapter.shutdownChannel(org);
            streamAdapter.createChannel(org, orgConfig.getPubsubUrl());

            // Re-subscribe to all topics for this org
            for (String topic : orgConfig.getTopics()) {
                String key = subscriptionKey(org, topic);
                Subscription existing = activeSubscriptions.get(key);
                if (existing != null && !existing.isActive()) {
                    activeSubscriptions.remove(key);
                }

                Optional<ReplayId> lastReplayId = replayStore.getLastReplayId(org, topic);
                ReplayId replayId = lastReplayId.orElse(null);

                Subscription subscription = new Subscription(org, topic, batchProcessor, () -> cancelStream(key));
                activeSubscriptions.put(key, subscription);
                retryCounters.put(key, new AtomicInteger(0));

                startStream(org, topic, replayId, subscription);
            }

            circuitBreaker.recordSuccess(org);
            log.info("Successfully reconnected org '{}'", org);
            return true;

        } catch (Exception e) {
            log.error("Reconnection failed for org '{}'", org, e);
            circuitBreaker.recordFailure(org, e);
            return false;
        }
    }

    @Override
    public ConnectionStatus getStatus(String org) {
        OrgConfig orgConfig = salesforceProperties.getOrgs().get(org);
        if (orgConfig == null) {
            return ConnectionStatus.down("Unknown org: " + org);
        }

        if (!orgConfig.isEnabled()) {
            return ConnectionStatus.up("Org disabled by configuration");
        }

        CircuitState circuitState = circuitBreaker.getState(org);
        if (circuitState == CircuitState.OPEN) {
            return ConnectionStatus.down("Circuit breaker OPEN for org: " + org);
        }

        boolean connected = streamAdapter.isConnected(org);
        if (!connected) {
            return ConnectionStatus.down("gRPC channel not connected for org: " + org);
        }

        // Check if all expected subscriptions are active
        long activeCount = orgConfig.getTopics().stream()
                .map(topic -> subscriptionKey(org, topic))
                .map(activeSubscriptions::get)
                .filter(sub -> sub != null && sub.isActive())
                .count();

        int expectedCount = orgConfig.getTopics().size();
        if (activeCount < expectedCount) {
            return ConnectionStatus.down(
                    String.format("Only %d of %d subscriptions active for org: %s",
                            activeCount, expectedCount, org));
        }

        return ConnectionStatus.up("All subscriptions active for org: " + org);
    }

    /**
     * Shuts down all subscriptions and channels. Called during application shutdown.
     */
    public void shutdown() {
        log.info("Shutting down PubSubConnector");
        for (Map.Entry<String, Subscription> entry : activeSubscriptions.entrySet()) {
            entry.getValue().stop();
        }
        activeSubscriptions.clear();
        activeStreams.clear();
        retryCounters.clear();

        for (String org : salesforceProperties.getOrgs().keySet()) {
            streamAdapter.shutdownChannel(org);
        }
    }

    /**
     * Returns the active subscription for the given org and topic, if any.
     */
    public Optional<Subscription> getSubscription(String org, String topic) {
        return Optional.ofNullable(activeSubscriptions.get(subscriptionKey(org, topic)));
    }

    private ReplayId resolveReplayId(String org, String topic, ReplayId provided) {
        if (provided != null) {
            log.debug("Using provided replay ID for org='{}', topic='{}'", org, topic);
            return provided;
        }

        Optional<ReplayId> stored = replayStore.getLastReplayId(org, topic);
        if (stored.isPresent()) {
            log.info("Resuming from stored replay ID for org='{}', topic='{}'", org, topic);
            return stored.get();
        }

        log.info("No replay ID found for org='{}', topic='{}'. Subscribing from earliest.", org, topic);
        return null;
    }

    private void startStream(String org, String topic, ReplayId replayId, Subscription subscription) {
        String key = subscriptionKey(org, topic);

        try {
            String accessToken = authService.getAccessToken(org);
            String instanceUrl = authService.getInstanceUrl(org);

            GrpcStreamAdapter.StreamHandle handle = streamAdapter.subscribe(
                    org, topic, replayId, accessToken, instanceUrl,
                    batch -> onBatchReceived(subscription, batch),
                    error -> onStreamError(org, topic, error)
            );

            activeStreams.put(key, handle);
            log.info("Stream started for org='{}', topic='{}'", org, topic);

        } catch (StatusRuntimeException e) {
            handleGrpcError(org, topic, e);
        } catch (SalesforceAuthException e) {
            log.error("Authentication failed for org='{}', topic='{}'", org, topic, e);
            circuitBreaker.recordFailure(org, e);
        } catch (Exception e) {
            log.error("Failed to start stream for org='{}', topic='{}'", org, topic, e);
            circuitBreaker.recordFailure(org, e);
        }
    }

    private void onBatchReceived(Subscription subscription, EventBatch batch) {
        if (!subscription.isActive()) {
            return;
        }

        String key = subscription.getKey();
        // Reset retry counter on successful batch receipt
        AtomicInteger counter = retryCounters.get(key);
        if (counter != null) {
            counter.set(0);
        }

        try {
            circuitBreaker.execute(subscription.getOrg(), () -> {
                subscription.deliverBatch(batch);
                return null;
            });
            circuitBreaker.recordSuccess(subscription.getOrg());
        } catch (CircuitOpenException e) {
            log.warn("Circuit open for org='{}', dropping batch for topic='{}'",
                    subscription.getOrg(), subscription.getTopic());
        } catch (Exception e) {
            log.error("Error processing batch for org='{}', topic='{}'",
                    subscription.getOrg(), subscription.getTopic(), e);
            circuitBreaker.recordFailure(subscription.getOrg(), e);
        }
    }

    private void onStreamError(String org, String topic, Throwable error) {
        String key = subscriptionKey(org, topic);
        log.error("Stream error for org='{}', topic='{}'", org, topic, error);

        if (error instanceof StatusRuntimeException sre) {
            handleGrpcError(org, topic, sre);
        } else {
            circuitBreaker.recordFailure(org, error instanceof Exception ex ? ex : new RuntimeException(error));
            attemptRetry(org, topic);
        }
    }

    void handleGrpcError(String org, String topic, StatusRuntimeException e) {
        Status.Code code = e.getStatus().getCode();
        log.warn("gRPC error for org='{}', topic='{}': status={}, description={}",
                org, topic, code, e.getStatus().getDescription());

        switch (code) {
            case UNAUTHENTICATED -> handleUnauthenticated(org, topic);
            case INVALID_ARGUMENT -> handleInvalidArgument(org, topic, e);
            case UNAVAILABLE, DEADLINE_EXCEEDED, RESOURCE_EXHAUSTED -> {
                circuitBreaker.recordFailure(org, e);
                attemptRetry(org, topic);
            }
            default -> {
                log.error("Unhandled gRPC status code {} for org='{}', topic='{}'", code, org, topic);
                circuitBreaker.recordFailure(org, e);
                attemptRetry(org, topic);
            }
        }
    }

    private void handleUnauthenticated(String org, String topic) {
        log.info("Token expired for org='{}'. Refreshing and retrying.", org);
        authService.invalidateToken(org);
        try {
            String key = subscriptionKey(org, topic);
            Subscription subscription = activeSubscriptions.get(key);
            if (subscription != null && subscription.isActive()) {
                Optional<ReplayId> lastReplayId = replayStore.getLastReplayId(org, topic);
                startStream(org, topic, lastReplayId.orElse(null), subscription);
            }
        } catch (Exception retryError) {
            log.error("Failed to re-authenticate for org='{}'", org, retryError);
            circuitBreaker.recordFailure(org, retryError);
        }
    }

    private void handleInvalidArgument(String org, String topic, StatusRuntimeException e) {
        log.error("Invalid argument for org='{}', topic='{}'. This is a configuration error: {}",
                org, topic, e.getStatus().getDescription());
        stopSubscription(org, topic);
    }

    private void attemptRetry(String org, String topic) {
        String key = subscriptionKey(org, topic);
        RetryConfig retryConfig = salesforceProperties.getRetry();
        AtomicInteger counter = retryCounters.get(key);

        if (counter == null) {
            return;
        }

        int attempt = counter.incrementAndGet();
        if (attempt > retryConfig.getMaxAttempts()) {
            log.error("Max retry attempts ({}) exceeded for org='{}', topic='{}'. Opening circuit breaker.",
                    retryConfig.getMaxAttempts(), org, topic);
            circuitBreaker.recordFailure(org,
                    new PubSubException("Max retries exceeded", Status.Code.UNAVAILABLE));
            stopSubscription(org, topic);
            return;
        }

        long backoffMs = calculateBackoff(attempt, retryConfig);
        log.info("Retry attempt {}/{} for org='{}', topic='{}' in {}ms",
                attempt, retryConfig.getMaxAttempts(), org, topic, backoffMs);

        // Schedule retry with backoff
        scheduleRetry(org, topic, backoffMs);
    }

    long calculateBackoff(int attempt, RetryConfig retryConfig) {
        double interval = retryConfig.getInitialIntervalMs() *
                Math.pow(retryConfig.getMultiplier(), attempt - 1);
        return Math.min((long) interval, retryConfig.getMaxIntervalMs());
    }

    private void scheduleRetry(String org, String topic, long delayMs) {
        Thread retryThread = new Thread(() -> {
            try {
                Thread.sleep(delayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }

            String key = subscriptionKey(org, topic);
            Subscription subscription = activeSubscriptions.get(key);
            if (subscription != null && subscription.isActive()) {
                Optional<ReplayId> lastReplayId = replayStore.getLastReplayId(org, topic);
                startStream(org, topic, lastReplayId.orElse(null), subscription);
            }
        }, "pubsub-retry-" + org + "-" + topic);
        retryThread.setDaemon(true);
        retryThread.start();
    }

    private void cancelStream(String key) {
        GrpcStreamAdapter.StreamHandle handle = activeStreams.remove(key);
        if (handle != null && handle.isActive()) {
            handle.cancel();
        }
    }

    static String subscriptionKey(String org, String topic) {
        return org + ":" + topic;
    }
}
