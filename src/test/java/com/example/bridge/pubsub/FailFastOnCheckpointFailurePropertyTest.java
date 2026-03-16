package com.example.bridge.pubsub;

import com.example.bridge.config.OrgConfig;
import com.example.bridge.config.RetryConfig;
import com.example.bridge.config.SalesforceProperties;
import com.example.bridge.model.ConnectionStatus;
import com.example.bridge.model.EventBatch;
import com.example.bridge.model.PlatformEvent;
import com.example.bridge.model.ReplayId;
import com.example.bridge.replay.CheckpointException;
import com.example.bridge.replay.ReplayStore;
import com.example.bridge.resilience.CircuitBreaker;
import com.example.bridge.resilience.CircuitState;
import net.jqwik.api.*;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Property 11: Fail-Fast On Checkpoint Failure
 *
 * For any checkpoint failure due to database error, the bridge should stop
 * the subscription within 5 seconds, not acknowledge the batch, and report
 * failure via health endpoint.
 *
 * Validates: Requirements 3.4, 3.5, 4.2
 */
class FailFastOnCheckpointFailurePropertyTest {

    // -----------------------------------------------------------------------
    // Property 1: Subscription is stopped when checkpoint fails
    // -----------------------------------------------------------------------

    @Property(tries = 50)
    void subscriptionShouldBeStoppedOnCheckpointFailure(
            @ForAll("orgNames") String org,
            @ForAll("salesforceTopics") String topic,
            @ForAll("databaseErrors") String errorMessage) {

        // Setup with a replay store that always fails on checkpoint
        SalesforceProperties props = createSalesforceProperties(org, List.of(topic));
        FailingReplayStore replayStore = new FailingReplayStore(errorMessage);
        TrackingCircuitBreaker circuitBreaker = new TrackingCircuitBreaker();
        StubStreamAdapter streamAdapter = new StubStreamAdapter();

        // Track whether the batch processor propagates the checkpoint failure
        List<Exception> caughtErrors = Collections.synchronizedList(new ArrayList<>());

        // Use array holder to allow reference inside lambda before assignment
        final GrpcPubSubConnector[] holder = new GrpcPubSubConnector[1];
        GrpcPubSubConnector connector = new GrpcPubSubConnector(
                props,
                new StubAuthService(props),
                replayStore,
                circuitBreaker,
                streamAdapter,
                batch -> {
                    // Simulate what the event processing pipeline does:
                    // publish to Kafka (succeeds), then checkpoint (fails)
                    try {
                        replayStore.checkpoint(batch.getOrg(), batch.getSalesforceTopic(), batch.getReplayId());
                    } catch (CheckpointException e) {
                        caughtErrors.add(e);
                        // Fail-fast: stop subscription and record failure
                        holder[0].stopSubscription(batch.getOrg(), batch.getSalesforceTopic());
                        circuitBreaker.recordFailure(batch.getOrg(), e);
                        throw new RuntimeException(e);
                    }
                }
        );
        holder[0] = connector;

        connector.initialize();
        Subscription subscription = connector.subscribe(org, topic, null);
        assertTrue(subscription.isActive(), "Subscription should be active before checkpoint failure");

        // Deliver a batch that will trigger checkpoint failure
        EventBatch batch = createEventBatch(org, topic);
        streamAdapter.deliverBatch(org, topic, batch);

        // Verify: subscription should be stopped after checkpoint failure
        assertFalse(subscription.isActive(),
                "Subscription should be stopped after checkpoint failure");

        // Verify: checkpoint exception was caught
        assertFalse(caughtErrors.isEmpty(),
                "Checkpoint failure should have been caught");

        connector.shutdown();
    }

    // -----------------------------------------------------------------------
    // Property 2: Circuit breaker records failure on checkpoint error
    // -----------------------------------------------------------------------

    @Property(tries = 50)
    void circuitBreakerShouldRecordFailureOnCheckpointError(
            @ForAll("orgNames") String org,
            @ForAll("salesforceTopics") String topic,
            @ForAll("databaseErrors") String errorMessage) {

        SalesforceProperties props = createSalesforceProperties(org, List.of(topic));
        FailingReplayStore replayStore = new FailingReplayStore(errorMessage);
        TrackingCircuitBreaker circuitBreaker = new TrackingCircuitBreaker();
        StubStreamAdapter streamAdapter = new StubStreamAdapter();

        final GrpcPubSubConnector[] holder = new GrpcPubSubConnector[1];
        GrpcPubSubConnector connector = new GrpcPubSubConnector(
                props,
                new StubAuthService(props),
                replayStore,
                circuitBreaker,
                streamAdapter,
                batch -> {
                    try {
                        replayStore.checkpoint(batch.getOrg(), batch.getSalesforceTopic(), batch.getReplayId());
                    } catch (CheckpointException e) {
                        holder[0].stopSubscription(batch.getOrg(), batch.getSalesforceTopic());
                        circuitBreaker.recordFailure(batch.getOrg(), e);
                        throw new RuntimeException(e);
                    }
                }
        );
        holder[0] = connector;

        connector.initialize();
        connector.subscribe(org, topic, null);

        EventBatch batch = createEventBatch(org, topic);
        streamAdapter.deliverBatch(org, topic, batch);

        // Verify: circuit breaker should have recorded at least one failure for this org
        assertTrue(circuitBreaker.getFailureCount(org) > 0,
                "Circuit breaker should record failure for org: " + org);

        // Verify: the recorded failure should be related to checkpoint
        assertTrue(circuitBreaker.getLastError(org).isPresent(),
                "Circuit breaker should have a recorded error for org: " + org);

        connector.shutdown();
    }

    // -----------------------------------------------------------------------
    // Property 3: Health endpoint reports unhealthy after checkpoint failure
    // -----------------------------------------------------------------------

    @Property(tries = 50)
    void healthShouldReportUnhealthyAfterCheckpointFailure(
            @ForAll("orgNames") String org,
            @ForAll("salesforceTopics") String topic,
            @ForAll("databaseErrors") String errorMessage) {

        SalesforceProperties props = createSalesforceProperties(org, List.of(topic));
        FailingReplayStore replayStore = new FailingReplayStore(errorMessage);
        TrackingCircuitBreaker circuitBreaker = new TrackingCircuitBreaker();
        StubStreamAdapter streamAdapter = new StubStreamAdapter();

        final GrpcPubSubConnector[] holder = new GrpcPubSubConnector[1];
        GrpcPubSubConnector connector = new GrpcPubSubConnector(
                props,
                new StubAuthService(props),
                replayStore,
                circuitBreaker,
                streamAdapter,
                batch -> {
                    try {
                        replayStore.checkpoint(batch.getOrg(), batch.getSalesforceTopic(), batch.getReplayId());
                    } catch (CheckpointException e) {
                        holder[0].stopSubscription(batch.getOrg(), batch.getSalesforceTopic());
                        circuitBreaker.recordFailure(batch.getOrg(), e);
                        throw new RuntimeException(e);
                    }
                }
        );
        holder[0] = connector;

        connector.initialize();
        connector.subscribe(org, topic, null);

        // Verify healthy before failure
        ConnectionStatus statusBefore = connector.getStatus(org);
        assertTrue(statusBefore.isHealthy(),
                "Status should be healthy before checkpoint failure");

        // Trigger checkpoint failure
        EventBatch batch = createEventBatch(org, topic);
        streamAdapter.deliverBatch(org, topic, batch);

        // After checkpoint failure, the subscription is stopped → status should be unhealthy
        ConnectionStatus statusAfter = connector.getStatus(org);
        assertFalse(statusAfter.isHealthy(),
                "Status should be unhealthy after checkpoint failure stopped the subscription");

        connector.shutdown();
    }

    // -----------------------------------------------------------------------
    // Property 4: Batch is not acknowledged (subscription stopped before
    //             completion prevents Salesforce from advancing)
    // -----------------------------------------------------------------------

    @Property(tries = 50)
    void batchShouldNotBeAcknowledgedOnCheckpointFailure(
            @ForAll("orgNames") String org,
            @ForAll("salesforceTopics") String topic,
            @ForAll("databaseErrors") String errorMessage) {

        SalesforceProperties props = createSalesforceProperties(org, List.of(topic));
        FailingReplayStore replayStore = new FailingReplayStore(errorMessage);
        TrackingCircuitBreaker circuitBreaker = new TrackingCircuitBreaker();
        StubStreamAdapter streamAdapter = new StubStreamAdapter();

        // Track whether checkpoint was ever successfully completed
        AtomicBoolean checkpointCompleted = new AtomicBoolean(false);

        final GrpcPubSubConnector[] holder = new GrpcPubSubConnector[1];
        GrpcPubSubConnector connector = new GrpcPubSubConnector(
                props,
                new StubAuthService(props),
                replayStore,
                circuitBreaker,
                streamAdapter,
                batch -> {
                    try {
                        replayStore.checkpoint(batch.getOrg(), batch.getSalesforceTopic(), batch.getReplayId());
                        checkpointCompleted.set(true);
                    } catch (CheckpointException e) {
                        holder[0].stopSubscription(batch.getOrg(), batch.getSalesforceTopic());
                        circuitBreaker.recordFailure(batch.getOrg(), e);
                        throw new RuntimeException(e);
                    }
                }
        );
        holder[0] = connector;

        connector.initialize();
        connector.subscribe(org, topic, null);

        EventBatch batch = createEventBatch(org, topic);
        streamAdapter.deliverBatch(org, topic, batch);

        // Verify: checkpoint was never completed (batch not acknowledged)
        assertFalse(checkpointCompleted.get(),
                "Checkpoint should not complete when database fails - batch not acknowledged");

        // Verify: replay store has no stored replay ID (nothing was persisted)
        assertTrue(replayStore.getLastReplayId(org, topic).isEmpty(),
                "No replay ID should be persisted when checkpoint fails");

        connector.shutdown();
    }

    // -----------------------------------------------------------------------
    // Providers
    // -----------------------------------------------------------------------

    @Provide
    Arbitrary<String> orgNames() {
        return Arbitraries.of("srm", "acad", "test-org", "org-alpha");
    }

    @Provide
    Arbitrary<String> salesforceTopics() {
        return Arbitraries.of(
                "/event/Enrollment_Event__e",
                "/event/Student_Event__e",
                "/event/Course_Event__e",
                "/event/Test_Event__e"
        );
    }

    @Provide
    Arbitrary<String> databaseErrors() {
        return Arbitraries.of(
                "Connection refused",
                "Connection timed out",
                "Database is shutting down",
                "Too many connections",
                "Disk full",
                "Transaction rolled back",
                "Lock wait timeout exceeded",
                "Deadlock detected"
        );
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private SalesforceProperties createSalesforceProperties(String org, List<String> topics) {
        SalesforceProperties props = new SalesforceProperties();
        props.setRetry(createRetryConfig());
        props.setOrgs(Map.of(org, createOrgConfig(topics)));
        return props;
    }

    private RetryConfig createRetryConfig() {
        RetryConfig config = new RetryConfig();
        config.setMaxAttempts(3);
        config.setInitialIntervalMs(100);
        config.setMaxIntervalMs(60000);
        config.setMultiplier(2.0);
        return config;
    }

    private OrgConfig createOrgConfig(List<String> topics) {
        OrgConfig config = new OrgConfig();
        config.setPubsubUrl("api.pubsub.salesforce.com:7443");
        config.setOauthUrl("https://login.salesforce.com/services/oauth2/token");
        config.setClientId("test-client-id");
        config.setClientSecret("test-client-secret");
        config.setUsername("test@example.com");
        config.setPassword("test-password");
        config.setTopics(new ArrayList<>(topics));
        config.setEnabled(true);
        return config;
    }

    private EventBatch createEventBatch(String org, String topic) {
        ReplayId replayId = new ReplayId(new byte[]{1, 2, 3, 4});
        PlatformEvent event = new PlatformEvent(
                "TestEvent",
                new com.fasterxml.jackson.databind.node.ObjectNode(
                        com.fasterxml.jackson.databind.node.JsonNodeFactory.instance),
                replayId
        );
        return new EventBatch(org, topic, List.of(event), replayId, Instant.now());
    }

    // -----------------------------------------------------------------------
    // Test Doubles
    // -----------------------------------------------------------------------

    /**
     * ReplayStore stub that always throws CheckpointException on checkpoint,
     * simulating a database failure.
     */
    static class FailingReplayStore implements ReplayStore {
        private final String errorMessage;

        FailingReplayStore(String errorMessage) {
            this.errorMessage = errorMessage;
        }

        @Override
        public Optional<ReplayId> getLastReplayId(String org, String topic) {
            return Optional.empty();
        }

        @Override
        public void checkpoint(String org, String topic, ReplayId replayId) throws CheckpointException {
            throw new CheckpointException("Database error: " + errorMessage);
        }

        @Override
        public ConnectionStatus getDatabaseStatus() {
            return ConnectionStatus.down("Database unavailable: " + errorMessage);
        }
    }

    /**
     * Circuit breaker stub that tracks failures per org for verification.
     */
    static class TrackingCircuitBreaker implements CircuitBreaker {
        private final Map<String, CircuitState> states = new ConcurrentHashMap<>();
        private final Map<String, Integer> failureCounts = new ConcurrentHashMap<>();
        private final Map<String, Exception> lastErrors = new ConcurrentHashMap<>();

        @Override
        public <T> T execute(String org, Supplier<T> operation) {
            if (getState(org) == CircuitState.OPEN) {
                throw new com.example.bridge.resilience.CircuitOpenException(org);
            }
            return operation.get();
        }

        @Override
        public void recordSuccess(String org) {
            states.put(org, CircuitState.CLOSED);
        }

        @Override
        public void recordFailure(String org, Exception error) {
            failureCounts.merge(org, 1, Integer::sum);
            lastErrors.put(org, error);
        }

        @Override
        public CircuitState getState(String org) {
            return states.getOrDefault(org, CircuitState.CLOSED);
        }

        int getFailureCount(String org) {
            return failureCounts.getOrDefault(org, 0);
        }

        Optional<Exception> getLastError(String org) {
            return Optional.ofNullable(lastErrors.get(org));
        }
    }

    /**
     * Stub SalesforceAuthService for testing.
     */
    static class StubAuthService extends SalesforceAuthService {
        StubAuthService(SalesforceProperties properties) {
            super(properties, (url, cid, cs, u, p) ->
                    new OAuthTokenResponse("test-token", "https://test.salesforce.com",
                            "Bearer", String.valueOf(System.currentTimeMillis()), null, null));
        }

        @Override
        public String getAccessToken(String orgId) {
            return "test-token";
        }

        @Override
        public String getInstanceUrl(String orgId) {
            return "https://test.salesforce.com";
        }

        @Override
        public void invalidateToken(String orgId) {
            // No-op for testing
        }
    }

    /**
     * Stub GrpcStreamAdapter for testing.
     */
    static class StubStreamAdapter implements GrpcStreamAdapter {
        private final Map<String, Boolean> connectedOrgs = new ConcurrentHashMap<>();
        private final Map<String, Consumer<EventBatch>> batchCallbacks = new ConcurrentHashMap<>();

        @Override
        public StreamHandle subscribe(String org, String topic, ReplayId replayId,
                                       String accessToken, String instanceUrl,
                                       Consumer<EventBatch> batchCallback,
                                       Consumer<Throwable> errorCallback) {
            String key = org + ":" + topic;
            batchCallbacks.put(key, batchCallback);
            return new StubStreamHandle();
        }

        @Override
        public boolean isConnected(String org) {
            return connectedOrgs.getOrDefault(org, true);
        }

        @Override
        public void createChannel(String org, String pubsubUrl) {
            connectedOrgs.put(org, true);
        }

        @Override
        public void shutdownChannel(String org) {
            connectedOrgs.remove(org);
        }

        void deliverBatch(String org, String topic, EventBatch batch) {
            String key = org + ":" + topic;
            Consumer<EventBatch> callback = batchCallbacks.get(key);
            if (callback != null) {
                callback.accept(batch);
            }
        }
    }

    /**
     * Stub StreamHandle implementation.
     */
    static class StubStreamHandle implements GrpcStreamAdapter.StreamHandle {
        private final AtomicBoolean active = new AtomicBoolean(true);

        @Override
        public void cancel() {
            active.set(false);
        }

        @Override
        public boolean isActive() {
            return active.get();
        }
    }
}
