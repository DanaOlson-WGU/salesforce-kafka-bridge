package com.example.bridge.pubsub;

import com.example.bridge.config.OrgConfig;
import com.example.bridge.config.RetryConfig;
import com.example.bridge.config.SalesforceProperties;
import com.example.bridge.model.ConnectionStatus;
import com.example.bridge.model.EventBatch;
import com.example.bridge.model.ReplayId;
import com.example.bridge.replay.ReplayStore;
import com.example.bridge.resilience.CircuitBreaker;
import com.example.bridge.resilience.CircuitState;
import net.jqwik.api.*;
import net.jqwik.api.constraints.IntRange;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Property 2: Active Subscription Maintenance
 *
 * For any configured Salesforce topic, while the bridge is running and
 * circuit breaker is closed, an active subscription stream should exist.
 *
 * Validates: Requirement 1.4
 */
class ActiveSubscriptionMaintenancePropertyTest {

    // -----------------------------------------------------------------------
    // Property: Active subscription exists for each configured topic when
    //           circuit breaker is closed
    // -----------------------------------------------------------------------

    @Property(tries = 50)
    void activeSubscriptionExistsForEachConfiguredTopic(
            @ForAll("orgNames") String org,
            @ForAll("topicLists") List<String> topics) {

        // Setup
        SalesforceProperties props = createSalesforceProperties(org, topics);
        StubReplayStore replayStore = new StubReplayStore();
        StubCircuitBreaker circuitBreaker = new StubCircuitBreaker();
        StubStreamAdapter streamAdapter = new StubStreamAdapter();

        GrpcPubSubConnector connector = new GrpcPubSubConnector(
                props,
                new StubAuthService(props),
                replayStore,
                circuitBreaker,
                streamAdapter,
                batch -> {}
        );

        // Initialize and subscribe to all topics
        connector.initialize();
        for (String topic : topics) {
            connector.subscribe(org, topic, null);
        }

        // Verify: Each configured topic should have an active subscription
        for (String topic : topics) {
            Optional<Subscription> subscription = connector.getSubscription(org, topic);
            assertTrue(subscription.isPresent(),
                    "Subscription should exist for topic: " + topic);
            assertTrue(subscription.get().isActive(),
                    "Subscription should be active for topic: " + topic);
        }

        // Verify: Connection status should be healthy
        ConnectionStatus status = connector.getStatus(org);
        assertTrue(status.isHealthy(),
                "Connection status should be healthy when all subscriptions are active");

        // Cleanup
        connector.shutdown();
    }

    // -----------------------------------------------------------------------
    // Property: Subscription remains active while circuit breaker is closed
    // -----------------------------------------------------------------------

    @Property(tries = 50)
    void subscriptionRemainsActiveWhileCircuitClosed(
            @ForAll("orgNames") String org,
            @ForAll("salesforceTopics") String topic,
            @ForAll @IntRange(min = 1, max = 10) int batchCount) {

        // Setup
        SalesforceProperties props = createSalesforceProperties(org, List.of(topic));
        StubReplayStore replayStore = new StubReplayStore();
        StubCircuitBreaker circuitBreaker = new StubCircuitBreaker();
        StubStreamAdapter streamAdapter = new StubStreamAdapter();
        List<EventBatch> receivedBatches = Collections.synchronizedList(new ArrayList<>());

        GrpcPubSubConnector connector = new GrpcPubSubConnector(
                props,
                new StubAuthService(props),
                replayStore,
                circuitBreaker,
                streamAdapter,
                receivedBatches::add
        );

        connector.initialize();
        Subscription subscription = connector.subscribe(org, topic, null);

        // Simulate receiving multiple batches while circuit is closed
        for (int i = 0; i < batchCount; i++) {
            assertTrue(subscription.isActive(),
                    "Subscription should remain active after " + i + " batches");
            assertEquals(CircuitState.CLOSED, circuitBreaker.getState(org),
                    "Circuit breaker should remain closed");

            // Simulate batch delivery
            EventBatch batch = new EventBatch(org, topic, List.of(),
                    new ReplayId(new byte[]{(byte) i}), java.time.Instant.now());
            streamAdapter.deliverBatch(org, topic, batch);
        }

        // Verify subscription is still active
        assertTrue(subscription.isActive(),
                "Subscription should remain active after processing all batches");

        // Cleanup
        connector.shutdown();
    }

    // -----------------------------------------------------------------------
    // Property: Multiple orgs maintain independent active subscriptions
    // -----------------------------------------------------------------------

    @Property(tries = 30)
    void multipleOrgsHaveIndependentActiveSubscriptions(
            @ForAll("topicLists") List<String> topics) {

        String org1 = "srm";
        String org2 = "acad";

        // Setup with multiple orgs
        SalesforceProperties props = new SalesforceProperties();
        RetryConfig retryConfig = createRetryConfig();
        props.setRetry(retryConfig);

        Map<String, OrgConfig> orgs = new HashMap<>();
        orgs.put(org1, createOrgConfig(topics));
        orgs.put(org2, createOrgConfig(topics));
        props.setOrgs(orgs);

        StubReplayStore replayStore = new StubReplayStore();
        StubCircuitBreaker circuitBreaker = new StubCircuitBreaker();
        StubStreamAdapter streamAdapter = new StubStreamAdapter();

        GrpcPubSubConnector connector = new GrpcPubSubConnector(
                props,
                new StubAuthService(props),
                replayStore,
                circuitBreaker,
                streamAdapter,
                batch -> {}
        );

        connector.initialize();

        // Subscribe to all topics for both orgs
        for (String topic : topics) {
            connector.subscribe(org1, topic, null);
            connector.subscribe(org2, topic, null);
        }

        // Verify: Both orgs should have active subscriptions for all topics
        for (String topic : topics) {
            Optional<Subscription> sub1 = connector.getSubscription(org1, topic);
            Optional<Subscription> sub2 = connector.getSubscription(org2, topic);

            assertTrue(sub1.isPresent(), "Org1 should have subscription for: " + topic);
            assertTrue(sub2.isPresent(), "Org2 should have subscription for: " + topic);
            assertTrue(sub1.get().isActive(), "Org1 subscription should be active for: " + topic);
            assertTrue(sub2.get().isActive(), "Org2 subscription should be active for: " + topic);
        }

        // Verify: Both orgs should report healthy status
        assertTrue(connector.getStatus(org1).isHealthy(),
                "Org1 connection status should be healthy");
        assertTrue(connector.getStatus(org2).isHealthy(),
                "Org2 connection status should be healthy");

        // Cleanup
        connector.shutdown();
    }

    // -----------------------------------------------------------------------
    // Property: No active subscription when circuit breaker is open
    // -----------------------------------------------------------------------

    @Property(tries = 50)
    void statusReportsUnhealthyWhenCircuitOpen(
            @ForAll("orgNames") String org,
            @ForAll("salesforceTopics") String topic) {

        // Setup
        SalesforceProperties props = createSalesforceProperties(org, List.of(topic));
        StubReplayStore replayStore = new StubReplayStore();
        StubCircuitBreaker circuitBreaker = new StubCircuitBreaker();
        StubStreamAdapter streamAdapter = new StubStreamAdapter();

        GrpcPubSubConnector connector = new GrpcPubSubConnector(
                props,
                new StubAuthService(props),
                replayStore,
                circuitBreaker,
                streamAdapter,
                batch -> {}
        );

        connector.initialize();
        connector.subscribe(org, topic, null);

        // Open the circuit breaker
        circuitBreaker.forceOpen(org);

        // Verify: Status should report unhealthy when circuit is open
        ConnectionStatus status = connector.getStatus(org);
        assertFalse(status.isHealthy(),
                "Connection status should be unhealthy when circuit breaker is open");
        assertTrue(status.getDetail().contains("Circuit breaker OPEN"),
                "Status detail should indicate circuit breaker is open");

        // Cleanup
        connector.shutdown();
    }

    // -----------------------------------------------------------------------
    // Property: Subscription count matches configured topic count
    // -----------------------------------------------------------------------

    @Property(tries = 50)
    void subscriptionCountMatchesConfiguredTopicCount(
            @ForAll("orgNames") String org,
            @ForAll("topicLists") List<String> topics) {

        // Setup
        SalesforceProperties props = createSalesforceProperties(org, topics);
        StubReplayStore replayStore = new StubReplayStore();
        StubCircuitBreaker circuitBreaker = new StubCircuitBreaker();
        StubStreamAdapter streamAdapter = new StubStreamAdapter();

        GrpcPubSubConnector connector = new GrpcPubSubConnector(
                props,
                new StubAuthService(props),
                replayStore,
                circuitBreaker,
                streamAdapter,
                batch -> {}
        );

        connector.initialize();

        // Subscribe to all configured topics
        for (String topic : topics) {
            connector.subscribe(org, topic, null);
        }

        // Count active subscriptions
        long activeCount = topics.stream()
                .map(topic -> connector.getSubscription(org, topic))
                .filter(Optional::isPresent)
                .filter(opt -> opt.get().isActive())
                .count();

        assertEquals(topics.size(), activeCount,
                "Active subscription count should match configured topic count");

        // Cleanup
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
    Arbitrary<List<String>> topicLists() {
        return Arbitraries.of(
                "/event/Enrollment_Event__e",
                "/event/Student_Event__e",
                "/event/Course_Event__e"
        ).list().ofMinSize(1).ofMaxSize(3).uniqueElements();
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

    // -----------------------------------------------------------------------
    // Test Doubles
    // -----------------------------------------------------------------------

    /**
     * Stub implementation of ReplayStore for testing.
     */
    static class StubReplayStore implements ReplayStore {
        private final Map<String, ReplayId> store = new ConcurrentHashMap<>();

        @Override
        public Optional<ReplayId> getLastReplayId(String org, String topic) {
            return Optional.ofNullable(store.get(org + ":" + topic));
        }

        @Override
        public void checkpoint(String org, String topic, ReplayId replayId) {
            store.put(org + ":" + topic, replayId);
        }

        @Override
        public ConnectionStatus getDatabaseStatus() {
            return ConnectionStatus.up("Stub database healthy");
        }
    }

    /**
     * Stub implementation of CircuitBreaker for testing.
     */
    static class StubCircuitBreaker implements CircuitBreaker {
        private final Map<String, CircuitState> states = new ConcurrentHashMap<>();

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
            // For testing, we don't automatically open the circuit
        }

        @Override
        public CircuitState getState(String org) {
            return states.getOrDefault(org, CircuitState.CLOSED);
        }

        void forceOpen(String org) {
            states.put(org, CircuitState.OPEN);
        }
    }

    /**
     * Stub implementation of SalesforceAuthService for testing.
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
     * Stub implementation of GrpcStreamAdapter for testing.
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
