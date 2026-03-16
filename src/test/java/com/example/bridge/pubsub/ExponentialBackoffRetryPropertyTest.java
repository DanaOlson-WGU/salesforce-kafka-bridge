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
import net.jqwik.api.constraints.DoubleRange;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.LongRange;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Property 3: Exponential Backoff Retry
 *
 * For any gRPC connection loss, the bridge should attempt reconnection with
 * exponentially increasing intervals, where each interval is the previous
 * interval multiplied by the configured multiplier, up to the maximum
 * interval of 60 seconds.
 *
 * Validates: Requirements 1.5
 */
class ExponentialBackoffRetryPropertyTest {

    // -----------------------------------------------------------------------
    // Property 1: Backoff interval equals initialIntervalMs * multiplier^(attempt-1)
    //             capped at maxIntervalMs
    // -----------------------------------------------------------------------

    @Property(tries = 200)
    void backoffShouldEqualFormulaResult(
            @ForAll @IntRange(min = 1, max = 20) int attempt,
            @ForAll("retryConfigs") RetryConfig retryConfig) {

        GrpcPubSubConnector connector = createConnector(retryConfig);

        long actual = connector.calculateBackoff(attempt, retryConfig);
        double expected = retryConfig.getInitialIntervalMs()
                * Math.pow(retryConfig.getMultiplier(), attempt - 1);
        long expectedCapped = Math.min((long) expected, retryConfig.getMaxIntervalMs());

        assertEquals(expectedCapped, actual,
                String.format("Backoff for attempt=%d should be min(%d * %.1f^%d, %d) = %d",
                        attempt, retryConfig.getInitialIntervalMs(),
                        retryConfig.getMultiplier(), attempt - 1,
                        retryConfig.getMaxIntervalMs(), expectedCapped));
    }

    // -----------------------------------------------------------------------
    // Property 2: Backoff intervals are monotonically non-decreasing
    // -----------------------------------------------------------------------

    @Property(tries = 100)
    void backoffShouldBeMonotonicallyNonDecreasing(
            @ForAll("retryConfigs") RetryConfig retryConfig,
            @ForAll @IntRange(min = 2, max = 20) int maxAttempt) {

        GrpcPubSubConnector connector = createConnector(retryConfig);

        long previousBackoff = 0;
        for (int attempt = 1; attempt <= maxAttempt; attempt++) {
            long currentBackoff = connector.calculateBackoff(attempt, retryConfig);
            assertTrue(currentBackoff >= previousBackoff,
                    String.format("Backoff at attempt %d (%dms) should be >= previous (%dms)",
                            attempt, currentBackoff, previousBackoff));
            previousBackoff = currentBackoff;
        }
    }

    // -----------------------------------------------------------------------
    // Property 3: Backoff should never exceed maxIntervalMs
    // -----------------------------------------------------------------------

    @Property(tries = 200)
    void backoffShouldNeverExceedMaxInterval(
            @ForAll @IntRange(min = 1, max = 50) int attempt,
            @ForAll("retryConfigs") RetryConfig retryConfig) {

        GrpcPubSubConnector connector = createConnector(retryConfig);

        long backoff = connector.calculateBackoff(attempt, retryConfig);

        assertTrue(backoff <= retryConfig.getMaxIntervalMs(),
                String.format("Backoff %dms at attempt %d should not exceed maxIntervalMs %dms",
                        backoff, attempt, retryConfig.getMaxIntervalMs()));
    }

    // -----------------------------------------------------------------------
    // Property 4: After max retries exceeded, circuit breaker is notified
    //             and subscription is stopped
    // -----------------------------------------------------------------------

    @Property(tries = 50)
    void circuitBreakerShouldBeNotifiedAfterMaxRetries(
            @ForAll("orgNames") String org,
            @ForAll("salesforceTopics") String topic,
            @ForAll @IntRange(min = 1, max = 5) int maxAttempts) {

        RetryConfig retryConfig = new RetryConfig();
        retryConfig.setMaxAttempts(maxAttempts);
        retryConfig.setInitialIntervalMs(100);
        retryConfig.setMaxIntervalMs(60000);
        retryConfig.setMultiplier(2.0);

        SalesforceProperties props = createSalesforceProperties(org, List.of(topic), retryConfig);
        StubReplayStore replayStore = new StubReplayStore();
        StubCircuitBreaker circuitBreaker = new StubCircuitBreaker();
        // Use a non-retrying stream adapter so scheduleRetry threads don't interfere
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
        Subscription subscription = connector.subscribe(org, topic, null);

        // Simulate exceeding max retries by calling handleGrpcError repeatedly
        // Each call to handleGrpcError with UNAVAILABLE triggers attemptRetry
        // which increments the counter. After maxAttempts, it should stop.
        io.grpc.StatusRuntimeException unavailable =
                new io.grpc.StatusRuntimeException(io.grpc.Status.UNAVAILABLE);

        for (int i = 0; i <= maxAttempts; i++) {
            connector.handleGrpcError(org, topic, unavailable);
        }

        // Allow a brief moment for the stop to propagate
        // The subscription should be stopped after exceeding max retries
        assertFalse(subscription.isActive(),
                "Subscription should be stopped after exceeding max retries (" + maxAttempts + ")");

        // Circuit breaker should have recorded failures
        assertTrue(circuitBreaker.getFailureCount(org) > 0,
                "Circuit breaker should have recorded failures for org: " + org);
    }

    // -----------------------------------------------------------------------
    // Providers
    // -----------------------------------------------------------------------

    @Provide
    Arbitrary<RetryConfig> retryConfigs() {
        return Combinators.combine(
                Arbitraries.longs().between(100, 5000),       // initialIntervalMs
                Arbitraries.longs().between(10000, 60000),    // maxIntervalMs
                Arbitraries.doubles().between(1.5, 4.0)       // multiplier
        ).as((initial, max, mult) -> {
            RetryConfig config = new RetryConfig();
            config.setMaxAttempts(10);
            config.setInitialIntervalMs(initial);
            config.setMaxIntervalMs(max);
            config.setMultiplier(mult);
            return config;
        });
    }

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

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private GrpcPubSubConnector createConnector(RetryConfig retryConfig) {
        SalesforceProperties props = createSalesforceProperties("test-org",
                List.of("/event/Test_Event__e"), retryConfig);
        return new GrpcPubSubConnector(
                props,
                new StubAuthService(props),
                new StubReplayStore(),
                new StubCircuitBreaker(),
                new StubStreamAdapter(),
                batch -> {}
        );
    }

    private SalesforceProperties createSalesforceProperties(String org, List<String> topics,
                                                             RetryConfig retryConfig) {
        SalesforceProperties props = new SalesforceProperties();
        props.setRetry(retryConfig);
        props.setOrgs(Map.of(org, createOrgConfig(topics)));
        return props;
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
    // Test Doubles (same stub pattern as ActiveSubscriptionMaintenancePropertyTest)
    // -----------------------------------------------------------------------

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

    static class StubCircuitBreaker implements CircuitBreaker {
        private final Map<String, CircuitState> states = new ConcurrentHashMap<>();
        private final Map<String, Integer> failureCounts = new ConcurrentHashMap<>();

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
        }

        @Override
        public CircuitState getState(String org) {
            return states.getOrDefault(org, CircuitState.CLOSED);
        }

        int getFailureCount(String org) {
            return failureCounts.getOrDefault(org, 0);
        }
    }

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

    static class StubStreamAdapter implements GrpcStreamAdapter {
        private final Map<String, Boolean> connectedOrgs = new ConcurrentHashMap<>();

        @Override
        public StreamHandle subscribe(String org, String topic, ReplayId replayId,
                                       String accessToken, String instanceUrl,
                                       Consumer<EventBatch> batchCallback,
                                       Consumer<Throwable> errorCallback) {
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
    }

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
