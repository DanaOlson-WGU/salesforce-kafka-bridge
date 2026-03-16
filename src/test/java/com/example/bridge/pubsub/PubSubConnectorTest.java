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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class PubSubConnectorTest {

    private SalesforceProperties salesforceProperties;
    private ReplayStore replayStore;
    private CircuitBreaker circuitBreaker;
    private GrpcStreamAdapter streamAdapter;
    private GrpcPubSubConnector connector;
    private AtomicReference<EventBatch> receivedBatch;
    private StubAuthService stubAuthService;

    private static final String ORG = "srm";
    private static final String TOPIC = "/event/Enrollment_Event__e";

    @BeforeEach
    void setUp() {
        salesforceProperties = new SalesforceProperties();
        RetryConfig retryConfig = new RetryConfig();
        retryConfig.setMaxAttempts(3);
        retryConfig.setInitialIntervalMs(100);
        retryConfig.setMaxIntervalMs(60000);
        retryConfig.setMultiplier(2.0);
        salesforceProperties.setRetry(retryConfig);

        OrgConfig orgConfig = createOrgConfig();
        salesforceProperties.setOrgs(Map.of(ORG, orgConfig));

        stubAuthService = new StubAuthService(salesforceProperties);

        replayStore = mock(ReplayStore.class);
        circuitBreaker = mock(CircuitBreaker.class);
        streamAdapter = mock(GrpcStreamAdapter.class);
        when(streamAdapter.isConnected(ORG)).thenReturn(true);

        receivedBatch = new AtomicReference<>();

        connector = new GrpcPubSubConnector(
                salesforceProperties, stubAuthService, replayStore,
                circuitBreaker, streamAdapter, receivedBatch::set
        );
    }

    @Test
    void subscribe_usesLastReplayIdFromStore() {
        ReplayId storedReplayId = new ReplayId(new byte[]{1, 2, 3});
        when(replayStore.getLastReplayId(ORG, TOPIC)).thenReturn(Optional.of(storedReplayId));
        GrpcStreamAdapter.StreamHandle handle = newActiveHandle();
        when(streamAdapter.subscribe(anyString(), anyString(), any(), anyString(), anyString(), any(), any()))
                .thenReturn(handle);

        Subscription sub = connector.subscribe(ORG, TOPIC, null);

        assertNotNull(sub);
        assertTrue(sub.isActive());
        verify(streamAdapter).subscribe(eq(ORG), eq(TOPIC), eq(storedReplayId),
                eq("test-token"), eq("https://test.salesforce.com"), any(), any());
    }

    @Test
    void subscribe_usesEarliestWhenNoReplayIdExists() {
        when(replayStore.getLastReplayId(ORG, TOPIC)).thenReturn(Optional.empty());
        GrpcStreamAdapter.StreamHandle handle = newActiveHandle();
        when(streamAdapter.subscribe(anyString(), anyString(), any(), anyString(), anyString(), any(), any()))
                .thenReturn(handle);

        Subscription sub = connector.subscribe(ORG, TOPIC, null);

        assertNotNull(sub);
        assertTrue(sub.isActive());
        verify(streamAdapter).subscribe(eq(ORG), eq(TOPIC), isNull(),
                eq("test-token"), eq("https://test.salesforce.com"), any(), any());
    }

    @Test
    void subscribe_usesProvidedReplayId() {
        ReplayId providedReplayId = new ReplayId(new byte[]{4, 5, 6});
        GrpcStreamAdapter.StreamHandle handle = newActiveHandle();
        when(streamAdapter.subscribe(anyString(), anyString(), any(), anyString(), anyString(), any(), any()))
                .thenReturn(handle);

        Subscription sub = connector.subscribe(ORG, TOPIC, providedReplayId);

        assertNotNull(sub);
        verify(streamAdapter).subscribe(eq(ORG), eq(TOPIC), eq(providedReplayId),
                anyString(), anyString(), any(), any());
    }

    @Test
    void stopSubscription_cancelsStreamAndRemovesSubscription() {
        StubStreamHandle handle = new StubStreamHandle();
        when(streamAdapter.subscribe(anyString(), anyString(), any(), anyString(), anyString(), any(), any()))
                .thenReturn(handle);
        when(replayStore.getLastReplayId(ORG, TOPIC)).thenReturn(Optional.empty());

        connector.subscribe(ORG, TOPIC, null);
        connector.stopSubscription(ORG, TOPIC);

        assertTrue(handle.wasCancelled());
        assertTrue(connector.getSubscription(ORG, TOPIC).isEmpty());
    }

    @Test
    void getStatus_returnsUpWhenAllSubscriptionsActive() {
        GrpcStreamAdapter.StreamHandle handle = newActiveHandle();
        when(streamAdapter.subscribe(anyString(), anyString(), any(), anyString(), anyString(), any(), any()))
                .thenReturn(handle);
        when(replayStore.getLastReplayId(ORG, TOPIC)).thenReturn(Optional.empty());
        when(circuitBreaker.getState(ORG)).thenReturn(CircuitState.CLOSED);

        connector.subscribe(ORG, TOPIC, null);
        ConnectionStatus status = connector.getStatus(ORG);

        assertTrue(status.isHealthy());
    }

    @Test
    void getStatus_returnsDownWhenCircuitBreakerOpen() {
        when(circuitBreaker.getState(ORG)).thenReturn(CircuitState.OPEN);
        ConnectionStatus status = connector.getStatus(ORG);
        assertFalse(status.isHealthy());
        assertTrue(status.getDetail().contains("Circuit breaker OPEN"));
    }

    @Test
    void getStatus_returnsDownForUnknownOrg() {
        ConnectionStatus status = connector.getStatus("unknown-org");
        assertFalse(status.isHealthy());
        assertTrue(status.getDetail().contains("Unknown org"));
    }

    @Test
    void getStatus_returnsDownWhenChannelNotConnected() {
        when(streamAdapter.isConnected(ORG)).thenReturn(false);
        when(circuitBreaker.getState(ORG)).thenReturn(CircuitState.CLOSED);
        ConnectionStatus status = connector.getStatus(ORG);
        assertFalse(status.isHealthy());
        assertTrue(status.getDetail().contains("not connected"));
    }

    @Test
    void reconnect_refreshesTokenAndResubscribes() {
        ReplayId storedReplayId = new ReplayId(new byte[]{7, 8, 9});
        when(replayStore.getLastReplayId(ORG, TOPIC)).thenReturn(Optional.of(storedReplayId));
        GrpcStreamAdapter.StreamHandle handle = newActiveHandle();
        when(streamAdapter.subscribe(anyString(), anyString(), any(), anyString(), anyString(), any(), any()))
                .thenReturn(handle);

        boolean result = connector.reconnect(ORG);

        assertTrue(result);
        assertTrue(stubAuthService.wasInvalidated(ORG));
        verify(streamAdapter).shutdownChannel(ORG);
        verify(streamAdapter).createChannel(eq(ORG), anyString());
        verify(circuitBreaker).recordSuccess(ORG);
    }

    @Test
    void reconnect_returnsFalseForUnknownOrg() {
        boolean result = connector.reconnect("unknown-org");
        assertFalse(result);
    }

    @Test
    void reconnect_returnsFalseOnAuthFailure() {
        stubAuthService.setFailOnGetToken(true);
        boolean result = connector.reconnect(ORG);
        assertFalse(result);
        verify(circuitBreaker).recordFailure(eq(ORG), any(Exception.class));
    }

    @Test
    void handleGrpcError_unauthenticated_refreshesToken() {
        GrpcStreamAdapter.StreamHandle handle = newActiveHandle();
        when(streamAdapter.subscribe(anyString(), anyString(), any(), anyString(), anyString(), any(), any()))
                .thenReturn(handle);
        when(replayStore.getLastReplayId(ORG, TOPIC)).thenReturn(Optional.empty());

        connector.subscribe(ORG, TOPIC, null);
        stubAuthService.resetInvalidated();

        StatusRuntimeException unauthError = new StatusRuntimeException(Status.UNAUTHENTICATED);
        connector.handleGrpcError(ORG, TOPIC, unauthError);

        assertTrue(stubAuthService.wasInvalidated(ORG));
    }

    @Test
    void handleGrpcError_invalidArgument_stopsSubscription() {
        GrpcStreamAdapter.StreamHandle handle = newActiveHandle();
        when(streamAdapter.subscribe(anyString(), anyString(), any(), anyString(), anyString(), any(), any()))
                .thenReturn(handle);
        when(replayStore.getLastReplayId(ORG, TOPIC)).thenReturn(Optional.empty());

        connector.subscribe(ORG, TOPIC, null);

        StatusRuntimeException invalidError = new StatusRuntimeException(Status.INVALID_ARGUMENT);
        connector.handleGrpcError(ORG, TOPIC, invalidError);

        assertTrue(connector.getSubscription(ORG, TOPIC).isEmpty());
    }

    @Test
    void handleGrpcError_unavailable_recordsFailure() {
        GrpcStreamAdapter.StreamHandle handle = newActiveHandle();
        when(streamAdapter.subscribe(anyString(), anyString(), any(), anyString(), anyString(), any(), any()))
                .thenReturn(handle);
        when(replayStore.getLastReplayId(ORG, TOPIC)).thenReturn(Optional.empty());

        connector.subscribe(ORG, TOPIC, null);

        StatusRuntimeException unavailableError = new StatusRuntimeException(Status.UNAVAILABLE);
        connector.handleGrpcError(ORG, TOPIC, unavailableError);

        verify(circuitBreaker).recordFailure(eq(ORG), eq(unavailableError));
    }

    @Test
    void calculateBackoff_exponentialWithCap() {
        RetryConfig config = new RetryConfig();
        config.setInitialIntervalMs(1000);
        config.setMultiplier(2.0);
        config.setMaxIntervalMs(60000);

        assertEquals(1000, connector.calculateBackoff(1, config));
        assertEquals(2000, connector.calculateBackoff(2, config));
        assertEquals(4000, connector.calculateBackoff(3, config));
        assertEquals(8000, connector.calculateBackoff(4, config));
        assertEquals(16000, connector.calculateBackoff(5, config));
        assertEquals(32000, connector.calculateBackoff(6, config));
        assertEquals(60000, connector.calculateBackoff(7, config)); // capped
    }

    @Test
    void initialize_createsChannelsForEnabledOrgs() {
        OrgConfig disabledOrg = createOrgConfig();
        disabledOrg.setEnabled(false);
        salesforceProperties.setOrgs(Map.of(ORG, createOrgConfig(), "disabled-org", disabledOrg));

        connector.initialize();

        verify(streamAdapter).createChannel(eq(ORG), anyString());
        verify(streamAdapter, never()).createChannel(eq("disabled-org"), anyString());
    }

    @Test
    void shutdown_stopsAllSubscriptionsAndChannels() {
        GrpcStreamAdapter.StreamHandle handle = newActiveHandle();
        when(streamAdapter.subscribe(anyString(), anyString(), any(), anyString(), anyString(), any(), any()))
                .thenReturn(handle);
        when(replayStore.getLastReplayId(ORG, TOPIC)).thenReturn(Optional.empty());

        connector.subscribe(ORG, TOPIC, null);
        connector.shutdown();

        assertTrue(connector.getSubscription(ORG, TOPIC).isEmpty());
        verify(streamAdapter).shutdownChannel(ORG);
    }

    @Test
    void emptyBatchProcessing_deliversBatchToCallback() {
        when(streamAdapter.subscribe(anyString(), anyString(), any(), anyString(), anyString(), any(), any()))
                .thenAnswer(invocation -> {
                    @SuppressWarnings("unchecked")
                    Consumer<EventBatch> callback = invocation.getArgument(5);
                    EventBatch emptyBatch = new EventBatch(ORG, TOPIC, List.of(),
                            new ReplayId(new byte[]{10, 11}), Instant.now());
                    callback.accept(emptyBatch);
                    return newActiveHandle();
                });
        when(replayStore.getLastReplayId(ORG, TOPIC)).thenReturn(Optional.empty());
        when(circuitBreaker.execute(anyString(), any())).thenAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            Supplier<Object> op = invocation.getArgument(1);
            return op.get();
        });

        connector.subscribe(ORG, TOPIC, null);

        assertNotNull(receivedBatch.get());
        assertTrue(receivedBatch.get().isEmpty());
    }

    @Test
    void batchReceived_circuitOpen_dropsBatch() {
        when(streamAdapter.subscribe(anyString(), anyString(), any(), anyString(), anyString(), any(), any()))
                .thenAnswer(invocation -> {
                    @SuppressWarnings("unchecked")
                    Consumer<EventBatch> callback = invocation.getArgument(5);
                    EventBatch batch = new EventBatch(ORG, TOPIC, List.of(),
                            new ReplayId(new byte[]{1}), Instant.now());
                    callback.accept(batch);
                    return newActiveHandle();
                });
        when(replayStore.getLastReplayId(ORG, TOPIC)).thenReturn(Optional.empty());
        when(circuitBreaker.execute(anyString(), any())).thenThrow(new CircuitOpenException(ORG));

        connector.subscribe(ORG, TOPIC, null);

        assertNull(receivedBatch.get());
    }

    @Test
    void subscription_isActiveAfterCreation() {
        Subscription sub = new Subscription(ORG, TOPIC, batch -> {}, () -> {});
        assertTrue(sub.isActive());
        assertEquals(ORG, sub.getOrg());
        assertEquals(TOPIC, sub.getTopic());
        assertEquals(ORG + ":" + TOPIC, sub.getKey());
    }

    @Test
    void subscription_stopIsIdempotent() {
        AtomicInteger stopCount = new AtomicInteger(0);
        Subscription sub = new Subscription(ORG, TOPIC, batch -> {}, stopCount::incrementAndGet);

        sub.stop();
        sub.stop();
        sub.stop();

        assertFalse(sub.isActive());
        assertEquals(1, stopCount.get());
    }

    @Test
    void subscription_deliverBatchIgnoredWhenInactive() {
        AtomicReference<EventBatch> delivered = new AtomicReference<>();
        Subscription sub = new Subscription(ORG, TOPIC, delivered::set, () -> {});

        sub.stop();
        EventBatch batch = new EventBatch(ORG, TOPIC, List.of(),
                new ReplayId(new byte[]{1}), Instant.now());
        sub.deliverBatch(batch);

        assertNull(delivered.get());
    }

    @Test
    void getStatus_returnsUpForDisabledOrg() {
        OrgConfig disabledOrg = createOrgConfig();
        disabledOrg.setEnabled(false);
        salesforceProperties.setOrgs(Map.of("disabled-org", disabledOrg));

        ConnectionStatus status = connector.getStatus("disabled-org");

        assertTrue(status.isHealthy());
        assertTrue(status.getDetail().contains("disabled"));
    }

    @Test
    void initialize_createsChannelsAndSubscriptionsForMultipleOrgs() {
        String org2 = "acad";
        String topic2 = "/event/Course_Event__e";
        OrgConfig orgConfig2 = createOrgConfig();
        orgConfig2.setTopics(List.of(topic2));

        salesforceProperties.setOrgs(Map.of(ORG, createOrgConfig(), org2, orgConfig2));
        when(streamAdapter.isConnected(org2)).thenReturn(true);

        connector.initialize();

        verify(streamAdapter).createChannel(eq(ORG), anyString());
        verify(streamAdapter).createChannel(eq(org2), anyString());

        // Subscribe to topics for each org
        GrpcStreamAdapter.StreamHandle handle = newActiveHandle();
        when(streamAdapter.subscribe(anyString(), anyString(), any(), anyString(), anyString(), any(), any()))
                .thenReturn(handle);
        when(replayStore.getLastReplayId(anyString(), anyString())).thenReturn(Optional.empty());

        Subscription sub1 = connector.subscribe(ORG, TOPIC, null);
        Subscription sub2 = connector.subscribe(org2, topic2, null);

        assertTrue(sub1.isActive());
        assertTrue(sub2.isActive());
        assertTrue(connector.getSubscription(ORG, TOPIC).isPresent());
        assertTrue(connector.getSubscription(org2, topic2).isPresent());
    }

    @Test
    void handleGrpcError_exceedingMaxRetries_stopsSubscriptionAndRecordsFailure() {
        GrpcStreamAdapter.StreamHandle handle = newActiveHandle();
        when(streamAdapter.subscribe(anyString(), anyString(), any(), anyString(), anyString(), any(), any()))
                .thenReturn(handle);
        when(replayStore.getLastReplayId(ORG, TOPIC)).thenReturn(Optional.empty());

        connector.subscribe(ORG, TOPIC, null);

        StatusRuntimeException unavailableError = new StatusRuntimeException(Status.UNAVAILABLE);

        // maxAttempts is 3 in setUp; call handleGrpcError 4 times to exceed it
        for (int i = 0; i < 4; i++) {
            connector.handleGrpcError(ORG, TOPIC, unavailableError);
        }

        // After exceeding max retries, subscription should be stopped
        assertTrue(connector.getSubscription(ORG, TOPIC).isEmpty());
        // recordFailure called once per handleGrpcError + once when max exceeded in attemptRetry
        verify(circuitBreaker, atLeast(4)).recordFailure(eq(ORG), any(Exception.class));
    }

    @Test
    void handleGrpcError_deadlineExceeded_recordsFailure() {
        GrpcStreamAdapter.StreamHandle handle = newActiveHandle();
        when(streamAdapter.subscribe(anyString(), anyString(), any(), anyString(), anyString(), any(), any()))
                .thenReturn(handle);
        when(replayStore.getLastReplayId(ORG, TOPIC)).thenReturn(Optional.empty());

        connector.subscribe(ORG, TOPIC, null);

        StatusRuntimeException deadlineError = new StatusRuntimeException(Status.DEADLINE_EXCEEDED);
        connector.handleGrpcError(ORG, TOPIC, deadlineError);

        verify(circuitBreaker).recordFailure(eq(ORG), eq(deadlineError));
    }

    @Test
    void handleGrpcError_resourceExhausted_recordsFailure() {
        GrpcStreamAdapter.StreamHandle handle = newActiveHandle();
        when(streamAdapter.subscribe(anyString(), anyString(), any(), anyString(), anyString(), any(), any()))
                .thenReturn(handle);
        when(replayStore.getLastReplayId(ORG, TOPIC)).thenReturn(Optional.empty());

        connector.subscribe(ORG, TOPIC, null);

        StatusRuntimeException resourceError = new StatusRuntimeException(Status.RESOURCE_EXHAUSTED);
        connector.handleGrpcError(ORG, TOPIC, resourceError);

        verify(circuitBreaker).recordFailure(eq(ORG), eq(resourceError));
    }

    @Test
    void getStatus_returnsDownWhenNotAllSubscriptionsActive() {
        String topic2 = "/event/Student_Event__e";
        OrgConfig multiTopicConfig = createOrgConfig();
        multiTopicConfig.setTopics(List.of(TOPIC, topic2));
        salesforceProperties.setOrgs(Map.of(ORG, multiTopicConfig));

        when(circuitBreaker.getState(ORG)).thenReturn(CircuitState.CLOSED);

        GrpcStreamAdapter.StreamHandle handle = newActiveHandle();
        when(streamAdapter.subscribe(anyString(), anyString(), any(), anyString(), anyString(), any(), any()))
                .thenReturn(handle);
        when(replayStore.getLastReplayId(anyString(), anyString())).thenReturn(Optional.empty());

        // Only subscribe to one of the two configured topics
        connector.subscribe(ORG, TOPIC, null);

        ConnectionStatus status = connector.getStatus(ORG);

        assertFalse(status.isHealthy());
        assertTrue(status.getDetail().contains("1"));
        assertTrue(status.getDetail().contains("2"));
    }

    @Test
    void reconnect_resubscribesToAllTopicsForOrg() {
        String topic2 = "/event/Student_Event__e";
        OrgConfig multiTopicConfig = createOrgConfig();
        multiTopicConfig.setTopics(List.of(TOPIC, topic2));
        salesforceProperties.setOrgs(Map.of(ORG, multiTopicConfig));

        GrpcStreamAdapter.StreamHandle handle = newActiveHandle();
        when(streamAdapter.subscribe(anyString(), anyString(), any(), anyString(), anyString(), any(), any()))
                .thenReturn(handle);
        when(replayStore.getLastReplayId(anyString(), anyString())).thenReturn(Optional.empty());

        boolean result = connector.reconnect(ORG);

        assertTrue(result);
        // Verify subscribe was called for both topics
        verify(streamAdapter).subscribe(eq(ORG), eq(TOPIC), any(), anyString(), anyString(), any(), any());
        verify(streamAdapter).subscribe(eq(ORG), eq(topic2), any(), anyString(), anyString(), any(), any());
        // Both subscriptions should be active
        assertTrue(connector.getSubscription(ORG, TOPIC).isPresent());
        assertTrue(connector.getSubscription(ORG, topic2).isPresent());
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private OrgConfig createOrgConfig() {
        OrgConfig config = new OrgConfig();
        config.setPubsubUrl("api.pubsub.salesforce.com:7443");
        config.setOauthUrl("https://login.salesforce.com/services/oauth2/token");
        config.setClientId("test-client-id");
        config.setClientSecret("test-client-secret");
        config.setUsername("test@example.com");
        config.setPassword("test-password");
        config.setTopics(List.of(TOPIC));
        config.setEnabled(true);
        return config;
    }

    private GrpcStreamAdapter.StreamHandle newActiveHandle() {
        return new StubStreamHandle();
    }

    /**
     * Simple stub for StreamHandle that avoids Mockito nesting issues.
     */
    static class StubStreamHandle implements GrpcStreamAdapter.StreamHandle {
        private final AtomicBoolean active = new AtomicBoolean(true);
        private final AtomicBoolean cancelled = new AtomicBoolean(false);

        @Override
        public void cancel() {
            active.set(false);
            cancelled.set(true);
        }

        @Override
        public boolean isActive() {
            return active.get();
        }

        boolean wasCancelled() {
            return cancelled.get();
        }
    }

    /**
     * Simple test double for SalesforceAuthService that avoids Mockito mocking issues.
     */
    static class StubAuthService extends SalesforceAuthService {
        private boolean failOnGetToken = false;
        private final java.util.Set<String> invalidatedOrgs = new java.util.HashSet<>();

        StubAuthService(SalesforceProperties properties) {
            super(properties, (url, cid, cs, u, p) ->
                    new OAuthTokenResponse("test-token", "https://test.salesforce.com",
                            "Bearer", String.valueOf(System.currentTimeMillis()), null, null));
        }

        @Override
        public String getAccessToken(String orgId) {
            if (failOnGetToken) {
                throw new SalesforceAuthException("Simulated auth failure");
            }
            return "test-token";
        }

        @Override
        public String getInstanceUrl(String orgId) {
            return "https://test.salesforce.com";
        }

        @Override
        public void invalidateToken(String orgId) {
            invalidatedOrgs.add(orgId);
        }

        void setFailOnGetToken(boolean fail) {
            this.failOnGetToken = fail;
        }

        boolean wasInvalidated(String orgId) {
            return invalidatedOrgs.contains(orgId);
        }

        void resetInvalidated() {
            invalidatedOrgs.clear();
        }
    }
}
