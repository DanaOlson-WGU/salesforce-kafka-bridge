package com.example.bridge.service;

import com.example.bridge.config.BridgeProperties;
import com.example.bridge.config.OrgConfig;
import com.example.bridge.config.RetryConfig;
import com.example.bridge.config.SalesforceProperties;
import com.example.bridge.kafka.KafkaEventPublisher;
import com.example.bridge.model.ConnectionStatus;
import com.example.bridge.model.EventBatch;
import com.example.bridge.model.PlatformEvent;
import com.example.bridge.model.PublishResult;
import com.example.bridge.model.ReplayId;
import com.example.bridge.pubsub.GrpcPubSubConnector;
import com.example.bridge.pubsub.GrpcStreamAdapter;
import com.example.bridge.pubsub.SalesforceAuthService;
import com.example.bridge.replay.CheckpointException;
import com.example.bridge.replay.ReplayStore;
import com.example.bridge.resilience.CircuitBreaker;
import com.example.bridge.metrics.BridgeMetrics;
import com.example.bridge.resilience.CircuitOpenException;
import com.example.bridge.resilience.CircuitState;
import com.example.bridge.routing.ConfigurableTopicRouter;
import com.example.bridge.routing.TopicRouter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import net.jqwik.api.*;
import net.jqwik.api.constraints.IntRange;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Property 14: Multi-Org Concurrency
 *
 * For any configuration with N orgs, the bridge should maintain N concurrent
 * active subscriptions with independent connections, circuit breakers, and
 * replay store entries.
 *
 * Validates: Requirements 5.1, 5.2
 */
class MultiOrgConcurrencyPropertyTest {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    // -----------------------------------------------------------------------
    // Property: N orgs produce N active subscriptions
    // -----------------------------------------------------------------------

    @Property(tries = 30)
    void nOrgsShouldProduceNActiveSubscriptions(
            @ForAll("orgSets") List<String> orgNames) {

        String topic = "/event/Test_Event__e";
        SalesforceProperties sfProps = createMultiOrgProperties(orgNames, List.of(topic));
        BridgeProperties bridgeProps = createBridgeProperties(orgNames, topic, "kafka.test");

        StubStreamAdapter streamAdapter = new StubStreamAdapter();
        StubReplayStore replayStore = new StubReplayStore();
        StubCircuitBreaker circuitBreaker = new StubCircuitBreaker();
        StubAuthService authService = new StubAuthService(sfProps);
        TopicRouter topicRouter = new ConfigurableTopicRouter(bridgeProps, sfProps);

        AtomicInteger batchCount = new AtomicInteger(0);
        GrpcPubSubConnector connector = new GrpcPubSubConnector(
                sfProps, authService, replayStore, circuitBreaker, streamAdapter,
                batch -> batchCount.incrementAndGet());

        StubKafkaPublisher kafkaPublisher = new StubKafkaPublisher(replayStore, topicRouter);

        EventProcessingService service = new EventProcessingService(
                sfProps, connector, topicRouter, kafkaPublisher, circuitBreaker, replayStore, new BridgeMetrics(new SimpleMeterRegistry()));

        service.start();

        // Each enabled org should have an active subscription for each topic
        int expectedSubscriptions = orgNames.size();
        assertEquals(expectedSubscriptions, service.getActiveSubscriptionCount(),
                "Should have " + expectedSubscriptions + " active subscriptions for " + orgNames.size() + " orgs");

        for (String org : orgNames) {
            assertTrue(service.isSubscriptionActive(org, topic),
                    "Subscription should be active for org '" + org + "'");
        }

        service.shutdown();
    }

    // -----------------------------------------------------------------------
    // Property: Each org has independent replay store entries
    // -----------------------------------------------------------------------

    @Property(tries = 30)
    void eachOrgShouldHaveIndependentReplayStoreEntries(
            @ForAll("orgSets") List<String> orgNames) {

        String topic = "/event/Test_Event__e";
        SalesforceProperties sfProps = createMultiOrgProperties(orgNames, List.of(topic));
        BridgeProperties bridgeProps = createBridgeProperties(orgNames, topic, "kafka.test");

        StubStreamAdapter streamAdapter = new StubStreamAdapter();
        StubReplayStore replayStore = new StubReplayStore();
        StubCircuitBreaker circuitBreaker = new StubCircuitBreaker();
        StubAuthService authService = new StubAuthService(sfProps);
        TopicRouter topicRouter = new ConfigurableTopicRouter(bridgeProps, sfProps);
        StubKafkaPublisher kafkaPublisher = new StubKafkaPublisher(replayStore, topicRouter);

        GrpcPubSubConnector connector = new GrpcPubSubConnector(
                sfProps, authService, replayStore, circuitBreaker, streamAdapter,
                batch -> {});

        EventProcessingService service = new EventProcessingService(
                sfProps, connector, topicRouter, kafkaPublisher, circuitBreaker, replayStore, new BridgeMetrics(new SimpleMeterRegistry()));

        service.start();

        // Checkpoint different replay IDs for each org
        Map<String, ReplayId> expectedReplayIds = new HashMap<>();
        for (String org : orgNames) {
            byte[] replayBytes = (org + "-replay-data").getBytes();
            ReplayId replayId = new ReplayId(replayBytes);
            expectedReplayIds.put(org, replayId);

            EventBatch batch = createBatch(org, topic, replayId, 1);
            service.processBatch(batch);
        }

        // Verify each org has its own independent replay ID
        for (String org : orgNames) {
            Optional<ReplayId> stored = replayStore.getLastReplayId(org, topic);
            assertTrue(stored.isPresent(),
                    "Replay ID should be stored for org '" + org + "'");
            assertEquals(expectedReplayIds.get(org), stored.get(),
                    "Replay ID for org '" + org + "' should match what was checkpointed");
        }

        service.shutdown();
    }

    // -----------------------------------------------------------------------
    // Property: Each org has independent circuit breakers
    // -----------------------------------------------------------------------

    @Property(tries = 30)
    void eachOrgShouldHaveIndependentCircuitBreakers(
            @ForAll("orgSets") List<String> orgNames) {

        Assume.that(orgNames.size() >= 2);

        String topic = "/event/Test_Event__e";
        SalesforceProperties sfProps = createMultiOrgProperties(orgNames, List.of(topic));
        BridgeProperties bridgeProps = createBridgeProperties(orgNames, topic, "kafka.test");

        StubStreamAdapter streamAdapter = new StubStreamAdapter();
        StubReplayStore replayStore = new StubReplayStore();
        StubCircuitBreaker circuitBreaker = new StubCircuitBreaker();
        StubAuthService authService = new StubAuthService(sfProps);
        TopicRouter topicRouter = new ConfigurableTopicRouter(bridgeProps, sfProps);
        StubKafkaPublisher kafkaPublisher = new StubKafkaPublisher(replayStore, topicRouter);

        GrpcPubSubConnector connector = new GrpcPubSubConnector(
                sfProps, authService, replayStore, circuitBreaker, streamAdapter,
                batch -> {});

        EventProcessingService service = new EventProcessingService(
                sfProps, connector, topicRouter, kafkaPublisher, circuitBreaker, replayStore, new BridgeMetrics(new SimpleMeterRegistry()));

        service.start();

        // Open circuit for the first org only
        String failingOrg = orgNames.get(0);
        circuitBreaker.forceOpen(failingOrg);

        // All other orgs should still have CLOSED circuits
        for (int i = 1; i < orgNames.size(); i++) {
            String org = orgNames.get(i);
            assertEquals(CircuitState.CLOSED, circuitBreaker.getState(org),
                    "Circuit should remain CLOSED for org '" + org + "' when '" + failingOrg + "' is OPEN");
        }

        assertEquals(CircuitState.OPEN, circuitBreaker.getState(failingOrg),
                "Circuit should be OPEN for failing org '" + failingOrg + "'");

        service.shutdown();
    }

    // -----------------------------------------------------------------------
    // Property: Concurrent batch processing across orgs
    // -----------------------------------------------------------------------

    @Property(tries = 20)
    void concurrentBatchProcessingAcrossOrgs(
            @ForAll("orgSets") List<String> orgNames) throws InterruptedException {

        String topic = "/event/Test_Event__e";
        SalesforceProperties sfProps = createMultiOrgProperties(orgNames, List.of(topic));
        BridgeProperties bridgeProps = createBridgeProperties(orgNames, topic, "kafka.test");

        StubStreamAdapter streamAdapter = new StubStreamAdapter();
        StubReplayStore replayStore = new StubReplayStore();
        StubCircuitBreaker circuitBreaker = new StubCircuitBreaker();
        StubAuthService authService = new StubAuthService(sfProps);
        TopicRouter topicRouter = new ConfigurableTopicRouter(bridgeProps, sfProps);
        StubKafkaPublisher kafkaPublisher = new StubKafkaPublisher(replayStore, topicRouter);

        GrpcPubSubConnector connector = new GrpcPubSubConnector(
                sfProps, authService, replayStore, circuitBreaker, streamAdapter,
                batch -> {});

        EventProcessingService service = new EventProcessingService(
                sfProps, connector, topicRouter, kafkaPublisher, circuitBreaker, replayStore, new BridgeMetrics(new SimpleMeterRegistry()));

        service.start();

        // Process batches concurrently from all orgs
        CountDownLatch latch = new CountDownLatch(orgNames.size());
        Map<String, Boolean> results = new ConcurrentHashMap<>();

        for (String org : orgNames) {
            Thread t = new Thread(() -> {
                try {
                    EventBatch batch = createBatch(org, topic, new ReplayId(org.getBytes()), 2);
                    service.processBatch(batch);
                    results.put(org, true);
                } catch (Exception e) {
                    results.put(org, false);
                } finally {
                    latch.countDown();
                }
            }, "test-" + org);
            t.setDaemon(true);
            t.start();
        }

        assertTrue(latch.await(5, TimeUnit.SECONDS), "All batches should complete within 5 seconds");

        // All orgs should have processed successfully
        for (String org : orgNames) {
            assertTrue(results.getOrDefault(org, false),
                    "Batch processing should succeed for org '" + org + "'");
        }

        service.shutdown();
    }

    // -----------------------------------------------------------------------
    // Property: Independent gRPC connections per org
    // -----------------------------------------------------------------------

    @Property(tries = 30)
    void eachOrgShouldHaveIndependentGrpcConnections(
            @ForAll("orgSets") List<String> orgNames) {

        String topic = "/event/Test_Event__e";
        SalesforceProperties sfProps = createMultiOrgProperties(orgNames, List.of(topic));
        BridgeProperties bridgeProps = createBridgeProperties(orgNames, topic, "kafka.test");

        TrackingStreamAdapter streamAdapter = new TrackingStreamAdapter();
        StubReplayStore replayStore = new StubReplayStore();
        StubCircuitBreaker circuitBreaker = new StubCircuitBreaker();
        StubAuthService authService = new StubAuthService(sfProps);
        TopicRouter topicRouter = new ConfigurableTopicRouter(bridgeProps, sfProps);
        StubKafkaPublisher kafkaPublisher = new StubKafkaPublisher(replayStore, topicRouter);

        GrpcPubSubConnector connector = new GrpcPubSubConnector(
                sfProps, authService, replayStore, circuitBreaker, streamAdapter,
                batch -> {});

        EventProcessingService service = new EventProcessingService(
                sfProps, connector, topicRouter, kafkaPublisher, circuitBreaker, replayStore, new BridgeMetrics(new SimpleMeterRegistry()));

        service.start();

        // Verify each org got its own channel
        assertEquals(orgNames.size(), streamAdapter.getChannelCount(),
                "Should create " + orgNames.size() + " independent gRPC channels");

        for (String org : orgNames) {
            assertTrue(streamAdapter.hasChannel(org),
                    "Should have created a channel for org '" + org + "'");
        }

        service.shutdown();
    }

    // -----------------------------------------------------------------------
    // Providers
    // -----------------------------------------------------------------------

    @Provide
    Arbitrary<List<String>> orgSets() {
        return Arbitraries.of("srm", "acad", "org-alpha", "org-beta", "org-gamma")
                .list().ofMinSize(2).ofMaxSize(5).uniqueElements();
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private SalesforceProperties createMultiOrgProperties(List<String> orgNames, List<String> topics) {
        SalesforceProperties props = new SalesforceProperties();
        props.setRetry(createRetryConfig());
        Map<String, OrgConfig> orgs = new LinkedHashMap<>();
        for (String org : orgNames) {
            orgs.put(org, createOrgConfig(topics));
        }
        props.setOrgs(orgs);
        return props;
    }

    private BridgeProperties createBridgeProperties(List<String> orgNames, String sfTopic, String kafkaTopic) {
        BridgeProperties props = new BridgeProperties();
        Map<String, Map<String, String>> routing = new LinkedHashMap<>();
        for (String org : orgNames) {
            routing.put(org, Map.of(sfTopic, kafkaTopic + "." + org));
        }
        props.setTopicRouting(routing);
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

    private EventBatch createBatch(String org, String topic, ReplayId replayId, int eventCount) {
        List<PlatformEvent> events = new ArrayList<>();
        for (int i = 0; i < eventCount; i++) {
            ObjectNode payload = objectMapper.createObjectNode();
            payload.put("Id", org + "-event-" + i);
            payload.put("data", "test-data-" + i);
            events.add(new PlatformEvent("TestEvent__e", payload, replayId));
        }
        return new EventBatch(org, topic, events, replayId, Instant.now());
    }


    // -----------------------------------------------------------------------
    // Test Doubles
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

        @Override
        public <T> T execute(String org, Supplier<T> operation) {
            if (getState(org) == CircuitState.OPEN) {
                throw new CircuitOpenException(org);
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
    }

    static class TrackingStreamAdapter implements GrpcStreamAdapter {
        private final Map<String, Boolean> channels = new ConcurrentHashMap<>();

        @Override
        public StreamHandle subscribe(String org, String topic, ReplayId replayId,
                                       String accessToken, String instanceUrl,
                                       Consumer<EventBatch> batchCallback,
                                       Consumer<Throwable> errorCallback) {
            return new StubStreamHandle();
        }

        @Override
        public boolean isConnected(String org) {
            return channels.containsKey(org);
        }

        @Override
        public void createChannel(String org, String pubsubUrl) {
            channels.put(org, true);
        }

        @Override
        public void shutdownChannel(String org) {
            channels.remove(org);
        }

        int getChannelCount() {
            return channels.size();
        }

        boolean hasChannel(String org) {
            return channels.containsKey(org);
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

    static class StubKafkaPublisher implements KafkaEventPublisher {
        private final ReplayStore replayStore;
        private final TopicRouter topicRouter;

        StubKafkaPublisher(ReplayStore replayStore, TopicRouter topicRouter) {
            this.replayStore = replayStore;
            this.topicRouter = topicRouter;
        }

        @Override
        public PublishResult publishBatch(EventBatch batch) {
            Optional<String> kafkaTopic = topicRouter.getKafkaTopic(batch.getOrg(), batch.getSalesforceTopic());
            if (kafkaTopic.isEmpty()) {
                return PublishResult.success(0);
            }

            try {
                replayStore.checkpoint(batch.getOrg(), batch.getSalesforceTopic(), batch.getReplayId());
                return PublishResult.success(batch.getEvents().size());
            } catch (CheckpointException e) {
                return PublishResult.failure(e);
            }
        }

        @Override
        public ConnectionStatus getKafkaStatus() {
            return ConnectionStatus.up("Stub Kafka healthy");
        }
    }
}
