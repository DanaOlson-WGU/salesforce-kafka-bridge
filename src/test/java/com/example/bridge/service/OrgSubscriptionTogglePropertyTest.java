package com.example.bridge.service;

import com.example.bridge.config.BridgeProperties;
import com.example.bridge.config.OrgConfig;
import com.example.bridge.config.RetryConfig;
import com.example.bridge.config.SalesforceProperties;
import com.example.bridge.kafka.KafkaEventPublisher;
import com.example.bridge.model.ConnectionStatus;
import com.example.bridge.model.EventBatch;
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
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import net.jqwik.api.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Property 24: Org Subscription Toggle
 *
 * For any org with subscription disabled via configuration flag, the bridge
 * should not establish a subscription for that org.
 *
 * Validates: Requirements 10.4
 */
class OrgSubscriptionTogglePropertyTest {

    // -----------------------------------------------------------------------
    // Property: Disabled orgs should not have subscriptions
    // -----------------------------------------------------------------------

    @Property(tries = 30)
    void disabledOrgsShouldNotHaveSubscriptions(
            @ForAll("orgSets") List<String> allOrgs,
            @ForAll("disabledOrgIndices") int disabledIndex) {

        Assume.that(disabledIndex < allOrgs.size());

        String topic = "/event/Test_Event__e";
        String disabledOrg = allOrgs.get(disabledIndex);

        SalesforceProperties sfProps = createPropertiesWithDisabledOrg(allOrgs, disabledOrg, List.of(topic));
        BridgeProperties bridgeProps = createBridgeProperties(allOrgs, topic, "kafka.test");

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

        // Disabled org should NOT have a subscription
        assertFalse(service.isSubscriptionActive(disabledOrg, topic),
                "Disabled org '" + disabledOrg + "' should NOT have an active subscription");

        // Disabled org should NOT have a gRPC channel
        assertFalse(streamAdapter.hasChannel(disabledOrg),
                "Disabled org '" + disabledOrg + "' should NOT have a gRPC channel");

        // All enabled orgs should have subscriptions
        for (String org : allOrgs) {
            if (!org.equals(disabledOrg)) {
                assertTrue(service.isSubscriptionActive(org, topic),
                        "Enabled org '" + org + "' should have an active subscription");
                assertTrue(streamAdapter.hasChannel(org),
                        "Enabled org '" + org + "' should have a gRPC channel");
            }
        }

        // Total active subscriptions should be N-1 (all except disabled)
        assertEquals(allOrgs.size() - 1, service.getActiveSubscriptionCount(),
                "Should have " + (allOrgs.size() - 1) + " active subscriptions (excluding disabled org)");

        service.shutdown();
    }

    // -----------------------------------------------------------------------
    // Property: All orgs disabled means no subscriptions
    // -----------------------------------------------------------------------

    @Property(tries = 20)
    void allOrgsDisabledMeansNoSubscriptions(
            @ForAll("orgSets") List<String> allOrgs) {

        String topic = "/event/Test_Event__e";

        SalesforceProperties sfProps = createPropertiesAllDisabled(allOrgs, List.of(topic));
        BridgeProperties bridgeProps = createBridgeProperties(allOrgs, topic, "kafka.test");

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

        // No subscriptions should be active
        assertEquals(0, service.getActiveSubscriptionCount(),
                "Should have 0 active subscriptions when all orgs are disabled");

        // No channels should be created
        assertEquals(0, streamAdapter.getChannelCount(),
                "Should have 0 gRPC channels when all orgs are disabled");

        service.shutdown();
    }

    // -----------------------------------------------------------------------
    // Property: Multiple disabled orgs are all skipped
    // -----------------------------------------------------------------------

    @Property(tries = 30)
    void multipleDisabledOrgsAreAllSkipped(
            @ForAll("orgSets") List<String> allOrgs,
            @ForAll("disabledCounts") int disabledCount) {

        Assume.that(disabledCount > 0 && disabledCount < allOrgs.size());

        String topic = "/event/Test_Event__e";
        Set<String> disabledOrgs = new HashSet<>(allOrgs.subList(0, disabledCount));

        SalesforceProperties sfProps = createPropertiesWithMultipleDisabled(allOrgs, disabledOrgs, List.of(topic));
        BridgeProperties bridgeProps = createBridgeProperties(allOrgs, topic, "kafka.test");

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

        // All disabled orgs should NOT have subscriptions
        for (String disabledOrg : disabledOrgs) {
            assertFalse(service.isSubscriptionActive(disabledOrg, topic),
                    "Disabled org '" + disabledOrg + "' should NOT have an active subscription");
        }

        // All enabled orgs should have subscriptions
        int expectedEnabled = allOrgs.size() - disabledCount;
        assertEquals(expectedEnabled, service.getActiveSubscriptionCount(),
                "Should have " + expectedEnabled + " active subscriptions");

        service.shutdown();
    }

    // -----------------------------------------------------------------------
    // Property: Enabled flag true means subscription is created
    // -----------------------------------------------------------------------

    @Property(tries = 30)
    void enabledFlagTrueMeansSubscriptionCreated(
            @ForAll("orgSets") List<String> allOrgs) {

        String topic = "/event/Test_Event__e";

        // All orgs enabled
        SalesforceProperties sfProps = createPropertiesAllEnabled(allOrgs, List.of(topic));
        BridgeProperties bridgeProps = createBridgeProperties(allOrgs, topic, "kafka.test");

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

        // All orgs should have subscriptions
        assertEquals(allOrgs.size(), service.getActiveSubscriptionCount(),
                "All " + allOrgs.size() + " orgs should have active subscriptions");

        for (String org : allOrgs) {
            assertTrue(service.isSubscriptionActive(org, topic),
                    "Org '" + org + "' should have an active subscription");
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

    @Provide
    Arbitrary<Integer> disabledOrgIndices() {
        return Arbitraries.integers().between(0, 4);
    }

    @Provide
    Arbitrary<Integer> disabledCounts() {
        return Arbitraries.integers().between(1, 4);
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private SalesforceProperties createPropertiesWithDisabledOrg(
            List<String> orgNames, String disabledOrg, List<String> topics) {
        SalesforceProperties props = new SalesforceProperties();
        props.setRetry(createRetryConfig());
        Map<String, OrgConfig> orgs = new LinkedHashMap<>();
        for (String org : orgNames) {
            OrgConfig config = createOrgConfig(topics);
            config.setEnabled(!org.equals(disabledOrg));
            orgs.put(org, config);
        }
        props.setOrgs(orgs);
        return props;
    }

    private SalesforceProperties createPropertiesWithMultipleDisabled(
            List<String> orgNames, Set<String> disabledOrgs, List<String> topics) {
        SalesforceProperties props = new SalesforceProperties();
        props.setRetry(createRetryConfig());
        Map<String, OrgConfig> orgs = new LinkedHashMap<>();
        for (String org : orgNames) {
            OrgConfig config = createOrgConfig(topics);
            config.setEnabled(!disabledOrgs.contains(org));
            orgs.put(org, config);
        }
        props.setOrgs(orgs);
        return props;
    }

    private SalesforceProperties createPropertiesAllDisabled(List<String> orgNames, List<String> topics) {
        SalesforceProperties props = new SalesforceProperties();
        props.setRetry(createRetryConfig());
        Map<String, OrgConfig> orgs = new LinkedHashMap<>();
        for (String org : orgNames) {
            OrgConfig config = createOrgConfig(topics);
            config.setEnabled(false);
            orgs.put(org, config);
        }
        props.setOrgs(orgs);
        return props;
    }

    private SalesforceProperties createPropertiesAllEnabled(List<String> orgNames, List<String> topics) {
        SalesforceProperties props = new SalesforceProperties();
        props.setRetry(createRetryConfig());
        Map<String, OrgConfig> orgs = new LinkedHashMap<>();
        for (String org : orgNames) {
            OrgConfig config = createOrgConfig(topics);
            config.setEnabled(true);
            orgs.put(org, config);
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
        public void recordFailure(String org, Exception error) {}

        @Override
        public CircuitState getState(String org) {
            return states.getOrDefault(org, CircuitState.CLOSED);
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
        public void invalidateToken(String orgId) {}
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
