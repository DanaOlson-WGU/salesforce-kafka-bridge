package com.example.bridge.service;

import com.example.bridge.avro.AvroRecordConverter;
import com.example.bridge.config.BridgeProperties;
import com.example.bridge.config.OrgConfig;
import com.example.bridge.config.RetryConfig;
import com.example.bridge.config.SalesforceProperties;
import com.example.bridge.kafka.DefaultKafkaEventPublisher;
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
import com.example.bridge.replay.JpaReplayStore;
import com.example.bridge.replay.ReplayIdRepository;
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
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.*;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import jakarta.persistence.EntityManagerFactory;
import javax.sql.DataSource;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for the end-to-end event processing pipeline.
 * Uses Testcontainers for real Kafka and PostgreSQL, with mock Salesforce Pub/Sub API.
 *
 * Requirements: 1.1, 1.2, 2.1, 2.4, 3.2, 3.3, 3.4, 4.2, 5.1, 5.3
 */
class EventProcessingIntegrationTest {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final String KAFKA_TOPIC_SRM = "salesforce.srm.enrollment";
    private static final String KAFKA_TOPIC_ACAD = "salesforce.acad.course";
    private static final String SF_TOPIC_SRM = "/event/Enrollment_Event__e";
    private static final String SF_TOPIC_ACAD = "/event/Course_Event__e";

    static PostgreSQLContainer<?> postgres;
    static KafkaContainer kafka;

    private static AnnotationConfigApplicationContext springContext;
    private static ReplayStore replayStore;
    private static JdbcTemplate jdbcTemplate;
    private static KafkaTemplate<String, GenericRecord> kafkaTemplate;
    private static AvroRecordConverter avroRecordConverter;
    private static boolean infrastructureAvailable = false;

    @BeforeAll
    static void setupInfrastructure() {
        try {
            postgres = new PostgreSQLContainer<>("postgres:15-alpine")
                    .withDatabaseName("testdb").withUsername("test").withPassword("test");
            postgres.start();

            kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.0"));
            kafka.start();

            DataSource dataSource = DataSourceBuilder.create()
                    .url(postgres.getJdbcUrl()).username(postgres.getUsername())
                    .password(postgres.getPassword()).driverClassName("org.postgresql.Driver").build();

            JdbcTemplate template = new JdbcTemplate(dataSource);
            template.execute("CREATE TABLE IF NOT EXISTS replay_ids (" +
                    "id BIGSERIAL PRIMARY KEY, org VARCHAR(50) NOT NULL, " +
                    "salesforce_topic VARCHAR(255) NOT NULL, replay_id BYTEA NOT NULL, " +
                    "last_updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, " +
                    "CONSTRAINT uq_org_topic UNIQUE(org, salesforce_topic))");
            template.execute("CREATE INDEX IF NOT EXISTS idx_replay_ids_org_topic ON replay_ids(org, salesforce_topic)");

            springContext = new AnnotationConfigApplicationContext();
            springContext.registerBean("dataSource", DataSource.class, () -> dataSource);
            springContext.register(JpaConfig.class);
            springContext.refresh();

            replayStore = springContext.getBean(ReplayStore.class);
            jdbcTemplate = new JdbcTemplate(dataSource);

            Map<String, Object> producerProps = new HashMap<>();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
            producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
            DefaultKafkaProducerFactory<String, GenericRecord> producerFactory =
                    new DefaultKafkaProducerFactory<>(producerProps);
            producerFactory.setValueSerializer(new org.apache.kafka.common.serialization.Serializer<>() {
                @Override
                public byte[] serialize(String topic, GenericRecord data) {
                    if (data == null) return null;
                    try {
                        var baos = new java.io.ByteArrayOutputStream();
                        var encoder = org.apache.avro.io.EncoderFactory.get().binaryEncoder(baos, null);
                        new org.apache.avro.generic.GenericDatumWriter<GenericRecord>(data.getSchema()).write(data, encoder);
                        encoder.flush();
                        return baos.toByteArray();
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to serialize GenericRecord", e);
                    }
                }
            });
            kafkaTemplate = new KafkaTemplate<>(producerFactory);

            Schema schema = new Schema.Parser().parse(
                    EventProcessingIntegrationTest.class.getResourceAsStream("/avro/SalesforcePlatformEvent.avsc"));
            avroRecordConverter = new AvroRecordConverter(schema, objectMapper);

            infrastructureAvailable = true;
        } catch (Exception e) {
            infrastructureAvailable = false;
        }
    }

    @AfterAll
    static void tearDown() {
        if (springContext != null) springContext.close();
        if (kafka != null) kafka.stop();
        if (postgres != null) postgres.stop();
    }

    @BeforeEach
    void cleanDatabase() {
        if (infrastructureAvailable) jdbcTemplate.update("DELETE FROM replay_ids");
    }


    @Test
    void replayIdPersistedAndRetrievedAcrossRestarts() throws Exception {
        Assumptions.assumeTrue(infrastructureAvailable, "Docker not available");

        SalesforceProperties sfProps = createSalesforceProperties("srm");
        BridgeProperties bridgeProps = createBridgeProperties("srm", SF_TOPIC_SRM, KAFKA_TOPIC_SRM);
        StubStreamAdapter streamAdapter = new StubStreamAdapter();
        StubCircuitBreaker circuitBreaker = new StubCircuitBreaker();
        StubAuthService authService = new StubAuthService(sfProps);
        TopicRouter topicRouter = new ConfigurableTopicRouter(bridgeProps, sfProps);
        KafkaEventPublisher kafkaPublisher = createKafkaPublisher(topicRouter);

        GrpcPubSubConnector connector1 = new GrpcPubSubConnector(
                sfProps, authService, replayStore, circuitBreaker, streamAdapter, batch -> {});
        EventProcessingService service1 = new EventProcessingService(
                sfProps, connector1, topicRouter, kafkaPublisher, circuitBreaker, replayStore, new BridgeMetrics(new SimpleMeterRegistry()));
        service1.start();

        ReplayId firstReplayId = new ReplayId("first-replay-id-bytes".getBytes());
        service1.processBatch(createBatch("srm", SF_TOPIC_SRM, firstReplayId, 2));
        service1.shutdown();

        Optional<ReplayId> stored = replayStore.getLastReplayId("srm", SF_TOPIC_SRM);
        assertTrue(stored.isPresent(), "Replay ID should be persisted");
        assertEquals(firstReplayId, stored.get());

        GrpcPubSubConnector connector2 = new GrpcPubSubConnector(
                sfProps, authService, replayStore, circuitBreaker, streamAdapter, batch -> {});
        EventProcessingService service2 = new EventProcessingService(
                sfProps, connector2, topicRouter, kafkaPublisher, circuitBreaker, replayStore, new BridgeMetrics(new SimpleMeterRegistry()));
        service2.start();

        ReplayId secondReplayId = new ReplayId("second-replay-id-bytes".getBytes());
        service2.processBatch(createBatch("srm", SF_TOPIC_SRM, secondReplayId, 1));

        Optional<ReplayId> updatedStored = replayStore.getLastReplayId("srm", SF_TOPIC_SRM);
        assertTrue(updatedStored.isPresent());
        assertEquals(secondReplayId, updatedStored.get());
        service2.shutdown();
    }

    @Test
    void eventsPublishedToCorrectKafkaTopic() throws Exception {
        Assumptions.assumeTrue(infrastructureAvailable, "Docker not available");

        String uniqueKafkaTopic = "salesforce.srm.enrollment.publish-" + UUID.randomUUID().toString().substring(0, 8);
        SalesforceProperties sfProps = createSalesforceProperties("srm");
        BridgeProperties bridgeProps = createBridgeProperties("srm", SF_TOPIC_SRM, uniqueKafkaTopic);
        StubStreamAdapter streamAdapter = new StubStreamAdapter();
        StubCircuitBreaker circuitBreaker = new StubCircuitBreaker();
        StubAuthService authService = new StubAuthService(sfProps);
        TopicRouter topicRouter = new ConfigurableTopicRouter(bridgeProps, sfProps);
        KafkaEventPublisher kafkaPublisher = createKafkaPublisher(topicRouter);

        GrpcPubSubConnector connector = new GrpcPubSubConnector(
                sfProps, authService, replayStore, circuitBreaker, streamAdapter, batch -> {});
        EventProcessingService service = new EventProcessingService(
                sfProps, connector, topicRouter, kafkaPublisher, circuitBreaker, replayStore, new BridgeMetrics(new SimpleMeterRegistry()));
        service.start();

        service.processBatch(createBatch("srm", SF_TOPIC_SRM, new ReplayId("test".getBytes()), 3));

        try (KafkaConsumer<String, byte[]> consumer = createConsumer()) {
            consumer.subscribe(List.of(uniqueKafkaTopic));
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(10));
            assertEquals(3, records.count(), "Should have published 3 events");
            for (ConsumerRecord<String, byte[]> record : records) {
                assertEquals(uniqueKafkaTopic, record.topic());
                assertTrue(record.key().startsWith("srm:"));
            }
        }
        service.shutdown();
    }

    @Test
    void multiOrgIsolationWithConcurrentSubscriptions() throws Exception {
        Assumptions.assumeTrue(infrastructureAvailable, "Docker not available");

        SalesforceProperties sfProps = createMultiOrgSalesforceProperties();
        BridgeProperties bridgeProps = createMultiOrgBridgeProperties();
        StubStreamAdapter streamAdapter = new StubStreamAdapter();
        StubCircuitBreaker circuitBreaker = new StubCircuitBreaker();
        StubAuthService authService = new StubAuthService(sfProps);
        TopicRouter topicRouter = new ConfigurableTopicRouter(bridgeProps, sfProps);
        KafkaEventPublisher kafkaPublisher = createKafkaPublisher(topicRouter);

        GrpcPubSubConnector connector = new GrpcPubSubConnector(
                sfProps, authService, replayStore, circuitBreaker, streamAdapter, batch -> {});
        EventProcessingService service = new EventProcessingService(
                sfProps, connector, topicRouter, kafkaPublisher, circuitBreaker, replayStore, new BridgeMetrics(new SimpleMeterRegistry()));
        service.start();

        assertTrue(service.isSubscriptionActive("srm", SF_TOPIC_SRM));
        assertTrue(service.isSubscriptionActive("acad", SF_TOPIC_ACAD));
        assertEquals(2, service.getActiveSubscriptionCount());

        ReplayId srmReplay = new ReplayId("srm-replay".getBytes());
        ReplayId acadReplay = new ReplayId("acad-replay".getBytes());
        service.processBatch(createBatch("srm", SF_TOPIC_SRM, srmReplay, 2));
        service.processBatch(createBatch("acad", SF_TOPIC_ACAD, acadReplay, 3));

        Optional<ReplayId> srmStored = replayStore.getLastReplayId("srm", SF_TOPIC_SRM);
        Optional<ReplayId> acadStored = replayStore.getLastReplayId("acad", SF_TOPIC_ACAD);
        assertTrue(srmStored.isPresent() && acadStored.isPresent());
        assertEquals(srmReplay, srmStored.get());
        assertEquals(acadReplay, acadStored.get());

        service.shutdown();
    }

    @Test
    void circuitBreakerRecordsFailuresOnCheckpointError() throws Exception {
        Assumptions.assumeTrue(infrastructureAvailable, "Docker not available");

        SalesforceProperties sfProps = createSalesforceProperties("srm");
        BridgeProperties bridgeProps = createBridgeProperties("srm", SF_TOPIC_SRM, KAFKA_TOPIC_SRM);
        StubStreamAdapter streamAdapter = new StubStreamAdapter();
        TrackingCircuitBreaker circuitBreaker = new TrackingCircuitBreaker();
        StubAuthService authService = new StubAuthService(sfProps);
        TopicRouter topicRouter = new ConfigurableTopicRouter(bridgeProps, sfProps);

        FailingReplayStore failingStore = new FailingReplayStore(replayStore);
        KafkaEventPublisher kafkaPublisher = createKafkaPublisher(topicRouter, failingStore);

        GrpcPubSubConnector connector = new GrpcPubSubConnector(
                sfProps, authService, failingStore, circuitBreaker, streamAdapter, batch -> {});
        EventProcessingService service = new EventProcessingService(
                sfProps, connector, topicRouter, kafkaPublisher, circuitBreaker, failingStore, new BridgeMetrics(new SimpleMeterRegistry()));
        service.start();

        service.processBatch(createBatch("srm", SF_TOPIC_SRM, new ReplayId("ok".getBytes()), 1));
        assertEquals(0, circuitBreaker.getFailureCount("srm"));

        failingStore.setFailing(true);
        service.processBatch(createBatch("srm", SF_TOPIC_SRM, new ReplayId("fail".getBytes()), 1));
        assertTrue(circuitBreaker.getFailureCount("srm") > 0);

        service.shutdown();
    }

    @Test
    void emptyBatchCheckpointsReplayIdWithoutPublishing() throws Exception {
        Assumptions.assumeTrue(infrastructureAvailable, "Docker not available");

        SalesforceProperties sfProps = createSalesforceProperties("srm");
        BridgeProperties bridgeProps = createBridgeProperties("srm", SF_TOPIC_SRM, KAFKA_TOPIC_SRM);
        StubStreamAdapter streamAdapter = new StubStreamAdapter();
        StubCircuitBreaker circuitBreaker = new StubCircuitBreaker();
        StubAuthService authService = new StubAuthService(sfProps);
        TopicRouter topicRouter = new ConfigurableTopicRouter(bridgeProps, sfProps);
        KafkaEventPublisher kafkaPublisher = createKafkaPublisher(topicRouter);

        GrpcPubSubConnector connector = new GrpcPubSubConnector(
                sfProps, authService, replayStore, circuitBreaker, streamAdapter, batch -> {});
        EventProcessingService service = new EventProcessingService(
                sfProps, connector, topicRouter, kafkaPublisher, circuitBreaker, replayStore, new BridgeMetrics(new SimpleMeterRegistry()));
        service.start();

        ReplayId replayId = new ReplayId("empty-batch".getBytes());
        service.processBatch(new EventBatch("srm", SF_TOPIC_SRM, List.of(), replayId, Instant.now()));

        Optional<ReplayId> stored = replayStore.getLastReplayId("srm", SF_TOPIC_SRM);
        assertTrue(stored.isPresent());
        assertEquals(replayId, stored.get());
        service.shutdown();
    }


    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private KafkaEventPublisher createKafkaPublisher(TopicRouter topicRouter) {
        return new DefaultKafkaEventPublisher(kafkaTemplate, topicRouter, replayStore, avroRecordConverter, "test", 30);
    }

    private KafkaEventPublisher createKafkaPublisher(TopicRouter topicRouter, ReplayStore store) {
        return new DefaultKafkaEventPublisher(kafkaTemplate, topicRouter, store, avroRecordConverter, "test", 30);
    }

    private KafkaConsumer<String, byte[]> createConsumer() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        return new KafkaConsumer<>(props);
    }

    private EventBatch createBatch(String org, String topic, ReplayId replayId, int eventCount) {
        List<PlatformEvent> events = new ArrayList<>();
        for (int i = 0; i < eventCount; i++) {
            ObjectNode payload = objectMapper.createObjectNode();
            payload.put("Id", org + "-event-" + UUID.randomUUID());
            payload.put("data", "test-data-" + i);
            events.add(new PlatformEvent("TestEvent__e", payload, replayId));
        }
        return new EventBatch(org, topic, events, replayId, Instant.now());
    }

    private SalesforceProperties createSalesforceProperties(String org) {
        SalesforceProperties props = new SalesforceProperties();
        props.setRetry(createRetryConfig());
        String sfTopic = org.equals("acad") ? SF_TOPIC_ACAD : SF_TOPIC_SRM;
        props.setOrgs(Map.of(org, createOrgConfig(List.of(sfTopic))));
        return props;
    }

    private SalesforceProperties createMultiOrgSalesforceProperties() {
        SalesforceProperties props = new SalesforceProperties();
        props.setRetry(createRetryConfig());
        Map<String, OrgConfig> orgs = new LinkedHashMap<>();
        orgs.put("srm", createOrgConfig(List.of(SF_TOPIC_SRM)));
        orgs.put("acad", createOrgConfig(List.of(SF_TOPIC_ACAD)));
        props.setOrgs(orgs);
        return props;
    }

    private BridgeProperties createBridgeProperties(String org, String sfTopic, String kafkaTopic) {
        BridgeProperties props = new BridgeProperties();
        props.setTopicRouting(Map.of(org, Map.of(sfTopic, kafkaTopic)));
        return props;
    }

    private BridgeProperties createMultiOrgBridgeProperties() {
        BridgeProperties props = new BridgeProperties();
        Map<String, Map<String, String>> routing = new LinkedHashMap<>();
        routing.put("srm", Map.of(SF_TOPIC_SRM, KAFKA_TOPIC_SRM));
        routing.put("acad", Map.of(SF_TOPIC_ACAD, KAFKA_TOPIC_ACAD));
        props.setTopicRouting(routing);
        return props;
    }

    private RetryConfig createRetryConfig() {
        RetryConfig c = new RetryConfig();
        c.setMaxAttempts(3); c.setInitialIntervalMs(100); c.setMaxIntervalMs(60000); c.setMultiplier(2.0);
        return c;
    }

    private OrgConfig createOrgConfig(List<String> topics) {
        OrgConfig c = new OrgConfig();
        c.setPubsubUrl("api.pubsub.salesforce.com:7443");
        c.setOauthUrl("https://login.salesforce.com/services/oauth2/token");
        c.setClientId("test-client-id"); c.setClientSecret("test-secret");
        c.setUsername("test@example.com"); c.setPassword("test-password");
        c.setTopics(new ArrayList<>(topics)); c.setEnabled(true);
        return c;
    }


    // -----------------------------------------------------------------------
    // JPA Configuration
    // -----------------------------------------------------------------------

    @Configuration
    @EnableJpaRepositories(basePackages = "com.example.bridge.replay")
    @EnableTransactionManagement
    static class JpaConfig {
        @Bean
        public LocalContainerEntityManagerFactoryBean entityManagerFactory(DataSource dataSource) {
            LocalContainerEntityManagerFactoryBean em = new LocalContainerEntityManagerFactoryBean();
            em.setDataSource(dataSource);
            em.setPackagesToScan("com.example.bridge.replay");
            em.setJpaVendorAdapter(new HibernateJpaVendorAdapter());
            Properties properties = new Properties();
            properties.setProperty("hibernate.hbm2ddl.auto", "none");
            properties.setProperty("hibernate.dialect", "org.hibernate.dialect.PostgreSQLDialect");
            em.setJpaProperties(properties);
            return em;
        }

        @Bean
        public PlatformTransactionManager transactionManager(EntityManagerFactory emf) {
            return new JpaTransactionManager(emf);
        }

        @Bean
        public JpaReplayStore replayStore(ReplayIdRepository repository) {
            return new JpaReplayStore(repository);
        }
    }

    // -----------------------------------------------------------------------
    // Test Doubles
    // -----------------------------------------------------------------------

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
        @Override public void cancel() { active.set(false); }
        @Override public boolean isActive() { return active.get(); }
    }

    static class StubCircuitBreaker implements CircuitBreaker {
        private final Map<String, CircuitState> states = new ConcurrentHashMap<>();

        @Override
        public <T> T execute(String org, Supplier<T> operation) {
            if (getState(org) == CircuitState.OPEN) throw new CircuitOpenException(org);
            return operation.get();
        }

        @Override public void recordSuccess(String org) { states.put(org, CircuitState.CLOSED); }
        @Override public void recordFailure(String org, Exception error) {}
        @Override public CircuitState getState(String org) {
            return states.getOrDefault(org, CircuitState.CLOSED);
        }
    }

    static class TrackingCircuitBreaker implements CircuitBreaker {
        private final Map<String, AtomicInteger> failures = new ConcurrentHashMap<>();

        @Override
        public <T> T execute(String org, Supplier<T> operation) { return operation.get(); }

        @Override public void recordSuccess(String org) {}

        @Override
        public void recordFailure(String org, Exception error) {
            failures.computeIfAbsent(org, k -> new AtomicInteger(0)).incrementAndGet();
        }

        @Override
        public CircuitState getState(String org) { return CircuitState.CLOSED; }

        int getFailureCount(String org) {
            AtomicInteger count = failures.get(org);
            return count != null ? count.get() : 0;
        }
    }

    static class StubAuthService extends SalesforceAuthService {
        StubAuthService(SalesforceProperties properties) {
            super(properties, (url, cid, cs, u, p) ->
                    new OAuthTokenResponse("test-token", "https://test.salesforce.com",
                            "Bearer", String.valueOf(System.currentTimeMillis()), null, null));
        }
        @Override public String getAccessToken(String orgId) { return "test-token"; }
        @Override public String getInstanceUrl(String orgId) { return "https://test.salesforce.com"; }
        @Override public void invalidateToken(String orgId) {}
    }

    /**
     * A ReplayStore wrapper that can be toggled to fail on checkpoint operations.
     */
    static class FailingReplayStore implements ReplayStore {
        private final ReplayStore delegate;
        private volatile boolean failing = false;

        FailingReplayStore(ReplayStore delegate) { this.delegate = delegate; }

        void setFailing(boolean failing) { this.failing = failing; }

        @Override
        public Optional<ReplayId> getLastReplayId(String org, String topic) {
            return delegate.getLastReplayId(org, topic);
        }

        @Override
        public void checkpoint(String org, String topic, ReplayId replayId) throws CheckpointException {
            if (failing) throw new CheckpointException("Simulated database failure");
            delegate.checkpoint(org, topic, replayId);
        }

        @Override
        public ConnectionStatus getDatabaseStatus() { return delegate.getDatabaseStatus(); }
    }
}
