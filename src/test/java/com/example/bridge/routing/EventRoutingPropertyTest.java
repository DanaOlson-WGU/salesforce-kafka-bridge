package com.example.bridge.routing;

import com.example.bridge.config.BridgeProperties;
import com.example.bridge.config.OrgConfig;
import com.example.bridge.config.SalesforceProperties;
import com.example.bridge.model.EventBatch;
import com.example.bridge.model.PlatformEvent;
import com.example.bridge.model.ReplayId;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.jqwik.api.*;

import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Property 5: Event Routing
 *
 * For any event batch, each event should be published to the Kafka topic
 * determined by the routing configuration.
 *
 * Validates: Requirements 2.1
 */
class EventRoutingPropertyTest {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    // -----------------------------------------------------------------------
    // Generators
    // -----------------------------------------------------------------------

    @Provide
    Arbitrary<String> orgIdentifiers() {
        return Arbitraries.of("srm", "acad", "test");
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
    Arbitrary<String> kafkaTopics() {
        return Arbitraries.of(
                "salesforce.srm.enrollment",
                "salesforce.srm.student",
                "salesforce.acad.course",
                "salesforce.test.event"
        );
    }

    @Provide
    Arbitrary<ReplayId> replayIds() {
        return Arbitraries.bytes()
                .array(byte[].class)
                .ofMinSize(16)
                .ofMaxSize(32)
                .map(ReplayId::new);
    }

    @Provide
    Arbitrary<JsonNode> eventPayloads() {
        return Arbitraries.maps(
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(20),
                Arbitraries.oneOf(
                        Arbitraries.strings().alpha().ofMaxLength(50),
                        Arbitraries.integers(),
                        Arbitraries.doubles()
                )
        ).ofMinSize(1).ofMaxSize(5).map(map -> objectMapper.valueToTree(map));
    }

    @Provide
    Arbitrary<PlatformEvent> platformEvent() {
        return Combinators.combine(
                Arbitraries.strings().alpha().ofMinLength(5).ofMaxLength(30),
                eventPayloads(),
                replayIds()
        ).as(PlatformEvent::new);
    }

    @Provide
    Arbitrary<List<PlatformEvent>> platformEvents() {
        return platformEvent().list().ofMinSize(1).ofMaxSize(10);
    }

    @Provide
    Arbitrary<EventBatch> eventBatches() {
        return Combinators.combine(
                orgIdentifiers(),
                salesforceTopics(),
                platformEvents(),
                replayIds(),
                Arbitraries.longs().map(Instant::ofEpochMilli)
        ).as(EventBatch::new);
    }

    // -----------------------------------------------------------------------
    // Property: Event routing based on configuration
    // -----------------------------------------------------------------------

    @Property(tries = 100)
    void eventBatchRoutedToConfiguredKafkaTopic(
            @ForAll("orgIdentifiers") String org,
            @ForAll("salesforceTopics") String salesforceTopic,
            @ForAll("kafkaTopics") String expectedKafkaTopic,
            @ForAll("platformEvents") List<PlatformEvent> events,
            @ForAll("replayIds") ReplayId batchReplayId) throws Exception {

        Assume.that(!events.isEmpty());

        // Setup: Create routing configuration
        Map<String, String> orgMapping = new HashMap<>();
        orgMapping.put(salesforceTopic, expectedKafkaTopic);

        Map<String, Map<String, String>> routing = new HashMap<>();
        routing.put(org, orgMapping);

        BridgeProperties bridgeProps = new BridgeProperties();
        bridgeProps.setTopicRouting(routing);

        // Setup: Create Salesforce properties
        SalesforceProperties sfProps = new SalesforceProperties();
        Map<String, OrgConfig> orgConfigs = new HashMap<>();
        
        OrgConfig config = new OrgConfig();
        config.setPubsubUrl("api.pubsub.salesforce.com:7443");
        config.setOauthUrl("https://login.salesforce.com/services/oauth2/token");
        config.setClientId("test-client-id");
        config.setClientSecret("test-client-secret");
        config.setUsername("test@example.com");
        config.setPassword("test-password");
        config.setTopics(List.of(salesforceTopic));
        config.setEnabled(true);
        
        orgConfigs.put(org, config);
        sfProps.setOrgs(orgConfigs);

        // Create router
        ConfigurableTopicRouter router = new ConfigurableTopicRouter(bridgeProps, sfProps);
        router.validateConfiguration();

        // Create event batch
        EventBatch batch = new EventBatch(org, salesforceTopic, events, batchReplayId, Instant.now());

        // Property: For the event batch, the router should return the configured Kafka topic
        Optional<String> routedKafkaTopic = router.getKafkaTopic(batch.getOrg(), batch.getSalesforceTopic());

        assertTrue(routedKafkaTopic.isPresent(),
                "Router should return Kafka topic for configured org and Salesforce topic");

        assertEquals(expectedKafkaTopic, routedKafkaTopic.get(),
                "Each event in the batch should be routed to the Kafka topic determined by routing configuration");

        // Property: All events in the batch should be routed to the same Kafka topic
        for (PlatformEvent event : batch.getEvents()) {
            Optional<String> eventKafkaTopic = router.getKafkaTopic(batch.getOrg(), batch.getSalesforceTopic());
            assertTrue(eventKafkaTopic.isPresent());
            assertEquals(expectedKafkaTopic, eventKafkaTopic.get(),
                    "All events in batch should route to same Kafka topic");
        }
    }

    @Property(tries = 100)
    void multipleEventBatchesRoutedIndependently(
            @ForAll("orgIdentifiers") String org1,
            @ForAll("orgIdentifiers") String org2,
            @ForAll("salesforceTopics") String topic1,
            @ForAll("salesforceTopics") String topic2,
            @ForAll("kafkaTopics") String kafkaTopic1,
            @ForAll("kafkaTopics") String kafkaTopic2,
            @ForAll("platformEvents") List<PlatformEvent> events1,
            @ForAll("platformEvents") List<PlatformEvent> events2,
            @ForAll("replayIds") ReplayId replayId1,
            @ForAll("replayIds") ReplayId replayId2) throws Exception {

        Assume.that(!events1.isEmpty() && !events2.isEmpty());
        Assume.that(!org1.equals(org2) || !topic1.equals(topic2));

        // Setup: Create routing configuration for both org/topic combinations
        Map<String, String> org1Mapping = new HashMap<>();
        org1Mapping.put(topic1, kafkaTopic1);
        
        Map<String, String> org2Mapping;
        if (org1.equals(org2)) {
            // Same org, add both topics to same mapping
            org1Mapping.put(topic2, kafkaTopic2);
            org2Mapping = org1Mapping;
        } else {
            // Different orgs, create separate mappings
            org2Mapping = new HashMap<>();
            org2Mapping.put(topic2, kafkaTopic2);
        }

        Map<String, Map<String, String>> routing = new HashMap<>();
        routing.put(org1, org1Mapping);
        if (!org1.equals(org2)) {
            routing.put(org2, org2Mapping);
        }

        BridgeProperties bridgeProps = new BridgeProperties();
        bridgeProps.setTopicRouting(routing);

        // Setup: Create Salesforce properties
        SalesforceProperties sfProps = new SalesforceProperties();
        Map<String, OrgConfig> orgConfigs = new HashMap<>();
        
        // Add org1
        OrgConfig config1 = new OrgConfig();
        config1.setPubsubUrl("api.pubsub.salesforce.com:7443");
        config1.setOauthUrl("https://login.salesforce.com/services/oauth2/token");
        config1.setClientId("test-client-id");
        config1.setClientSecret("test-client-secret");
        config1.setUsername("test@example.com");
        config1.setPassword("test-password");
        config1.setTopics(List.of(topic1));
        config1.setEnabled(true);
        orgConfigs.put(org1, config1);
        
        // Add org2 if different from org1
        if (!org1.equals(org2)) {
            OrgConfig config2 = new OrgConfig();
            config2.setPubsubUrl("api.pubsub.salesforce.com:7443");
            config2.setOauthUrl("https://login.salesforce.com/services/oauth2/token");
            config2.setClientId("test-client-id");
            config2.setClientSecret("test-client-secret");
            config2.setUsername("test@example.com");
            config2.setPassword("test-password");
            config2.setTopics(List.of(topic2));
            config2.setEnabled(true);
            orgConfigs.put(org2, config2);
        } else {
            // Same org, add topic2 to existing config
            config1.setTopics(List.of(topic1, topic2));
        }
        
        sfProps.setOrgs(orgConfigs);

        // Create router
        ConfigurableTopicRouter router = new ConfigurableTopicRouter(bridgeProps, sfProps);
        router.validateConfiguration();

        // Create event batches
        EventBatch batch1 = new EventBatch(org1, topic1, events1, replayId1, Instant.now());
        EventBatch batch2 = new EventBatch(org2, topic2, events2, replayId2, Instant.now());

        // Property: Each batch should be routed to its configured Kafka topic independently
        Optional<String> routedTopic1 = router.getKafkaTopic(batch1.getOrg(), batch1.getSalesforceTopic());
        Optional<String> routedTopic2 = router.getKafkaTopic(batch2.getOrg(), batch2.getSalesforceTopic());

        assertTrue(routedTopic1.isPresent(), "Batch 1 should have routing");
        assertTrue(routedTopic2.isPresent(), "Batch 2 should have routing");

        assertEquals(kafkaTopic1, routedTopic1.get(),
                "Batch 1 should route to its configured Kafka topic");
        assertEquals(kafkaTopic2, routedTopic2.get(),
                "Batch 2 should route to its configured Kafka topic");
    }

    @Property(tries = 100)
    void emptyBatchStillRoutedCorrectly(
            @ForAll("orgIdentifiers") String org,
            @ForAll("salesforceTopics") String salesforceTopic,
            @ForAll("kafkaTopics") String expectedKafkaTopic,
            @ForAll("replayIds") ReplayId batchReplayId) throws Exception {

        // Setup: Create routing configuration
        Map<String, String> orgMapping = new HashMap<>();
        orgMapping.put(salesforceTopic, expectedKafkaTopic);

        Map<String, Map<String, String>> routing = new HashMap<>();
        routing.put(org, orgMapping);

        BridgeProperties bridgeProps = new BridgeProperties();
        bridgeProps.setTopicRouting(routing);

        // Setup: Create Salesforce properties
        SalesforceProperties sfProps = new SalesforceProperties();
        Map<String, OrgConfig> orgConfigs = new HashMap<>();
        
        OrgConfig config = new OrgConfig();
        config.setPubsubUrl("api.pubsub.salesforce.com:7443");
        config.setOauthUrl("https://login.salesforce.com/services/oauth2/token");
        config.setClientId("test-client-id");
        config.setClientSecret("test-client-secret");
        config.setUsername("test@example.com");
        config.setPassword("test-password");
        config.setTopics(List.of(salesforceTopic));
        config.setEnabled(true);
        
        orgConfigs.put(org, config);
        sfProps.setOrgs(orgConfigs);

        // Create router
        ConfigurableTopicRouter router = new ConfigurableTopicRouter(bridgeProps, sfProps);
        router.validateConfiguration();

        // Create empty event batch
        EventBatch emptyBatch = new EventBatch(org, salesforceTopic, Collections.emptyList(), 
                batchReplayId, Instant.now());

        assertTrue(emptyBatch.isEmpty(), "Batch should be empty");

        // Property: Even empty batches should be routed correctly (for checkpointing)
        Optional<String> routedKafkaTopic = router.getKafkaTopic(emptyBatch.getOrg(), 
                emptyBatch.getSalesforceTopic());

        assertTrue(routedKafkaTopic.isPresent(),
                "Router should return Kafka topic even for empty batches");

        assertEquals(expectedKafkaTopic, routedKafkaTopic.get(),
                "Empty batch should route to configured Kafka topic for checkpointing");
    }

    @Property(tries = 100)
    void unmappedBatchReturnsEmpty(
            @ForAll("orgIdentifiers") String org,
            @ForAll("salesforceTopics") String mappedTopic,
            @ForAll("salesforceTopics") String unmappedTopic,
            @ForAll("kafkaTopics") String kafkaTopic,
            @ForAll("platformEvents") List<PlatformEvent> events,
            @ForAll("replayIds") ReplayId batchReplayId) throws Exception {

        Assume.that(!events.isEmpty());
        Assume.that(!mappedTopic.equals(unmappedTopic));

        // Setup: Create routing with only one topic mapped
        Map<String, String> orgMapping = new HashMap<>();
        orgMapping.put(mappedTopic, kafkaTopic);

        Map<String, Map<String, String>> routing = new HashMap<>();
        routing.put(org, orgMapping);

        BridgeProperties bridgeProps = new BridgeProperties();
        bridgeProps.setTopicRouting(routing);

        // Setup: Create Salesforce properties
        SalesforceProperties sfProps = new SalesforceProperties();
        Map<String, OrgConfig> orgConfigs = new HashMap<>();
        
        OrgConfig config = new OrgConfig();
        config.setPubsubUrl("api.pubsub.salesforce.com:7443");
        config.setOauthUrl("https://login.salesforce.com/services/oauth2/token");
        config.setClientId("test-client-id");
        config.setClientSecret("test-client-secret");
        config.setUsername("test@example.com");
        config.setPassword("test-password");
        config.setTopics(List.of(mappedTopic, unmappedTopic));
        config.setEnabled(true);
        
        orgConfigs.put(org, config);
        sfProps.setOrgs(orgConfigs);

        // Create router
        ConfigurableTopicRouter router = new ConfigurableTopicRouter(bridgeProps, sfProps);
        router.validateConfiguration();

        // Create batch for unmapped topic
        EventBatch unmappedBatch = new EventBatch(org, unmappedTopic, events, batchReplayId, Instant.now());

        // Property: Unmapped batch should return empty (events will be discarded)
        Optional<String> routedKafkaTopic = router.getKafkaTopic(unmappedBatch.getOrg(), 
                unmappedBatch.getSalesforceTopic());

        assertFalse(routedKafkaTopic.isPresent(),
                "Unmapped batch should return empty, indicating events will be discarded without publishing");
    }
}
