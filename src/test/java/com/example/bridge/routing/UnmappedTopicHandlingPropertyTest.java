package com.example.bridge.routing;

import com.example.bridge.config.BridgeProperties;
import com.example.bridge.config.OrgConfig;
import com.example.bridge.config.SalesforceProperties;
import net.jqwik.api.*;
import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Property 17: Unmapped Topic Handling
 *
 * For any event received for a topic with no configured mapping, the bridge
 * should log a warning and discard without checkpointing.
 *
 * Validates: Requirements 6.3
 */
class UnmappedTopicHandlingPropertyTest {

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
                "/event/Test_Event__e",
                "/event/Unmapped_Event__e"
        );
    }

    @Provide
    Arbitrary<String> kafkaTopics() {
        return Arbitraries.of(
                "salesforce.srm.enrollment",
                "salesforce.srm.student",
                "salesforce.acad.course"
        );
    }

    // -----------------------------------------------------------------------
    // Property: Unmapped topic returns empty and logs warning
    // -----------------------------------------------------------------------

    @Property(tries = 100)
    void unmappedTopicReturnsEmptyOptional(
            @ForAll("orgIdentifiers") String org,
            @ForAll("salesforceTopics") String mappedTopic,
            @ForAll("salesforceTopics") String unmappedTopic,
            @ForAll("kafkaTopics") String kafkaTopic) throws Exception {

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

        // Property: Unmapped topic returns empty Optional (event should be discarded)
        Optional<String> result = router.getKafkaTopic(org, unmappedTopic);

        assertFalse(result.isPresent(),
                "Unmapped topic should return empty Optional, indicating event will be discarded");
    }

    @Property(tries = 100)
    void unmappedTopicLogsWarning(
            @ForAll("orgIdentifiers") String org,
            @ForAll("salesforceTopics") String unmappedTopic) throws Exception {

        // Setup: Create routing with no mapping for this topic
        Map<String, String> orgMapping = new HashMap<>();
        // Intentionally empty - no mappings

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
        config.setTopics(List.of(unmappedTopic));
        config.setEnabled(true);
        
        orgConfigs.put(org, config);
        sfProps.setOrgs(orgConfigs);

        // Setup: Capture log output
        Logger logger = (Logger) LoggerFactory.getLogger(ConfigurableTopicRouter.class);
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.start();
        logger.addAppender(listAppender);

        try {
            // Create router
            ConfigurableTopicRouter router = new ConfigurableTopicRouter(bridgeProps, sfProps);
            router.validateConfiguration();

            // Clear logs from initialization
            listAppender.list.clear();

            // Property: Calling getKafkaTopic for unmapped topic should log warning
            Optional<String> result = router.getKafkaTopic(org, unmappedTopic);

            assertFalse(result.isPresent(), "Unmapped topic should return empty");

            // Verify warning was logged
            List<ILoggingEvent> logsList = listAppender.list;
            boolean warningLogged = logsList.stream()
                    .anyMatch(event -> 
                            event.getLevel().toString().equals("WARN") &&
                            event.getFormattedMessage().contains("No Kafka topic mapping found") &&
                            event.getFormattedMessage().contains(org) &&
                            event.getFormattedMessage().contains(unmappedTopic) &&
                            event.getFormattedMessage().contains("discarded"));

            assertTrue(warningLogged,
                    "Warning should be logged when unmapped topic is encountered, " +
                    "indicating event will be discarded without checkpointing");
        } finally {
            logger.detachAppender(listAppender);
        }
    }

    @Property(tries = 100)
    void unmappedOrgReturnsEmptyAndLogsWarning(
            @ForAll("orgIdentifiers") String configuredOrg,
            @ForAll("orgIdentifiers") String unmappedOrg,
            @ForAll("salesforceTopics") String salesforceTopic,
            @ForAll("kafkaTopics") String kafkaTopic) throws Exception {

        Assume.that(!configuredOrg.equals(unmappedOrg));

        // Setup: Create routing for only one org
        Map<String, String> orgMapping = new HashMap<>();
        orgMapping.put(salesforceTopic, kafkaTopic);

        Map<String, Map<String, String>> routing = new HashMap<>();
        routing.put(configuredOrg, orgMapping);

        BridgeProperties bridgeProps = new BridgeProperties();
        bridgeProps.setTopicRouting(routing);

        // Setup: Create Salesforce properties with both orgs
        SalesforceProperties sfProps = new SalesforceProperties();
        Map<String, OrgConfig> orgConfigs = new HashMap<>();
        
        for (String org : List.of(configuredOrg, unmappedOrg)) {
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
        }
        
        sfProps.setOrgs(orgConfigs);

        // Setup: Capture log output
        Logger logger = (Logger) LoggerFactory.getLogger(ConfigurableTopicRouter.class);
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.start();
        logger.addAppender(listAppender);

        try {
            // Create router
            ConfigurableTopicRouter router = new ConfigurableTopicRouter(bridgeProps, sfProps);
            router.validateConfiguration();

            // Clear logs from initialization
            listAppender.list.clear();

            // Property: Unmapped org returns empty and logs warning
            Optional<String> result = router.getKafkaTopic(unmappedOrg, salesforceTopic);

            assertFalse(result.isPresent(),
                    "Unmapped org should return empty Optional");

            // Verify warning was logged
            List<ILoggingEvent> logsList = listAppender.list;
            boolean warningLogged = logsList.stream()
                    .anyMatch(event -> 
                            event.getLevel().toString().equals("WARN") &&
                            event.getFormattedMessage().contains("No routing configuration found") &&
                            event.getFormattedMessage().contains(unmappedOrg));

            assertTrue(warningLogged,
                    "Warning should be logged when event received for org with no routing configuration");
        } finally {
            logger.detachAppender(listAppender);
        }
    }

    @Property(tries = 50)
    void nullInputsReturnEmptyAndLogWarning(
            @ForAll("orgIdentifiers") String org,
            @ForAll("salesforceTopics") String topic,
            @ForAll("kafkaTopics") String kafkaTopic) throws Exception {

        // Setup: Create valid routing
        Map<String, String> orgMapping = new HashMap<>();
        orgMapping.put(topic, kafkaTopic);

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
        config.setTopics(List.of(topic));
        config.setEnabled(true);
        
        orgConfigs.put(org, config);
        sfProps.setOrgs(orgConfigs);

        // Create router
        ConfigurableTopicRouter router = new ConfigurableTopicRouter(bridgeProps, sfProps);
        router.validateConfiguration();

        // Property: Null inputs should return empty
        Optional<String> result1 = router.getKafkaTopic(null, topic);
        Optional<String> result2 = router.getKafkaTopic(org, null);
        Optional<String> result3 = router.getKafkaTopic(null, null);

        assertFalse(result1.isPresent(), "Null org should return empty");
        assertFalse(result2.isPresent(), "Null topic should return empty");
        assertFalse(result3.isPresent(), "Null org and topic should return empty");
    }
}
