package com.example.bridge.routing;

import com.example.bridge.config.BridgeProperties;
import com.example.bridge.config.OrgConfig;
import com.example.bridge.config.SalesforceProperties;
import net.jqwik.api.*;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Property 16: One-to-One Topic Mapping
 *
 * For any org and Salesforce topic in routing config, there should be exactly
 * one corresponding Kafka topic.
 *
 * Validates: Requirements 6.2
 */
class OneToOneTopicMappingPropertyTest {

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
    Arbitrary<Map<String, Map<String, String>>> topicRoutingConfig() {
        return Combinators.combine(
                orgIdentifiers(),
                salesforceTopics(),
                kafkaTopics()
        ).as((org, sfTopic, kafkaTopic) -> {
            Map<String, String> orgMapping = new HashMap<>();
            orgMapping.put(sfTopic, kafkaTopic);
            
            Map<String, Map<String, String>> routing = new HashMap<>();
            routing.put(org, orgMapping);
            
            return routing;
        });
    }

    @Provide
    Arbitrary<SalesforceProperties> salesforcePropertiesWithOrgs() {
        return orgIdentifiers().list().ofMinSize(1).ofMaxSize(3).map(orgs -> {
            SalesforceProperties props = new SalesforceProperties();
            Map<String, OrgConfig> orgConfigs = new HashMap<>();
            
            for (String org : orgs) {
                OrgConfig config = new OrgConfig();
                config.setPubsubUrl("api.pubsub.salesforce.com:7443");
                config.setOauthUrl("https://login.salesforce.com/services/oauth2/token");
                config.setClientId("test-client-id");
                config.setClientSecret("test-client-secret");
                config.setUsername("test@example.com");
                config.setPassword("test-password");
                config.setTopics(List.of("/event/Test_Event__e"));
                config.setEnabled(true);
                
                orgConfigs.put(org, config);
            }
            
            props.setOrgs(orgConfigs);
            return props;
        });
    }

    // -----------------------------------------------------------------------
    // Property: One-to-one topic mapping
    // -----------------------------------------------------------------------

    @Property(tries = 100)
    void eachOrgAndSalesforceTopicMapsToExactlyOneKafkaTopic(
            @ForAll("orgIdentifiers") String org,
            @ForAll("salesforceTopics") String salesforceTopic,
            @ForAll("kafkaTopics") String kafkaTopic) throws Exception {

        // Setup: Create routing configuration with one-to-one mapping
        Map<String, String> orgMapping = new HashMap<>();
        orgMapping.put(salesforceTopic, kafkaTopic);

        Map<String, Map<String, String>> routing = new HashMap<>();
        routing.put(org, orgMapping);

        BridgeProperties bridgeProps = new BridgeProperties();
        bridgeProps.setTopicRouting(routing);

        // Setup: Create Salesforce properties with the org
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

        // Property: For the given org and Salesforce topic, there should be exactly one Kafka topic
        Optional<String> result1 = router.getKafkaTopic(org, salesforceTopic);
        Optional<String> result2 = router.getKafkaTopic(org, salesforceTopic);

        assertTrue(result1.isPresent(), "Kafka topic should be present for configured mapping");
        assertTrue(result2.isPresent(), "Kafka topic should be present on repeated lookup");
        
        assertEquals(result1.get(), result2.get(),
                "Multiple lookups should return the same Kafka topic (one-to-one mapping)");
        
        assertEquals(kafkaTopic, result1.get(),
                "Returned Kafka topic should match configured mapping");
    }

    @Property(tries = 100)
    void unmappedTopicReturnsEmpty(
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

        // Property: Mapped topic returns Kafka topic
        Optional<String> mappedResult = router.getKafkaTopic(org, mappedTopic);
        assertTrue(mappedResult.isPresent(), "Mapped topic should return Kafka topic");
        assertEquals(kafkaTopic, mappedResult.get());

        // Property: Unmapped topic returns empty
        Optional<String> unmappedResult = router.getKafkaTopic(org, unmappedTopic);
        assertFalse(unmappedResult.isPresent(),
                "Unmapped topic should return empty Optional");
    }

    @Property(tries = 100)
    void multipleOrgsHaveIndependentMappings(
            @ForAll("orgIdentifiers") String org1,
            @ForAll("orgIdentifiers") String org2,
            @ForAll("salesforceTopics") String salesforceTopic,
            @ForAll("kafkaTopics") String kafkaTopic1,
            @ForAll("kafkaTopics") String kafkaTopic2) throws Exception {

        Assume.that(!org1.equals(org2));
        Assume.that(!kafkaTopic1.equals(kafkaTopic2));

        // Setup: Create routing with different Kafka topics for different orgs
        Map<String, String> org1Mapping = new HashMap<>();
        org1Mapping.put(salesforceTopic, kafkaTopic1);

        Map<String, String> org2Mapping = new HashMap<>();
        org2Mapping.put(salesforceTopic, kafkaTopic2);

        Map<String, Map<String, String>> routing = new HashMap<>();
        routing.put(org1, org1Mapping);
        routing.put(org2, org2Mapping);

        BridgeProperties bridgeProps = new BridgeProperties();
        bridgeProps.setTopicRouting(routing);

        // Setup: Create Salesforce properties with both orgs
        SalesforceProperties sfProps = new SalesforceProperties();
        Map<String, OrgConfig> orgConfigs = new HashMap<>();
        
        for (String org : List.of(org1, org2)) {
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

        // Create router
        ConfigurableTopicRouter router = new ConfigurableTopicRouter(bridgeProps, sfProps);
        router.validateConfiguration();

        // Property: Each org has independent one-to-one mapping
        Optional<String> result1 = router.getKafkaTopic(org1, salesforceTopic);
        Optional<String> result2 = router.getKafkaTopic(org2, salesforceTopic);

        assertTrue(result1.isPresent(), "Org1 should have mapping");
        assertTrue(result2.isPresent(), "Org2 should have mapping");
        
        assertEquals(kafkaTopic1, result1.get(),
                "Org1 should map to its configured Kafka topic");
        assertEquals(kafkaTopic2, result2.get(),
                "Org2 should map to its configured Kafka topic");
        
        assertNotEquals(result1.get(), result2.get(),
                "Different orgs should have independent mappings");
    }
}
