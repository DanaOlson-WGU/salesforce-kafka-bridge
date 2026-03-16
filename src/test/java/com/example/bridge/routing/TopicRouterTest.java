package com.example.bridge.routing;

import com.example.bridge.config.BridgeProperties;
import com.example.bridge.config.OrgConfig;
import com.example.bridge.config.SalesforceProperties;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for TopicRouter implementation.
 * Tests valid routing, unmapped topics, startup validation, and configuration loading.
 *
 * Requirements: 6.1, 6.2, 6.3, 6.4
 */
class TopicRouterTest {

    // -----------------------------------------------------------------------
    // Test: Valid routing returns correct Kafka topic
    // -----------------------------------------------------------------------

    @Test
    void validRoutingReturnsCorrectKafkaTopic() throws Exception {
        // Setup
        Map<String, String> srmMapping = new HashMap<>();
        srmMapping.put("/event/Enrollment_Event__e", "salesforce.srm.enrollment");
        srmMapping.put("/event/Student_Event__e", "salesforce.srm.student");

        Map<String, Map<String, String>> routing = new HashMap<>();
        routing.put("srm", srmMapping);

        BridgeProperties bridgeProps = new BridgeProperties();
        bridgeProps.setTopicRouting(routing);

        SalesforceProperties sfProps = createSalesforceProperties("srm");

        ConfigurableTopicRouter router = new ConfigurableTopicRouter(bridgeProps, sfProps);
        router.validateConfiguration();

        // Test
        Optional<String> result1 = router.getKafkaTopic("srm", "/event/Enrollment_Event__e");
        Optional<String> result2 = router.getKafkaTopic("srm", "/event/Student_Event__e");

        // Verify
        assertTrue(result1.isPresent());
        assertEquals("salesforce.srm.enrollment", result1.get());

        assertTrue(result2.isPresent());
        assertEquals("salesforce.srm.student", result2.get());
    }

    // -----------------------------------------------------------------------
    // Test: Unmapped topic returns empty Optional
    // -----------------------------------------------------------------------

    @Test
    void unmappedTopicReturnsEmptyOptional() throws Exception {
        // Setup
        Map<String, String> srmMapping = new HashMap<>();
        srmMapping.put("/event/Enrollment_Event__e", "salesforce.srm.enrollment");

        Map<String, Map<String, String>> routing = new HashMap<>();
        routing.put("srm", srmMapping);

        BridgeProperties bridgeProps = new BridgeProperties();
        bridgeProps.setTopicRouting(routing);

        SalesforceProperties sfProps = createSalesforceProperties("srm");

        ConfigurableTopicRouter router = new ConfigurableTopicRouter(bridgeProps, sfProps);
        router.validateConfiguration();

        // Test - unmapped topic
        Optional<String> result = router.getKafkaTopic("srm", "/event/Unmapped_Event__e");

        // Verify
        assertFalse(result.isPresent(), "Unmapped topic should return empty Optional");
    }

    @Test
    void unmappedOrgReturnsEmptyOptional() throws Exception {
        // Setup
        Map<String, String> srmMapping = new HashMap<>();
        srmMapping.put("/event/Enrollment_Event__e", "salesforce.srm.enrollment");

        Map<String, Map<String, String>> routing = new HashMap<>();
        routing.put("srm", srmMapping);

        BridgeProperties bridgeProps = new BridgeProperties();
        bridgeProps.setTopicRouting(routing);

        SalesforceProperties sfProps = createSalesforceProperties("srm", "acad");

        ConfigurableTopicRouter router = new ConfigurableTopicRouter(bridgeProps, sfProps);
        router.validateConfiguration();

        // Test - unmapped org
        Optional<String> result = router.getKafkaTopic("acad", "/event/Course_Event__e");

        // Verify
        assertFalse(result.isPresent(), "Unmapped org should return empty Optional");
    }

    // -----------------------------------------------------------------------
    // Test: Startup validation fails for undefined org
    // -----------------------------------------------------------------------

    @Test
    void startupValidationFailsForUndefinedOrg() {
        // Setup - routing references "acad" but it's not in salesforce.orgs
        Map<String, String> acadMapping = new HashMap<>();
        acadMapping.put("/event/Course_Event__e", "salesforce.acad.course");

        Map<String, Map<String, String>> routing = new HashMap<>();
        routing.put("acad", acadMapping);

        BridgeProperties bridgeProps = new BridgeProperties();
        bridgeProps.setTopicRouting(routing);

        // Only configure "srm", not "acad"
        SalesforceProperties sfProps = createSalesforceProperties("srm");

        ConfigurableTopicRouter router = new ConfigurableTopicRouter(bridgeProps, sfProps);

        // Test - validation should fail
        ConfigurationException exception = assertThrows(ConfigurationException.class,
                router::validateConfiguration);

        // Verify
        assertTrue(exception.getMessage().contains("undefined org"),
                "Exception should mention undefined org");
        assertTrue(exception.getMessage().contains("acad"),
                "Exception should mention the specific org name");
    }

    @Test
    void startupValidationFailsForEmptyRouting() {
        // Setup - empty routing
        BridgeProperties bridgeProps = new BridgeProperties();
        bridgeProps.setTopicRouting(new HashMap<>());

        SalesforceProperties sfProps = createSalesforceProperties("srm");

        ConfigurableTopicRouter router = new ConfigurableTopicRouter(bridgeProps, sfProps);

        // Test - validation should fail
        ConfigurationException exception = assertThrows(ConfigurationException.class,
                router::validateConfiguration);

        // Verify
        assertTrue(exception.getMessage().contains("empty"),
                "Exception should mention empty routing configuration");
    }

    @Test
    void startupValidationFailsForNoSalesforceOrgs() {
        // Setup - valid routing but no Salesforce orgs
        Map<String, String> srmMapping = new HashMap<>();
        srmMapping.put("/event/Enrollment_Event__e", "salesforce.srm.enrollment");

        Map<String, Map<String, String>> routing = new HashMap<>();
        routing.put("srm", srmMapping);

        BridgeProperties bridgeProps = new BridgeProperties();
        bridgeProps.setTopicRouting(routing);

        SalesforceProperties sfProps = new SalesforceProperties();
        sfProps.setOrgs(new HashMap<>());

        ConfigurableTopicRouter router = new ConfigurableTopicRouter(bridgeProps, sfProps);

        // Test - validation should fail
        ConfigurationException exception = assertThrows(ConfigurationException.class,
                router::validateConfiguration);

        // Verify
        assertTrue(exception.getMessage().contains("No Salesforce orgs"),
                "Exception should mention missing Salesforce orgs");
    }

    // -----------------------------------------------------------------------
    // Test: Configuration loaded from properties
    // -----------------------------------------------------------------------

    @Test
    void configurationLoadedFromProperties() throws Exception {
        // Setup - multi-org configuration
        Map<String, String> srmMapping = new HashMap<>();
        srmMapping.put("/event/Enrollment_Event__e", "salesforce.srm.enrollment");
        srmMapping.put("/event/Student_Event__e", "salesforce.srm.student");

        Map<String, String> acadMapping = new HashMap<>();
        acadMapping.put("/event/Course_Event__e", "salesforce.acad.course");

        Map<String, Map<String, String>> routing = new HashMap<>();
        routing.put("srm", srmMapping);
        routing.put("acad", acadMapping);

        BridgeProperties bridgeProps = new BridgeProperties();
        bridgeProps.setTopicRouting(routing);

        SalesforceProperties sfProps = createSalesforceProperties("srm", "acad");

        ConfigurableTopicRouter router = new ConfigurableTopicRouter(bridgeProps, sfProps);
        router.validateConfiguration();

        // Test - verify all mappings loaded correctly
        Optional<String> srm1 = router.getKafkaTopic("srm", "/event/Enrollment_Event__e");
        Optional<String> srm2 = router.getKafkaTopic("srm", "/event/Student_Event__e");
        Optional<String> acad1 = router.getKafkaTopic("acad", "/event/Course_Event__e");

        // Verify
        assertTrue(srm1.isPresent());
        assertEquals("salesforce.srm.enrollment", srm1.get());

        assertTrue(srm2.isPresent());
        assertEquals("salesforce.srm.student", srm2.get());

        assertTrue(acad1.isPresent());
        assertEquals("salesforce.acad.course", acad1.get());
    }

    @Test
    void validationSucceedsForValidConfiguration() throws Exception {
        // Setup
        Map<String, String> srmMapping = new HashMap<>();
        srmMapping.put("/event/Enrollment_Event__e", "salesforce.srm.enrollment");

        Map<String, Map<String, String>> routing = new HashMap<>();
        routing.put("srm", srmMapping);

        BridgeProperties bridgeProps = new BridgeProperties();
        bridgeProps.setTopicRouting(routing);

        SalesforceProperties sfProps = createSalesforceProperties("srm");

        ConfigurableTopicRouter router = new ConfigurableTopicRouter(bridgeProps, sfProps);

        // Test - validation should succeed
        assertDoesNotThrow(router::validateConfiguration,
                "Validation should succeed for valid configuration");
    }

    // -----------------------------------------------------------------------
    // Test: Null handling
    // -----------------------------------------------------------------------

    @Test
    void nullOrgReturnsEmptyOptional() throws Exception {
        // Setup
        Map<String, String> srmMapping = new HashMap<>();
        srmMapping.put("/event/Enrollment_Event__e", "salesforce.srm.enrollment");

        Map<String, Map<String, String>> routing = new HashMap<>();
        routing.put("srm", srmMapping);

        BridgeProperties bridgeProps = new BridgeProperties();
        bridgeProps.setTopicRouting(routing);

        SalesforceProperties sfProps = createSalesforceProperties("srm");

        ConfigurableTopicRouter router = new ConfigurableTopicRouter(bridgeProps, sfProps);
        router.validateConfiguration();

        // Test
        Optional<String> result = router.getKafkaTopic(null, "/event/Enrollment_Event__e");

        // Verify
        assertFalse(result.isPresent(), "Null org should return empty Optional");
    }

    @Test
    void nullTopicReturnsEmptyOptional() throws Exception {
        // Setup
        Map<String, String> srmMapping = new HashMap<>();
        srmMapping.put("/event/Enrollment_Event__e", "salesforce.srm.enrollment");

        Map<String, Map<String, String>> routing = new HashMap<>();
        routing.put("srm", srmMapping);

        BridgeProperties bridgeProps = new BridgeProperties();
        bridgeProps.setTopicRouting(routing);

        SalesforceProperties sfProps = createSalesforceProperties("srm");

        ConfigurableTopicRouter router = new ConfigurableTopicRouter(bridgeProps, sfProps);
        router.validateConfiguration();

        // Test
        Optional<String> result = router.getKafkaTopic("srm", null);

        // Verify
        assertFalse(result.isPresent(), "Null topic should return empty Optional");
    }

    // -----------------------------------------------------------------------
    // Test: Multiple orgs with independent routing
    // -----------------------------------------------------------------------

    @Test
    void multipleOrgsHaveIndependentRouting() throws Exception {
        // Setup
        Map<String, String> srmMapping = new HashMap<>();
        srmMapping.put("/event/Enrollment_Event__e", "salesforce.srm.enrollment");

        Map<String, String> acadMapping = new HashMap<>();
        acadMapping.put("/event/Enrollment_Event__e", "salesforce.acad.enrollment");

        Map<String, Map<String, String>> routing = new HashMap<>();
        routing.put("srm", srmMapping);
        routing.put("acad", acadMapping);

        BridgeProperties bridgeProps = new BridgeProperties();
        bridgeProps.setTopicRouting(routing);

        SalesforceProperties sfProps = createSalesforceProperties("srm", "acad");

        ConfigurableTopicRouter router = new ConfigurableTopicRouter(bridgeProps, sfProps);
        router.validateConfiguration();

        // Test - same Salesforce topic, different orgs
        Optional<String> srmResult = router.getKafkaTopic("srm", "/event/Enrollment_Event__e");
        Optional<String> acadResult = router.getKafkaTopic("acad", "/event/Enrollment_Event__e");

        // Verify - different Kafka topics
        assertTrue(srmResult.isPresent());
        assertTrue(acadResult.isPresent());
        assertEquals("salesforce.srm.enrollment", srmResult.get());
        assertEquals("salesforce.acad.enrollment", acadResult.get());
        assertNotEquals(srmResult.get(), acadResult.get(),
                "Different orgs should have independent routing");
    }

    // -----------------------------------------------------------------------
    // Helper methods
    // -----------------------------------------------------------------------

    private SalesforceProperties createSalesforceProperties(String... orgs) {
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
    }
}
