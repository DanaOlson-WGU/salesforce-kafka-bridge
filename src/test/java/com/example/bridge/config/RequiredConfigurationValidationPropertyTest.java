package com.example.bridge.config;

import net.jqwik.api.*;
import net.jqwik.api.constraints.IntRange;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.context.properties.bind.validation.BindValidationException;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Property 22: Required Configuration Validation
 *
 * For any required configuration property that is missing at startup,
 * the bridge should fail to start with a descriptive error message.
 *
 * Validates: Requirement 9.3
 */
class RequiredConfigurationValidationPropertyTest {

    /**
     * Minimal config class that enables the @ConfigurationProperties beans
     * without booting the full application (no DB, Kafka, gRPC needed).
     */
    @Configuration
    @EnableConfigurationProperties({SalesforceProperties.class, BridgeProperties.class, CircuitBreakerProperties.class})
    static class TestConfig {
    }

    // All properties required for a valid SalesforceProperties + one org
    private static final Map<String, String> VALID_SALESFORCE_ORG_PROPS = Map.of(
            "salesforce.orgs.srm.pubsub-url", "api.pubsub.salesforce.com:7443",
            "salesforce.orgs.srm.oauth-url", "https://login.salesforce.com/services/oauth2/token",
            "salesforce.orgs.srm.client-id", "test-client-id",
            "salesforce.orgs.srm.client-secret", "test-client-secret",
            "salesforce.orgs.srm.username", "test-user",
            "salesforce.orgs.srm.password", "test-pass",
            "salesforce.orgs.srm.topics[0]", "/event/Test_Event__e"
    );

    private static final Map<String, String> VALID_BRIDGE_PROPS = Map.of(
            "bridge.topic-routing.srm./event/Test_Event__e", "salesforce.srm.test"
    );

    /** All properties combined for a fully valid configuration. */
    private static Map<String, String> allValidProps() {
        Map<String, String> all = new LinkedHashMap<>();
        all.putAll(VALID_SALESFORCE_ORG_PROPS);
        all.putAll(VALID_BRIDGE_PROPS);
        return all;
    }

    private ApplicationContextRunner baseRunner() {
        return new ApplicationContextRunner()
                .withUserConfiguration(TestConfig.class);
    }

    private ApplicationContextRunner runnerWithProps(Map<String, String> props) {
        ApplicationContextRunner runner = baseRunner();
        for (Map.Entry<String, String> e : props.entrySet()) {
            runner = runner.withPropertyValues(e.getKey() + "=" + e.getValue());
        }
        return runner;
    }

    // -----------------------------------------------------------------------
    // Property: valid configuration should start successfully
    // -----------------------------------------------------------------------

    @Property(tries = 1)
    void validConfigurationShouldStartSuccessfully() {
        runnerWithProps(allValidProps()).run(context ->
                assertFalse(context.getStartupFailure() != null,
                        "Context should start with all valid properties")
        );
    }

    // -----------------------------------------------------------------------
    // Property: removing any non-empty subset of required Salesforce org
    // fields should cause startup failure with a descriptive message
    // -----------------------------------------------------------------------

    @Provide
    Arbitrary<Set<String>> salesforceOrgFieldSubsets() {
        List<String> requiredFields = List.of(
                "salesforce.orgs.srm.pubsub-url",
                "salesforce.orgs.srm.oauth-url",
                "salesforce.orgs.srm.client-id",
                "salesforce.orgs.srm.client-secret",
                "salesforce.orgs.srm.username",
                "salesforce.orgs.srm.password",
                "salesforce.orgs.srm.topics[0]"
        );
        // Generate non-empty subsets of required fields to remove
        return Arbitraries.of(requiredFields)
                .set().ofMinSize(1).ofMaxSize(requiredFields.size());
    }

    @Property(tries = 50)
    void missingAnySalesforceOrgFieldShouldFailStartup(
            @ForAll("salesforceOrgFieldSubsets") Set<String> fieldsToRemove) {

        Map<String, String> props = new LinkedHashMap<>(allValidProps());
        fieldsToRemove.forEach(props::remove);

        runnerWithProps(props).run(context -> {
            Throwable failure = context.getStartupFailure();
            assertNotNull(failure,
                    "Context should fail when missing: " + fieldsToRemove);
            assertHasDescriptiveMessage(failure);
        });
    }

    // -----------------------------------------------------------------------
    // Property: removing all salesforce orgs should fail startup
    // -----------------------------------------------------------------------

    @Property(tries = 1)
    void missingSalesforceOrgsShouldFailStartup() {
        // Only provide bridge props, no salesforce orgs at all
        runnerWithProps(VALID_BRIDGE_PROPS).run(context -> {
            Throwable failure = context.getStartupFailure();
            assertNotNull(failure,
                    "Context should fail when salesforce.orgs is missing entirely");
            assertHasDescriptiveMessage(failure);
        });
    }

    // -----------------------------------------------------------------------
    // Property: removing bridge topic-routing should fail startup
    // -----------------------------------------------------------------------

    @Property(tries = 1)
    void missingBridgeTopicRoutingShouldFailStartup() {
        // Only provide salesforce props, no bridge topic-routing
        runnerWithProps(VALID_SALESFORCE_ORG_PROPS).run(context -> {
            Throwable failure = context.getStartupFailure();
            assertNotNull(failure,
                    "Context should fail when bridge.topic-routing is missing");
            assertHasDescriptiveMessage(failure);
        });
    }

    // -----------------------------------------------------------------------
    // Property: blank values for required string fields should fail startup
    // -----------------------------------------------------------------------

    @Provide
    Arbitrary<String> salesforceStringFields() {
        return Arbitraries.of(
                "salesforce.orgs.srm.pubsub-url",
                "salesforce.orgs.srm.oauth-url",
                "salesforce.orgs.srm.client-id",
                "salesforce.orgs.srm.client-secret",
                "salesforce.orgs.srm.username",
                "salesforce.orgs.srm.password"
        );
    }

    @Provide
    Arbitrary<String> blankValues() {
        return Arbitraries.of("", "   ", "\t", "\n");
    }

    @Property(tries = 30)
    void blankRequiredFieldShouldFailStartup(
            @ForAll("salesforceStringFields") String field,
            @ForAll("blankValues") String blankValue) {

        Map<String, String> props = new LinkedHashMap<>(allValidProps());
        props.put(field, blankValue);

        runnerWithProps(props).run(context -> {
            Throwable failure = context.getStartupFailure();
            assertNotNull(failure,
                    "Context should fail when '" + field + "' is blank ('" + blankValue + "')");
            assertHasDescriptiveMessage(failure);
        });
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /**
     * Walks the exception chain and asserts that at least one cause contains
     * a BindValidationException (Spring's config validation error), which
     * carries descriptive messages about which property failed validation.
     */
    private void assertHasDescriptiveMessage(Throwable failure) {
        Throwable current = failure;
        while (current != null) {
            if (current instanceof BindValidationException) {
                String message = current.getMessage();
                assertNotNull(message, "Validation exception should have a message");
                assertFalse(message.isBlank(), "Validation exception message should not be blank");
                return;
            }
            current = current.getCause();
        }
        // If no BindValidationException, the failure itself should still have a message
        String rootMessage = failure.getMessage();
        assertNotNull(rootMessage, "Startup failure should have a descriptive message");
        assertFalse(rootMessage.isBlank(), "Startup failure message should not be blank");
    }
}
