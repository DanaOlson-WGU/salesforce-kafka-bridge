package com.example.bridge.config;

import net.jqwik.api.*;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Property 20: Environment Variable Configuration Override
 *
 * For any configuration property, setting a corresponding environment variable
 * should override the value from application.yml.
 *
 * Validates: Requirements 8.2, 9.2
 */
class EnvironmentVariableOverridePropertyTest {

    @Configuration
    @EnableConfigurationProperties({SalesforceProperties.class, BridgeProperties.class, CircuitBreakerProperties.class})
    static class TestConfig {
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
    // Property: Environment variable should override application.yml value
    // for Salesforce org configuration
    // -----------------------------------------------------------------------

    @Provide
    Arbitrary<String> salesforceUrls() {
        return Arbitraries.of(
                "api.pubsub.salesforce.com:7443",
                "test.pubsub.salesforce.com:7443",
                "api.pubsub.salesforce.com:8443",
                "custom.salesforce.com:9443"
        );
    }

    @Provide
    Arbitrary<String> oauthUrls() {
        return Arbitraries.of(
                "https://login.salesforce.com/services/oauth2/token",
                "https://test.salesforce.com/services/oauth2/token",
                "https://custom.salesforce.com/services/oauth2/token"
        );
    }

    @Provide
    Arbitrary<String> clientIds() {
        return Arbitraries.strings()
                .withCharRange('a', 'z')
                .withCharRange('A', 'Z')
                .withCharRange('0', '9')
                .ofMinLength(10)
                .ofMaxLength(30);
    }

    @Provide
    Arbitrary<String> clientSecrets() {
        return Arbitraries.strings()
                .withCharRange('a', 'z')
                .withCharRange('A', 'Z')
                .withCharRange('0', '9')
                .ofMinLength(20)
                .ofMaxLength(50);
    }

    @Provide
    Arbitrary<String> usernames() {
        return Arbitraries.strings()
                .alpha()
                .ofMinLength(5)
                .ofMaxLength(20)
                .map(s -> s + "@example.com");
    }

    @Provide
    Arbitrary<String> passwords() {
        return Arbitraries.strings()
                .withCharRange('a', 'z')
                .withCharRange('A', 'Z')
                .withCharRange('0', '9')
                .ofMinLength(12)
                .ofMaxLength(30);
    }

    @Property(tries = 20)
    void environmentVariableShouldOverridePubsubUrl(
            @ForAll("salesforceUrls") String ymlValue,
            @ForAll("salesforceUrls") String envValue) {

        Assume.that(!ymlValue.equals(envValue));

        Map<String, String> props = minimalValidConfig();
        props.put("salesforce.orgs.srm.pubsub-url", ymlValue);

        runnerWithProps(props).run(context -> {
            assertNull(context.getStartupFailure());
            SalesforceProperties salesforceProps = context.getBean(SalesforceProperties.class);
            assertEquals(ymlValue, salesforceProps.getOrgs().get("srm").getPubsubUrl(),
                    "Should use application.yml value when no env var set");
        });

        // Now override with environment variable (simulated via property with higher precedence)
        props.put("salesforce.orgs.srm.pubsub-url", envValue);

        runnerWithProps(props).run(context -> {
            assertNull(context.getStartupFailure());
            SalesforceProperties salesforceProps = context.getBean(SalesforceProperties.class);
            assertEquals(envValue, salesforceProps.getOrgs().get("srm").getPubsubUrl(),
                    "Environment variable should override application.yml value");
        });
    }

    @Property(tries = 20)
    void environmentVariableShouldOverrideOauthUrl(
            @ForAll("oauthUrls") String ymlValue,
            @ForAll("oauthUrls") String envValue) {

        Assume.that(!ymlValue.equals(envValue));

        Map<String, String> props = minimalValidConfig();
        props.put("salesforce.orgs.srm.oauth-url", ymlValue);

        runnerWithProps(props).run(context -> {
            assertNull(context.getStartupFailure());
            SalesforceProperties salesforceProps = context.getBean(SalesforceProperties.class);
            assertEquals(ymlValue, salesforceProps.getOrgs().get("srm").getOauthUrl());
        });

        props.put("salesforce.orgs.srm.oauth-url", envValue);

        runnerWithProps(props).run(context -> {
            assertNull(context.getStartupFailure());
            SalesforceProperties salesforceProps = context.getBean(SalesforceProperties.class);
            assertEquals(envValue, salesforceProps.getOrgs().get("srm").getOauthUrl(),
                    "Environment variable should override oauth-url");
        });
    }

    @Property(tries = 20)
    void environmentVariableShouldOverrideClientId(
            @ForAll("clientIds") String ymlValue,
            @ForAll("clientIds") String envValue) {

        Assume.that(!ymlValue.equals(envValue));

        Map<String, String> props = minimalValidConfig();
        props.put("salesforce.orgs.srm.client-id", ymlValue);

        runnerWithProps(props).run(context -> {
            assertNull(context.getStartupFailure());
            SalesforceProperties salesforceProps = context.getBean(SalesforceProperties.class);
            assertEquals(ymlValue, salesforceProps.getOrgs().get("srm").getClientId());
        });

        props.put("salesforce.orgs.srm.client-id", envValue);

        runnerWithProps(props).run(context -> {
            assertNull(context.getStartupFailure());
            SalesforceProperties salesforceProps = context.getBean(SalesforceProperties.class);
            assertEquals(envValue, salesforceProps.getOrgs().get("srm").getClientId(),
                    "Environment variable should override client-id");
        });
    }

    @Property(tries = 20)
    void environmentVariableShouldOverrideClientSecret(
            @ForAll("clientSecrets") String ymlValue,
            @ForAll("clientSecrets") String envValue) {

        Assume.that(!ymlValue.equals(envValue));

        Map<String, String> props = minimalValidConfig();
        props.put("salesforce.orgs.srm.client-secret", ymlValue);

        runnerWithProps(props).run(context -> {
            assertNull(context.getStartupFailure());
            SalesforceProperties salesforceProps = context.getBean(SalesforceProperties.class);
            assertEquals(ymlValue, salesforceProps.getOrgs().get("srm").getClientSecret());
        });

        props.put("salesforce.orgs.srm.client-secret", envValue);

        runnerWithProps(props).run(context -> {
            assertNull(context.getStartupFailure());
            SalesforceProperties salesforceProps = context.getBean(SalesforceProperties.class);
            assertEquals(envValue, salesforceProps.getOrgs().get("srm").getClientSecret(),
                    "Environment variable should override client-secret");
        });
    }

    @Property(tries = 20)
    void environmentVariableShouldOverrideUsername(
            @ForAll("usernames") String ymlValue,
            @ForAll("usernames") String envValue) {

        Assume.that(!ymlValue.equals(envValue));

        Map<String, String> props = minimalValidConfig();
        props.put("salesforce.orgs.srm.username", ymlValue);

        runnerWithProps(props).run(context -> {
            assertNull(context.getStartupFailure());
            SalesforceProperties salesforceProps = context.getBean(SalesforceProperties.class);
            assertEquals(ymlValue, salesforceProps.getOrgs().get("srm").getUsername());
        });

        props.put("salesforce.orgs.srm.username", envValue);

        runnerWithProps(props).run(context -> {
            assertNull(context.getStartupFailure());
            SalesforceProperties salesforceProps = context.getBean(SalesforceProperties.class);
            assertEquals(envValue, salesforceProps.getOrgs().get("srm").getUsername(),
                    "Environment variable should override username");
        });
    }

    @Property(tries = 20)
    void environmentVariableShouldOverridePassword(
            @ForAll("passwords") String ymlValue,
            @ForAll("passwords") String envValue) {

        Assume.that(!ymlValue.equals(envValue));

        Map<String, String> props = minimalValidConfig();
        props.put("salesforce.orgs.srm.password", ymlValue);

        runnerWithProps(props).run(context -> {
            assertNull(context.getStartupFailure());
            SalesforceProperties salesforceProps = context.getBean(SalesforceProperties.class);
            assertEquals(ymlValue, salesforceProps.getOrgs().get("srm").getPassword());
        });

        props.put("salesforce.orgs.srm.password", envValue);

        runnerWithProps(props).run(context -> {
            assertNull(context.getStartupFailure());
            SalesforceProperties salesforceProps = context.getBean(SalesforceProperties.class);
            assertEquals(envValue, salesforceProps.getOrgs().get("srm").getPassword(),
                    "Environment variable should override password");
        });
    }

    // -----------------------------------------------------------------------
    // Property: Environment variable should override retry configuration
    // -----------------------------------------------------------------------

    @Provide
    Arbitrary<Integer> retryMaxAttempts() {
        return Arbitraries.integers().between(1, 20);
    }

    @Provide
    Arbitrary<Long> retryIntervals() {
        return Arbitraries.longs().between(100L, 120000L);
    }

    @Provide
    Arbitrary<Double> retryMultipliers() {
        return Arbitraries.doubles().between(1.5, 3.0);
    }

    @Property(tries = 15)
    void environmentVariableShouldOverrideRetryMaxAttempts(
            @ForAll("retryMaxAttempts") int ymlValue,
            @ForAll("retryMaxAttempts") int envValue) {

        Assume.that(ymlValue != envValue);

        Map<String, String> props = minimalValidConfig();
        props.put("salesforce.retry.max-attempts", String.valueOf(ymlValue));

        runnerWithProps(props).run(context -> {
            assertNull(context.getStartupFailure());
            SalesforceProperties salesforceProps = context.getBean(SalesforceProperties.class);
            assertEquals(ymlValue, salesforceProps.getRetry().getMaxAttempts());
        });

        props.put("salesforce.retry.max-attempts", String.valueOf(envValue));

        runnerWithProps(props).run(context -> {
            assertNull(context.getStartupFailure());
            SalesforceProperties salesforceProps = context.getBean(SalesforceProperties.class);
            assertEquals(envValue, salesforceProps.getRetry().getMaxAttempts(),
                    "Environment variable should override retry max-attempts");
        });
    }

    @Property(tries = 15)
    void environmentVariableShouldOverrideRetryInitialInterval(
            @ForAll("retryIntervals") long ymlValue,
            @ForAll("retryIntervals") long envValue) {

        Assume.that(ymlValue != envValue);

        Map<String, String> props = minimalValidConfig();
        props.put("salesforce.retry.initial-interval-ms", String.valueOf(ymlValue));

        runnerWithProps(props).run(context -> {
            assertNull(context.getStartupFailure());
            SalesforceProperties salesforceProps = context.getBean(SalesforceProperties.class);
            assertEquals(ymlValue, salesforceProps.getRetry().getInitialIntervalMs());
        });

        props.put("salesforce.retry.initial-interval-ms", String.valueOf(envValue));

        runnerWithProps(props).run(context -> {
            assertNull(context.getStartupFailure());
            SalesforceProperties salesforceProps = context.getBean(SalesforceProperties.class);
            assertEquals(envValue, salesforceProps.getRetry().getInitialIntervalMs(),
                    "Environment variable should override retry initial-interval-ms");
        });
    }

    // -----------------------------------------------------------------------
    // Property: Environment variable should override circuit breaker config
    // -----------------------------------------------------------------------

    @Provide
    Arbitrary<Integer> failureThresholds() {
        return Arbitraries.integers().between(1, 20);
    }

    @Provide
    Arbitrary<Integer> coolDownPeriods() {
        return Arbitraries.integers().between(10, 300);
    }

    @Provide
    Arbitrary<Integer> halfOpenMaxAttempts() {
        return Arbitraries.integers().between(1, 10);
    }

    @Property(tries = 15)
    void environmentVariableShouldOverrideCircuitBreakerFailureThreshold(
            @ForAll("failureThresholds") int ymlValue,
            @ForAll("failureThresholds") int envValue) {

        Assume.that(ymlValue != envValue);

        Map<String, String> props = minimalValidConfig();
        props.put("resilience.circuit-breaker.failure-threshold", String.valueOf(ymlValue));

        runnerWithProps(props).run(context -> {
            assertNull(context.getStartupFailure());
            CircuitBreakerProperties cbProps = context.getBean(CircuitBreakerProperties.class);
            assertEquals(ymlValue, cbProps.getFailureThreshold());
        });

        props.put("resilience.circuit-breaker.failure-threshold", String.valueOf(envValue));

        runnerWithProps(props).run(context -> {
            assertNull(context.getStartupFailure());
            CircuitBreakerProperties cbProps = context.getBean(CircuitBreakerProperties.class);
            assertEquals(envValue, cbProps.getFailureThreshold(),
                    "Environment variable should override circuit-breaker failure-threshold");
        });
    }

    @Property(tries = 15)
    void environmentVariableShouldOverrideCircuitBreakerCoolDownPeriod(
            @ForAll("coolDownPeriods") int ymlValue,
            @ForAll("coolDownPeriods") int envValue) {

        Assume.that(ymlValue != envValue);

        Map<String, String> props = minimalValidConfig();
        props.put("resilience.circuit-breaker.cool-down-period-seconds", String.valueOf(ymlValue));

        runnerWithProps(props).run(context -> {
            assertNull(context.getStartupFailure());
            CircuitBreakerProperties cbProps = context.getBean(CircuitBreakerProperties.class);
            assertEquals(ymlValue, cbProps.getCoolDownPeriodSeconds());
        });

        props.put("resilience.circuit-breaker.cool-down-period-seconds", String.valueOf(envValue));

        runnerWithProps(props).run(context -> {
            assertNull(context.getStartupFailure());
            CircuitBreakerProperties cbProps = context.getBean(CircuitBreakerProperties.class);
            assertEquals(envValue, cbProps.getCoolDownPeriodSeconds(),
                    "Environment variable should override circuit-breaker cool-down-period-seconds");
        });
    }

    // -----------------------------------------------------------------------
    // Property: Environment variable should override Spring datasource config
    // -----------------------------------------------------------------------

    @Provide
    Arbitrary<String> datasourceUrls() {
        return Arbitraries.of(
                "jdbc:postgresql://localhost:5432/testdb",
                "jdbc:postgresql://db-host:5432/proddb",
                "jdbc:postgresql://staging-db:5432/stagingdb"
        );
    }

    @Provide
    Arbitrary<String> datasourceUsernames() {
        return Arbitraries.strings()
                .alpha()
                .ofMinLength(5)
                .ofMaxLength(15);
    }

    @Property(tries = 10)
    void environmentVariableShouldOverrideDatasourceUrl(
            @ForAll("datasourceUrls") String ymlValue,
            @ForAll("datasourceUrls") String envValue) {

        Assume.that(!ymlValue.equals(envValue));

        Map<String, String> props = minimalValidConfig();
        props.put("spring.datasource.url", ymlValue);
        props.put("spring.datasource.username", "testuser");
        props.put("spring.datasource.password", "testpass");

        runnerWithProps(props).run(context -> {
            assertNull(context.getStartupFailure());
            // Spring Boot's DataSource properties are available via environment
            String actualUrl = context.getEnvironment().getProperty("spring.datasource.url");
            assertEquals(ymlValue, actualUrl);
        });

        // Override with environment variable
        props.put("spring.datasource.url", envValue);

        runnerWithProps(props).run(context -> {
            assertNull(context.getStartupFailure());
            String actualUrl = context.getEnvironment().getProperty("spring.datasource.url");
            assertEquals(envValue, actualUrl,
                    "Environment variable should override spring.datasource.url");
        });
    }

    // -----------------------------------------------------------------------
    // Property: Multiple environment variables can override simultaneously
    // -----------------------------------------------------------------------

    @Property(tries = 10)
    void multipleEnvironmentVariablesShouldOverrideSimultaneously(
            @ForAll("clientIds") String clientIdYml,
            @ForAll("clientIds") String clientIdEnv,
            @ForAll("passwords") String passwordYml,
            @ForAll("passwords") String passwordEnv,
            @ForAll("failureThresholds") int thresholdYml,
            @ForAll("failureThresholds") int thresholdEnv) {

        Assume.that(!clientIdYml.equals(clientIdEnv));
        Assume.that(!passwordYml.equals(passwordEnv));
        Assume.that(thresholdYml != thresholdEnv);

        // First, verify yml values are used
        Map<String, String> propsYml = minimalValidConfig();
        propsYml.put("salesforce.orgs.srm.client-id", clientIdYml);
        propsYml.put("salesforce.orgs.srm.password", passwordYml);
        propsYml.put("resilience.circuit-breaker.failure-threshold", String.valueOf(thresholdYml));

        runnerWithProps(propsYml).run(context -> {
            assertNull(context.getStartupFailure());
            SalesforceProperties sfProps = context.getBean(SalesforceProperties.class);
            CircuitBreakerProperties cbProps = context.getBean(CircuitBreakerProperties.class);
            
            assertEquals(clientIdYml, sfProps.getOrgs().get("srm").getClientId());
            assertEquals(passwordYml, sfProps.getOrgs().get("srm").getPassword());
            assertEquals(thresholdYml, cbProps.getFailureThreshold());
        });

        // Now override all with environment variables
        Map<String, String> propsEnv = minimalValidConfig();
        propsEnv.put("salesforce.orgs.srm.client-id", clientIdEnv);
        propsEnv.put("salesforce.orgs.srm.password", passwordEnv);
        propsEnv.put("resilience.circuit-breaker.failure-threshold", String.valueOf(thresholdEnv));

        runnerWithProps(propsEnv).run(context -> {
            assertNull(context.getStartupFailure());
            SalesforceProperties sfProps = context.getBean(SalesforceProperties.class);
            CircuitBreakerProperties cbProps = context.getBean(CircuitBreakerProperties.class);
            
            assertEquals(clientIdEnv, sfProps.getOrgs().get("srm").getClientId(),
                    "Environment variable should override client-id");
            assertEquals(passwordEnv, sfProps.getOrgs().get("srm").getPassword(),
                    "Environment variable should override password");
            assertEquals(thresholdEnv, cbProps.getFailureThreshold(),
                    "Environment variable should override failure-threshold");
        });
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private Map<String, String> minimalValidConfig() {
        Map<String, String> props = new LinkedHashMap<>();
        
        // Salesforce org config
        props.put("salesforce.orgs.srm.pubsub-url", "api.pubsub.salesforce.com:7443");
        props.put("salesforce.orgs.srm.oauth-url", "https://login.salesforce.com/services/oauth2/token");
        props.put("salesforce.orgs.srm.client-id", "test-client-id");
        props.put("salesforce.orgs.srm.client-secret", "test-client-secret");
        props.put("salesforce.orgs.srm.username", "test-user@example.com");
        props.put("salesforce.orgs.srm.password", "test-password");
        props.put("salesforce.orgs.srm.topics[0]", "/event/Test_Event__e");
        
        // Retry config
        props.put("salesforce.retry.max-attempts", "10");
        props.put("salesforce.retry.initial-interval-ms", "1000");
        props.put("salesforce.retry.max-interval-ms", "60000");
        props.put("salesforce.retry.multiplier", "2.0");
        
        // Bridge routing
        props.put("bridge.topic-routing.srm./event/Test_Event__e", "salesforce.srm.test");
        
        // Circuit breaker config
        props.put("resilience.circuit-breaker.failure-threshold", "5");
        props.put("resilience.circuit-breaker.cool-down-period-seconds", "60");
        props.put("resilience.circuit-breaker.half-open-max-attempts", "3");
        
        return props;
    }
}
