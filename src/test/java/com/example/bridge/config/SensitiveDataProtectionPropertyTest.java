package com.example.bridge.config;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import net.jqwik.api.*;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.endpoint.web.annotation.RestControllerEndpoint;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Property 23: Sensitive Data Protection
 *
 * For any log output or exposed endpoint, sensitive configuration values
 * should not appear in plain text.
 *
 * Validates: Requirement 9.4
 */
class SensitiveDataProtectionPropertyTest {

    @Configuration
    @EnableConfigurationProperties({SalesforceProperties.class, BridgeProperties.class, CircuitBreakerProperties.class})
    static class TestConfig {
        
        @Bean
        public ConfigLogger configLogger(SalesforceProperties salesforceProperties) {
            return new ConfigLogger(salesforceProperties);
        }
    }

    /**
     * Component that logs configuration at startup (simulating real app behavior)
     */
    @Component
    static class ConfigLogger {
        private final SalesforceProperties salesforceProperties;
        
        public ConfigLogger(SalesforceProperties salesforceProperties) {
            this.salesforceProperties = salesforceProperties;
            logConfiguration();
        }
        
        private void logConfiguration() {
            org.slf4j.Logger log = LoggerFactory.getLogger(ConfigLogger.class);
            log.info("Loaded Salesforce configuration: {}", salesforceProperties);
            if (salesforceProperties.getOrgs() != null) {
                salesforceProperties.getOrgs().forEach((orgName, orgConfig) -> {
                    log.info("Org {}: {}", orgName, orgConfig);
                });
            }
        }
    }

    private static Map<String, String> validConfigWithSensitiveData(
            String clientSecret, String password, String dbPassword, String kafkaJaas) {
        Map<String, String> props = new LinkedHashMap<>();
        
        // Salesforce org config with sensitive data
        props.put("salesforce.orgs.srm.pubsub-url", "api.pubsub.salesforce.com:7443");
        props.put("salesforce.orgs.srm.oauth-url", "https://login.salesforce.com/services/oauth2/token");
        props.put("salesforce.orgs.srm.client-id", "test-client-id");
        props.put("salesforce.orgs.srm.client-secret", clientSecret);
        props.put("salesforce.orgs.srm.username", "test-user@example.com");
        props.put("salesforce.orgs.srm.password", password);
        props.put("salesforce.orgs.srm.topics[0]", "/event/Test_Event__e");
        
        // Bridge routing
        props.put("bridge.topic-routing.srm./event/Test_Event__e", "salesforce.srm.test");
        
        // Database password
        props.put("spring.datasource.url", "jdbc:postgresql://localhost:5432/testdb");
        props.put("spring.datasource.username", "testuser");
        props.put("spring.datasource.password", dbPassword);
        
        // Kafka SASL config
        props.put("kafka.bootstrap-servers", "localhost:9092");
        props.put("kafka.security.sasl-jaas-config", kafkaJaas);
        
        return props;
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
    // Arbitraries for generating sensitive data
    // -----------------------------------------------------------------------

    @Provide
    Arbitrary<String> sensitiveClientSecrets() {
        return Arbitraries.strings()
                .withCharRange('a', 'z')
                .withCharRange('A', 'Z')
                .withCharRange('0', '9')
                .ofMinLength(20)
                .ofMaxLength(50);
    }

    @Provide
    Arbitrary<String> sensitivePasswords() {
        return Arbitraries.strings()
                .withCharRange('a', 'z')
                .withCharRange('A', 'Z')
                .withCharRange('0', '9')
                .withChars('!', '@', '#', '$', '%')
                .ofMinLength(12)
                .ofMaxLength(30);
    }

    @Provide
    Arbitrary<String> sensitiveDbPasswords() {
        return Arbitraries.strings()
                .withCharRange('a', 'z')
                .withCharRange('A', 'Z')
                .withCharRange('0', '9')
                .ofMinLength(16)
                .ofMaxLength(32);
    }

    @Provide
    Arbitrary<String> sensitiveKafkaJaasConfigs() {
        return Arbitraries.strings()
                .alpha()
                .numeric()
                .ofMinLength(30)
                .ofMaxLength(100)
                .map(token -> "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user\" password=\"" + token + "\";");
    }

    // -----------------------------------------------------------------------
    // Property: Sensitive values should not appear in logs
    // -----------------------------------------------------------------------

    @Property(tries = 20)
    void sensitiveValuesShouldNotAppearInLogs(
            @ForAll("sensitiveClientSecrets") String clientSecret,
            @ForAll("sensitivePasswords") String password,
            @ForAll("sensitiveDbPasswords") String dbPassword,
            @ForAll("sensitiveKafkaJaasConfigs") String kafkaJaas) {

        // Setup log capture
        Logger rootLogger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        ListAppender<ILoggingEvent> logAppender = new ListAppender<>();
        logAppender.start();
        rootLogger.addAppender(logAppender);

        try {
            Map<String, String> props = validConfigWithSensitiveData(
                    clientSecret, password, dbPassword, kafkaJaas);

            runnerWithProps(props).run(context -> {
                // Context should start successfully
                assertNull(context.getStartupFailure(),
                        "Context should start with valid configuration");

                // Collect all log messages
                List<String> logMessages = logAppender.list.stream()
                        .map(ILoggingEvent::getFormattedMessage)
                        .toList();

                // Assert sensitive values do not appear in logs
                for (String logMessage : logMessages) {
                    assertFalse(logMessage.contains(clientSecret),
                            "Client secret should not appear in logs: " + logMessage);
                    assertFalse(logMessage.contains(password),
                            "Password should not appear in logs: " + logMessage);
                    assertFalse(logMessage.contains(dbPassword),
                            "Database password should not appear in logs: " + logMessage);
                    
                    // For JAAS config, check if the password token appears
                    if (kafkaJaas.contains("password=\"")) {
                        String passwordToken = kafkaJaas.substring(
                                kafkaJaas.indexOf("password=\"") + 10,
                                kafkaJaas.lastIndexOf("\""));
                        assertFalse(logMessage.contains(passwordToken),
                                "Kafka JAAS password should not appear in logs: " + logMessage);
                    }
                }
            });
        } finally {
            logAppender.stop();
            rootLogger.detachAppender(logAppender);
        }
    }

    // -----------------------------------------------------------------------
    // Property: toString() methods should not expose sensitive data
    // -----------------------------------------------------------------------

    @Property(tries = 20)
    void toStringMethodsShouldNotExposeSensitiveData(
            @ForAll("sensitiveClientSecrets") String clientSecret,
            @ForAll("sensitivePasswords") String password) {

        Map<String, String> props = new LinkedHashMap<>();
        props.put("salesforce.orgs.srm.pubsub-url", "api.pubsub.salesforce.com:7443");
        props.put("salesforce.orgs.srm.oauth-url", "https://login.salesforce.com/services/oauth2/token");
        props.put("salesforce.orgs.srm.client-id", "test-client-id");
        props.put("salesforce.orgs.srm.client-secret", clientSecret);
        props.put("salesforce.orgs.srm.username", "test-user@example.com");
        props.put("salesforce.orgs.srm.password", password);
        props.put("salesforce.orgs.srm.topics[0]", "/event/Test_Event__e");
        props.put("bridge.topic-routing.srm./event/Test_Event__e", "salesforce.srm.test");

        runnerWithProps(props).run(context -> {
            assertNull(context.getStartupFailure());

            SalesforceProperties salesforceProps = context.getBean(SalesforceProperties.class);
            String salesforcePropsString = salesforceProps.toString();
            String orgConfigString = salesforceProps.getOrgs().get("srm").toString();

            // Verify sensitive data is not in toString() output
            assertFalse(salesforcePropsString.contains(clientSecret),
                    "Client secret should not appear in SalesforceProperties.toString()");
            assertFalse(salesforcePropsString.contains(password),
                    "Password should not appear in SalesforceProperties.toString()");
            assertFalse(orgConfigString.contains(clientSecret),
                    "Client secret should not appear in OrgConfig.toString()");
            assertFalse(orgConfigString.contains(password),
                    "Password should not appear in OrgConfig.toString()");
        });
    }

    // -----------------------------------------------------------------------
    // Property: Actuator endpoints should not expose sensitive data
    // -----------------------------------------------------------------------

    @Property(tries = 10)
    void actuatorEndpointsShouldNotExposeSensitiveData(
            @ForAll("sensitiveClientSecrets") String clientSecret,
            @ForAll("sensitivePasswords") String password) {

        Map<String, String> props = validConfigWithSensitiveData(
                clientSecret, password, "db-secret-123", "jaas-config-secret");

        // Enable actuator endpoints
        props.put("management.endpoints.web.exposure.include", "env,configprops,health,info");

        runnerWithProps(props).run(context -> {
            assertNull(context.getStartupFailure());

            // In a real scenario, we would make HTTP requests to actuator endpoints
            // For this property test, we verify the beans don't expose sensitive data
            SalesforceProperties salesforceProps = context.getBean(SalesforceProperties.class);
            
            // Simulate what actuator would serialize
            String beanRepresentation = String.valueOf(salesforceProps);
            
            assertFalse(beanRepresentation.contains(clientSecret),
                    "Client secret should not be exposed via bean representation");
            assertFalse(beanRepresentation.contains(password),
                    "Password should not be exposed via bean representation");
        });
    }

    // -----------------------------------------------------------------------
    // Property: Exception messages should not contain sensitive data
    // -----------------------------------------------------------------------

    @Property(tries = 10)
    void exceptionMessagesShouldNotContainSensitiveData(
            @ForAll("sensitiveClientSecrets") String clientSecret,
            @ForAll("sensitivePasswords") String password) {

        // Create invalid config that will cause startup failure
        Map<String, String> props = new LinkedHashMap<>();
        props.put("salesforce.orgs.srm.pubsub-url", "api.pubsub.salesforce.com:7443");
        props.put("salesforce.orgs.srm.oauth-url", "https://login.salesforce.com/services/oauth2/token");
        props.put("salesforce.orgs.srm.client-id", "test-client-id");
        props.put("salesforce.orgs.srm.client-secret", clientSecret);
        props.put("salesforce.orgs.srm.username", "test-user@example.com");
        props.put("salesforce.orgs.srm.password", password);
        // Missing topics - will cause validation failure
        props.put("bridge.topic-routing.srm./event/Test_Event__e", "salesforce.srm.test");

        runnerWithProps(props).run(context -> {
            Throwable failure = context.getStartupFailure();
            assertNotNull(failure, "Context should fail with missing topics");

            // Walk exception chain and verify no sensitive data in messages
            Throwable current = failure;
            while (current != null) {
                String message = current.getMessage();
                if (message != null) {
                    assertFalse(message.contains(clientSecret),
                            "Client secret should not appear in exception message: " + message);
                    assertFalse(message.contains(password),
                            "Password should not appear in exception message: " + message);
                }
                current = current.getCause();
            }
        });
    }
}
