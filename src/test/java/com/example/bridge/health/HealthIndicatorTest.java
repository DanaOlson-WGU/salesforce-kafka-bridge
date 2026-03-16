package com.example.bridge.health;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.example.bridge.config.ApplicationStartupLogger;
import com.example.bridge.config.OrgConfig;
import com.example.bridge.config.SalesforceProperties;
import com.example.bridge.kafka.KafkaEventPublisher;
import com.example.bridge.model.ConnectionStatus;
import com.example.bridge.pubsub.PubSubConnector;
import com.example.bridge.replay.ReplayStore;
import com.example.bridge.resilience.CircuitBreaker;
import com.example.bridge.resilience.CircuitState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.info.BuildProperties;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for health indicators.
 * Validates: Requirements 7.1, 7.2, 7.3, 7.5, 7.6
 */
class HealthIndicatorTest {

    private PubSubConnector pubSubConnector;
    private KafkaEventPublisher kafkaEventPublisher;
    private ReplayStore replayStore;
    private CircuitBreaker circuitBreaker;
    private SalesforceProperties salesforceProperties;

    @BeforeEach
    void setup() {
        pubSubConnector = mock(PubSubConnector.class);
        kafkaEventPublisher = mock(KafkaEventPublisher.class);
        replayStore = mock(ReplayStore.class);
        circuitBreaker = mock(CircuitBreaker.class);
        salesforceProperties = createSalesforceProperties();
    }

    // -----------------------------------------------------------------------
    // PubSub Health Indicator Tests
    // -----------------------------------------------------------------------

    @Test
    void pubSubHealthReportsAllComponentStatuses() {
        when(pubSubConnector.getStatus("srm")).thenReturn(ConnectionStatus.up("connected"));
        when(pubSubConnector.getStatus("acad")).thenReturn(ConnectionStatus.up("connected"));

        PubSubHealthIndicator indicator = new PubSubHealthIndicator(pubSubConnector, salesforceProperties);
        Health health = indicator.health();

        assertEquals(Status.UP, health.getStatus());
        assertTrue(health.getDetails().containsKey("srm"));
        assertTrue(health.getDetails().containsKey("acad"));
    }

    @Test
    void pubSubHealthReturns503WhenAnyComponentUnhealthy() {
        when(pubSubConnector.getStatus("srm")).thenReturn(ConnectionStatus.down("connection lost"));
        when(pubSubConnector.getStatus("acad")).thenReturn(ConnectionStatus.up("connected"));

        PubSubHealthIndicator indicator = new PubSubHealthIndicator(pubSubConnector, salesforceProperties);
        Health health = indicator.health();

        assertEquals(Status.DOWN, health.getStatus());
    }

    @Test
    void pubSubHealthSkipsDisabledOrgs() {
        SalesforceProperties props = createSalesforcePropertiesWithDisabledOrg();
        when(pubSubConnector.getStatus("srm")).thenReturn(ConnectionStatus.up("connected"));

        PubSubHealthIndicator indicator = new PubSubHealthIndicator(pubSubConnector, props);
        Health health = indicator.health();

        assertEquals(Status.UP, health.getStatus());
        assertEquals("disabled", health.getDetails().get("acad"));
        verify(pubSubConnector, never()).getStatus("acad");
    }

    // -----------------------------------------------------------------------
    // Kafka Health Indicator Tests
    // -----------------------------------------------------------------------

    @Test
    void kafkaHealthReportsUpWhenProducerHealthy() {
        when(kafkaEventPublisher.getKafkaStatus()).thenReturn(ConnectionStatus.up("connected"));

        KafkaHealthIndicator indicator = new KafkaHealthIndicator(kafkaEventPublisher);
        Health health = indicator.health();

        assertEquals(Status.UP, health.getStatus());
        assertNotNull(health.getDetails().get("status"));
    }

    @Test
    void kafkaHealthReturns503WhenProducerUnhealthy() {
        when(kafkaEventPublisher.getKafkaStatus()).thenReturn(ConnectionStatus.down("broker unavailable"));

        KafkaHealthIndicator indicator = new KafkaHealthIndicator(kafkaEventPublisher);
        Health health = indicator.health();

        assertEquals(Status.DOWN, health.getStatus());
    }

    // -----------------------------------------------------------------------
    // CircuitBreaker Health Indicator Tests
    // -----------------------------------------------------------------------

    @Test
    void circuitBreakerHealthReportsAllOrgStates() {
        when(circuitBreaker.getState("srm")).thenReturn(CircuitState.CLOSED);
        when(circuitBreaker.getState("acad")).thenReturn(CircuitState.CLOSED);

        CircuitBreakerHealthIndicator indicator = new CircuitBreakerHealthIndicator(circuitBreaker, salesforceProperties);
        Health health = indicator.health();

        assertEquals(Status.UP, health.getStatus());
        assertEquals("CLOSED", health.getDetails().get("srm"));
        assertEquals("CLOSED", health.getDetails().get("acad"));
    }

    @Test
    void circuitBreakerHealthReturns503WhenAnyOrgOpen() {
        when(circuitBreaker.getState("srm")).thenReturn(CircuitState.OPEN);
        when(circuitBreaker.getState("acad")).thenReturn(CircuitState.CLOSED);

        CircuitBreakerHealthIndicator indicator = new CircuitBreakerHealthIndicator(circuitBreaker, salesforceProperties);
        Health health = indicator.health();

        assertEquals(Status.DOWN, health.getStatus());
    }

    // -----------------------------------------------------------------------
    // Database Health Indicator Tests
    // -----------------------------------------------------------------------

    @Test
    void databaseHealthReportsUpWhenDatabaseHealthy() {
        when(replayStore.getDatabaseStatus()).thenReturn(ConnectionStatus.up("connected"));

        DatabaseHealthIndicator indicator = new DatabaseHealthIndicator(replayStore);
        Health health = indicator.health();

        assertEquals(Status.UP, health.getStatus());
    }

    @Test
    void databaseHealthReturns503WhenDatabaseUnhealthy() {
        when(replayStore.getDatabaseStatus()).thenReturn(ConnectionStatus.down("connection refused"));

        DatabaseHealthIndicator indicator = new DatabaseHealthIndicator(replayStore);
        Health health = indicator.health();

        assertEquals(Status.DOWN, health.getStatus());
    }

    // -----------------------------------------------------------------------
    // Startup Logging Tests
    // -----------------------------------------------------------------------

    @Test
    void startupMessageIncludesVersionOrgsAndTopics() {
        Logger logger = (Logger) LoggerFactory.getLogger(ApplicationStartupLogger.class);
        ListAppender<ILoggingEvent> logAppender = new ListAppender<>();
        logAppender.start();
        logger.addAppender(logAppender);

        try {
            Properties props = new Properties();
            props.setProperty("version", "1.0.0-TEST");
            BuildProperties buildProperties = new BuildProperties(props);

            ApplicationStartupLogger startupLogger = new ApplicationStartupLogger(
                    salesforceProperties, buildProperties);
            // Create a real ApplicationReadyEvent - we don't need to mock it
            // Just call the method directly since we only care about the logging
            startupLogger.onApplicationEvent(null);

            List<ILoggingEvent> logs = logAppender.list;
            assertFalse(logs.isEmpty(), "Startup log should be emitted");

            String message = logs.get(0).getFormattedMessage();
            assertTrue(message.contains("1.0.0-TEST"), "Startup log should include version");
            assertTrue(message.contains("srm"), "Startup log should include org srm");
            assertTrue(message.contains("acad"), "Startup log should include org acad");
            assertTrue(message.contains("/event/Enrollment_Event__e"), "Startup log should include topics");
        } finally {
            logger.detachAppender(logAppender);
            logAppender.stop();
        }
    }

    @Test
    void startupMessageHandlesMissingBuildProperties() {
        Logger logger = (Logger) LoggerFactory.getLogger(ApplicationStartupLogger.class);
        ListAppender<ILoggingEvent> logAppender = new ListAppender<>();
        logAppender.start();
        logger.addAppender(logAppender);

        try {
            ApplicationStartupLogger startupLogger = new ApplicationStartupLogger(
                    salesforceProperties, null);
            // Create a real ApplicationReadyEvent - we don't need to mock it
            // Just call the method directly since we only care about the logging
            startupLogger.onApplicationEvent(null);

            List<ILoggingEvent> logs = logAppender.list;
            assertFalse(logs.isEmpty(), "Startup log should be emitted even without BuildProperties");

            String message = logs.get(0).getFormattedMessage();
            assertTrue(message.contains("unknown"), "Startup log should show 'unknown' version when BuildProperties is null");
        } finally {
            logger.detachAppender(logAppender);
            logAppender.stop();
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private SalesforceProperties createSalesforceProperties() {
        SalesforceProperties props = new SalesforceProperties();
        Map<String, OrgConfig> orgs = new HashMap<>();
        orgs.put("srm", createOrgConfig(true, List.of("/event/Enrollment_Event__e", "/event/Student_Event__e")));
        orgs.put("acad", createOrgConfig(true, List.of("/event/Course_Event__e")));
        props.setOrgs(orgs);
        return props;
    }

    private SalesforceProperties createSalesforcePropertiesWithDisabledOrg() {
        SalesforceProperties props = new SalesforceProperties();
        Map<String, OrgConfig> orgs = new HashMap<>();
        orgs.put("srm", createOrgConfig(true, List.of("/event/Enrollment_Event__e")));
        orgs.put("acad", createOrgConfig(false, List.of("/event/Course_Event__e")));
        props.setOrgs(orgs);
        return props;
    }

    private OrgConfig createOrgConfig(boolean enabled, List<String> topics) {
        OrgConfig config = new OrgConfig();
        config.setPubsubUrl("api.pubsub.salesforce.com:7443");
        config.setOauthUrl("https://login.salesforce.com/services/oauth2/token");
        config.setClientId("test-client-id");
        config.setClientSecret("test-secret");
        config.setUsername("test-user");
        config.setPassword("test-pass");
        config.setTopics(topics);
        config.setEnabled(enabled);
        return config;
    }
}
