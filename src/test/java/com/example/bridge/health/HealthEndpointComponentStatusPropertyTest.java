package com.example.bridge.health;

import com.example.bridge.config.OrgConfig;
import com.example.bridge.config.SalesforceProperties;
import com.example.bridge.kafka.KafkaEventPublisher;
import com.example.bridge.model.ConnectionStatus;
import com.example.bridge.pubsub.PubSubConnector;
import com.example.bridge.replay.ReplayStore;
import com.example.bridge.resilience.CircuitBreaker;
import com.example.bridge.resilience.CircuitState;
import net.jqwik.api.*;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Property 18: Health Endpoint Component Status
 *
 * For any health check request, the health endpoint should report the status
 * of all org gRPC connections, the Kafka producer connection, and the PostgreSQL
 * database connection, and should return HTTP 503 when any component is unhealthy.
 *
 * Validates: Requirements 7.2, 7.3
 */
class HealthEndpointComponentStatusPropertyTest {

    // -----------------------------------------------------------------------
    // Property: PubSub health reports status for all enabled orgs
    // -----------------------------------------------------------------------

    @Property(tries = 50)
    void pubSubHealthShouldReportStatusForAllEnabledOrgs(
            @ForAll("orgConfigurations") Map<String, OrgConfig> orgs,
            @ForAll("healthyOrgSubset") List<String> healthyOrgs) {

        PubSubConnector connector = mock(PubSubConnector.class);
        SalesforceProperties props = createSalesforceProperties(orgs);

        // Configure mock to return healthy/unhealthy based on healthyOrgs list
        for (String org : orgs.keySet()) {
            if (orgs.get(org).isEnabled()) {
                boolean isHealthy = healthyOrgs.contains(org);
                when(connector.getStatus(org)).thenReturn(
                        isHealthy ? ConnectionStatus.up("connected") : ConnectionStatus.down("disconnected")
                );
            }
        }

        PubSubHealthIndicator indicator = new PubSubHealthIndicator(connector, props);
        Health health = indicator.health();

        // Verify all enabled orgs are reported
        Map<String, Object> details = health.getDetails();
        for (Map.Entry<String, OrgConfig> entry : orgs.entrySet()) {
            assertTrue(details.containsKey(entry.getKey()),
                    "Health should report status for org: " + entry.getKey());
        }
    }

    // -----------------------------------------------------------------------
    // Property: PubSub health returns DOWN when any org is unhealthy
    // -----------------------------------------------------------------------

    @Property(tries = 50)
    void pubSubHealthShouldReturnDownWhenAnyOrgUnhealthy(
            @ForAll("enabledOrgConfigurations") Map<String, OrgConfig> orgs,
            @ForAll("orgToFail") String failOrg) {

        PubSubConnector connector = mock(PubSubConnector.class);
        SalesforceProperties props = createSalesforceProperties(orgs);

        // Pick one org to be unhealthy
        String unhealthyOrg = null;
        for (String org : orgs.keySet()) {
            if (orgs.get(org).isEnabled()) {
                if (unhealthyOrg == null) {
                    unhealthyOrg = org;
                    when(connector.getStatus(org)).thenReturn(ConnectionStatus.down("connection lost"));
                } else {
                    when(connector.getStatus(org)).thenReturn(ConnectionStatus.up("connected"));
                }
            }
        }

        if (unhealthyOrg != null) {
            PubSubHealthIndicator indicator = new PubSubHealthIndicator(connector, props);
            Health health = indicator.health();
            assertEquals(Status.DOWN, health.getStatus(),
                    "Health should be DOWN when any org connection is unhealthy");
        }
    }

    // -----------------------------------------------------------------------
    // Property: PubSub health returns UP when all orgs are healthy
    // -----------------------------------------------------------------------

    @Property(tries = 50)
    void pubSubHealthShouldReturnUpWhenAllOrgsHealthy(
            @ForAll("enabledOrgConfigurations") Map<String, OrgConfig> orgs) {

        PubSubConnector connector = mock(PubSubConnector.class);
        SalesforceProperties props = createSalesforceProperties(orgs);

        for (String org : orgs.keySet()) {
            if (orgs.get(org).isEnabled()) {
                when(connector.getStatus(org)).thenReturn(ConnectionStatus.up("connected"));
            }
        }

        PubSubHealthIndicator indicator = new PubSubHealthIndicator(connector, props);
        Health health = indicator.health();
        assertEquals(Status.UP, health.getStatus(),
                "Health should be UP when all org connections are healthy");
    }

    // -----------------------------------------------------------------------
    // Property: Kafka health returns DOWN when producer is unhealthy
    // -----------------------------------------------------------------------

    @Property(tries = 20)
    void kafkaHealthShouldReturnDownWhenProducerUnhealthy(
            @ForAll("errorMessages") String errorDetail) {

        KafkaEventPublisher publisher = mock(KafkaEventPublisher.class);
        when(publisher.getKafkaStatus()).thenReturn(ConnectionStatus.down(errorDetail));

        KafkaHealthIndicator indicator = new KafkaHealthIndicator(publisher);
        Health health = indicator.health();

        assertEquals(Status.DOWN, health.getStatus(),
                "Kafka health should be DOWN when producer is unhealthy");
        assertNotNull(health.getDetails().get("status"),
                "Kafka health should include status detail");
    }

    // -----------------------------------------------------------------------
    // Property: Kafka health returns UP when producer is healthy
    // -----------------------------------------------------------------------

    @Property(tries = 20)
    void kafkaHealthShouldReturnUpWhenProducerHealthy() {
        KafkaEventPublisher publisher = mock(KafkaEventPublisher.class);
        when(publisher.getKafkaStatus()).thenReturn(ConnectionStatus.up("connected"));

        KafkaHealthIndicator indicator = new KafkaHealthIndicator(publisher);
        Health health = indicator.health();

        assertEquals(Status.UP, health.getStatus(),
                "Kafka health should be UP when producer is healthy");
    }

    // -----------------------------------------------------------------------
    // Property: Database health returns DOWN when database is unhealthy
    // -----------------------------------------------------------------------

    @Property(tries = 20)
    void databaseHealthShouldReturnDownWhenDatabaseUnhealthy(
            @ForAll("errorMessages") String errorDetail) {

        ReplayStore replayStore = mock(ReplayStore.class);
        when(replayStore.getDatabaseStatus()).thenReturn(ConnectionStatus.down(errorDetail));

        DatabaseHealthIndicator indicator = new DatabaseHealthIndicator(replayStore);
        Health health = indicator.health();

        assertEquals(Status.DOWN, health.getStatus(),
                "Database health should be DOWN when database is unhealthy");
    }

    // -----------------------------------------------------------------------
    // Property: CircuitBreaker health returns DOWN when any org circuit is OPEN
    // -----------------------------------------------------------------------

    @Property(tries = 50)
    void circuitBreakerHealthShouldReturnDownWhenAnyOrgOpen(
            @ForAll("enabledOrgConfigurations") Map<String, OrgConfig> orgs) {

        CircuitBreaker cb = mock(CircuitBreaker.class);
        SalesforceProperties props = createSalesforceProperties(orgs);

        // Set first enabled org to OPEN, rest to CLOSED
        boolean firstSet = false;
        for (String org : orgs.keySet()) {
            if (orgs.get(org).isEnabled()) {
                if (!firstSet) {
                    when(cb.getState(org)).thenReturn(CircuitState.OPEN);
                    firstSet = true;
                } else {
                    when(cb.getState(org)).thenReturn(CircuitState.CLOSED);
                }
            }
        }

        if (firstSet) {
            CircuitBreakerHealthIndicator indicator = new CircuitBreakerHealthIndicator(cb, props);
            Health health = indicator.health();
            assertEquals(Status.DOWN, health.getStatus(),
                    "CircuitBreaker health should be DOWN when any org circuit is OPEN");
        }
    }

    // -----------------------------------------------------------------------
    // Property: CircuitBreaker health returns UP when all circuits CLOSED
    // -----------------------------------------------------------------------

    @Property(tries = 50)
    void circuitBreakerHealthShouldReturnUpWhenAllClosed(
            @ForAll("enabledOrgConfigurations") Map<String, OrgConfig> orgs) {

        CircuitBreaker cb = mock(CircuitBreaker.class);
        SalesforceProperties props = createSalesforceProperties(orgs);

        for (String org : orgs.keySet()) {
            if (orgs.get(org).isEnabled()) {
                when(cb.getState(org)).thenReturn(CircuitState.CLOSED);
            }
        }

        CircuitBreakerHealthIndicator indicator = new CircuitBreakerHealthIndicator(cb, props);
        Health health = indicator.health();
        assertEquals(Status.UP, health.getStatus(),
                "CircuitBreaker health should be UP when all circuits are CLOSED");
    }

    // -----------------------------------------------------------------------
    // Providers & helpers
    // -----------------------------------------------------------------------

    @Provide
    Arbitrary<Map<String, OrgConfig>> orgConfigurations() {
        return Arbitraries.of("srm", "acad", "org-alpha", "org-beta")
                .list().ofMinSize(1).ofMaxSize(4).uniqueElements()
                .flatMap(orgNames -> {
                    Arbitrary<Boolean> enabledArb = Arbitraries.of(true, false);
                    return enabledArb.list().ofSize(orgNames.size()).map(enabledFlags -> {
                        Map<String, OrgConfig> orgs = new HashMap<>();
                        for (int i = 0; i < orgNames.size(); i++) {
                            OrgConfig config = createOrgConfig(enabledFlags.get(i));
                            orgs.put(orgNames.get(i), config);
                        }
                        return orgs;
                    });
                });
    }

    @Provide
    Arbitrary<Map<String, OrgConfig>> enabledOrgConfigurations() {
        return Arbitraries.of("srm", "acad", "org-alpha", "org-beta")
                .list().ofMinSize(1).ofMaxSize(4).uniqueElements()
                .map(orgNames -> {
                    Map<String, OrgConfig> orgs = new HashMap<>();
                    for (String name : orgNames) {
                        orgs.put(name, createOrgConfig(true));
                    }
                    return orgs;
                });
    }

    @Provide
    Arbitrary<List<String>> healthyOrgSubset() {
        return Arbitraries.of("srm", "acad", "org-alpha", "org-beta")
                .list().ofMinSize(0).ofMaxSize(4).uniqueElements();
    }

    @Provide
    Arbitrary<String> orgToFail() {
        return Arbitraries.of("srm", "acad", "org-alpha", "org-beta");
    }

    @Provide
    Arbitrary<String> errorMessages() {
        return Arbitraries.of("connection refused", "timeout", "unavailable", "auth failed");
    }

    private OrgConfig createOrgConfig(boolean enabled) {
        OrgConfig config = new OrgConfig();
        config.setPubsubUrl("api.pubsub.salesforce.com:7443");
        config.setOauthUrl("https://login.salesforce.com/services/oauth2/token");
        config.setClientId("test-client-id");
        config.setClientSecret("test-secret");
        config.setUsername("test-user");
        config.setPassword("test-pass");
        config.setTopics(List.of("/event/Test_Event__e"));
        config.setEnabled(enabled);
        return config;
    }

    private SalesforceProperties createSalesforceProperties(Map<String, OrgConfig> orgs) {
        SalesforceProperties props = new SalesforceProperties();
        props.setOrgs(orgs);
        return props;
    }
}
