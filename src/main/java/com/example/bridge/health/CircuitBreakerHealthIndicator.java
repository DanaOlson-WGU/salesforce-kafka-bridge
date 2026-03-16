package com.example.bridge.health;

import com.example.bridge.config.OrgConfig;
import com.example.bridge.config.SalesforceProperties;
import com.example.bridge.resilience.CircuitBreaker;
import com.example.bridge.resilience.CircuitState;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Reports the circuit breaker state for each configured Salesforce org.
 * Returns DOWN (503) when any org circuit breaker is OPEN.
 */
@Component
public class CircuitBreakerHealthIndicator implements HealthIndicator {

    private final CircuitBreaker circuitBreaker;
    private final SalesforceProperties salesforceProperties;

    public CircuitBreakerHealthIndicator(CircuitBreaker circuitBreaker,
                                          SalesforceProperties salesforceProperties) {
        this.circuitBreaker = circuitBreaker;
        this.salesforceProperties = salesforceProperties;
    }

    @Override
    public Health health() {
        Map<String, Object> details = new LinkedHashMap<>();
        boolean allClosed = true;

        for (Map.Entry<String, OrgConfig> entry : salesforceProperties.getOrgs().entrySet()) {
            String org = entry.getKey();
            if (!entry.getValue().isEnabled()) {
                details.put(org, "disabled");
                continue;
            }
            CircuitState state = circuitBreaker.getState(org);
            details.put(org, state.name());
            if (state != CircuitState.CLOSED) {
                allClosed = false;
            }
        }

        return allClosed
                ? Health.up().withDetails(details).build()
                : Health.down().withDetails(details).build();
    }
}
