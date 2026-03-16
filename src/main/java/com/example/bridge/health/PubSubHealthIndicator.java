package com.example.bridge.health;

import com.example.bridge.config.OrgConfig;
import com.example.bridge.config.SalesforceProperties;
import com.example.bridge.model.ConnectionStatus;
import com.example.bridge.pubsub.PubSubConnector;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Reports the gRPC connection status for each configured Salesforce org.
 * Returns DOWN (503) when any org connection is unhealthy.
 */
@Component
public class PubSubHealthIndicator implements HealthIndicator {

    private final PubSubConnector pubSubConnector;
    private final SalesforceProperties salesforceProperties;

    public PubSubHealthIndicator(PubSubConnector pubSubConnector,
                                  SalesforceProperties salesforceProperties) {
        this.pubSubConnector = pubSubConnector;
        this.salesforceProperties = salesforceProperties;
    }

    @Override
    public Health health() {
        Map<String, Object> details = new LinkedHashMap<>();
        boolean allHealthy = true;

        for (Map.Entry<String, OrgConfig> entry : salesforceProperties.getOrgs().entrySet()) {
            String org = entry.getKey();
            if (!entry.getValue().isEnabled()) {
                details.put(org, "disabled");
                continue;
            }
            ConnectionStatus status = pubSubConnector.getStatus(org);
            details.put(org, status.getDetail());
            if (!status.isHealthy()) {
                allHealthy = false;
            }
        }

        return allHealthy
                ? Health.up().withDetails(details).build()
                : Health.down().withDetails(details).build();
    }
}
