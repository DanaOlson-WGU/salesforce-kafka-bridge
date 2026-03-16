package com.example.bridge.health;

import com.example.bridge.model.ConnectionStatus;
import com.example.bridge.replay.ReplayStore;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * Reports the PostgreSQL database connection status.
 * Returns DOWN (503) when the database is unavailable.
 */
@Component
public class DatabaseHealthIndicator implements HealthIndicator {

    private final ReplayStore replayStore;

    public DatabaseHealthIndicator(ReplayStore replayStore) {
        this.replayStore = replayStore;
    }

    @Override
    public Health health() {
        ConnectionStatus status = replayStore.getDatabaseStatus();
        return status.isHealthy()
                ? Health.up().withDetail("status", status.getDetail()).build()
                : Health.down().withDetail("status", status.getDetail()).build();
    }
}
