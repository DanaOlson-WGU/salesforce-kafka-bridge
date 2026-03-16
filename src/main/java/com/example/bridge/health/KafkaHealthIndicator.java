package com.example.bridge.health;

import com.example.bridge.kafka.KafkaEventPublisher;
import com.example.bridge.model.ConnectionStatus;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * Reports the Kafka producer connection status.
 * Returns DOWN (503) when the Kafka producer is unavailable.
 */
@Component
public class KafkaHealthIndicator implements HealthIndicator {

    private final KafkaEventPublisher kafkaEventPublisher;

    public KafkaHealthIndicator(KafkaEventPublisher kafkaEventPublisher) {
        this.kafkaEventPublisher = kafkaEventPublisher;
    }

    @Override
    public Health health() {
        ConnectionStatus status = kafkaEventPublisher.getKafkaStatus();
        return status.isHealthy()
                ? Health.up().withDetail("status", status.getDetail()).build()
                : Health.down().withDetail("status", status.getDetail()).build();
    }
}
