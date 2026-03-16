package com.example.bridge.config;

import com.example.bridge.kafka.KafkaEventPublisher;
import com.example.bridge.model.EventBatch;
import com.example.bridge.pubsub.GrpcPubSubConnector;
import com.example.bridge.pubsub.GrpcStreamAdapter;
import com.example.bridge.pubsub.SalesforceAuthService;
import com.example.bridge.replay.ReplayStore;
import com.example.bridge.resilience.CircuitBreaker;
import com.example.bridge.routing.TopicRouter;
import com.example.bridge.service.EventProcessingService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

import java.util.function.Consumer;

/**
 * Configuration class that wires together the bridge components.
 * Uses @Lazy to break the circular dependency between GrpcPubSubConnector and EventProcessingService.
 */
@Configuration
public class BridgeConfiguration {

    @Bean
    public GrpcPubSubConnector grpcPubSubConnector(
            SalesforceProperties salesforceProperties,
            SalesforceAuthService authService,
            ReplayStore replayStore,
            CircuitBreaker circuitBreaker,
            GrpcStreamAdapter streamAdapter,
            @Lazy EventProcessingService eventProcessingService) {

        // The batch processor delegates to EventProcessingService
        Consumer<EventBatch> batchProcessor = eventProcessingService::processBatch;

        return new GrpcPubSubConnector(
                salesforceProperties,
                authService,
                replayStore,
                circuitBreaker,
                streamAdapter,
                batchProcessor
        );
    }
}
