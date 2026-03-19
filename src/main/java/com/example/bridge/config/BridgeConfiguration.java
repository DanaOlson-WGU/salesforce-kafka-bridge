package com.example.bridge.config;

import com.example.bridge.model.EventBatch;
import com.example.bridge.pubsub.GrpcPubSubConnector;
import com.example.bridge.pubsub.GrpcStreamAdapter;
import com.example.bridge.pubsub.SalesforceAuthService;
import com.example.bridge.replay.ReplayStore;
import com.example.bridge.resilience.CircuitBreaker;
import com.example.bridge.service.EventProcessingService;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Configuration class that wires together the bridge components.
 * Uses @Lazy to break the circular dependency between GrpcPubSubConnector and EventProcessingService.
 */
@Configuration
public class BridgeConfiguration {

    @Value("${kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Bean
    public ProducerFactory<String, GenericRecord> producerFactory(
            SchemaRegistryProperties schemaRegistryProperties) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put("schema.registry.url", schemaRegistryProperties.getUrl());

        if (schemaRegistryProperties.getAuthSource() != null
                && !schemaRegistryProperties.getAuthSource().isEmpty()) {
            props.put("basic.auth.credentials.source", schemaRegistryProperties.getAuthSource());
            props.put("basic.auth.user.info", schemaRegistryProperties.getUserInfo());
        }

        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, GenericRecord> kafkaTemplate(
            ProducerFactory<String, GenericRecord> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    public ConsumerFactory<String, GenericRecord> consumerFactory(
            SchemaRegistryProperties schemaRegistryProperties) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put("schema.registry.url", schemaRegistryProperties.getUrl());
        props.put("specific.avro.reader", false);

        if (schemaRegistryProperties.getAuthSource() != null
                && !schemaRegistryProperties.getAuthSource().isEmpty()) {
            props.put("basic.auth.credentials.source", schemaRegistryProperties.getAuthSource());
            props.put("basic.auth.user.info", schemaRegistryProperties.getUserInfo());
        }

        return new DefaultKafkaConsumerFactory<>(props);
    }

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
