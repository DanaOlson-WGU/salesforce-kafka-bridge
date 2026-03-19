package com.example.bridge.kafka;

import com.example.bridge.avro.AvroRecordConverter;
import com.example.bridge.model.ConnectionStatus;
import com.example.bridge.model.EventBatch;
import com.example.bridge.model.PlatformEvent;
import com.example.bridge.model.PublishResult;
import com.example.bridge.replay.CheckpointException;
import com.example.bridge.replay.ReplayStore;
import com.example.bridge.routing.TopicRouter;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Default implementation of KafkaEventPublisher using Spring Kafka KafkaTemplate.
 * Publishes events with idempotent producer guarantees and coordinates checkpointing.
 */
@Component
public class DefaultKafkaEventPublisher implements KafkaEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(DefaultKafkaEventPublisher.class);

    private static final String HEADER_REPLAY_ID = "replay-id";
    private static final String HEADER_ORG = "org";
    private static final String HEADER_EVENT_TYPE = "event-type";
    private static final String HEADER_RECEIVED_AT = "received-at";
    private static final String HEADER_BRIDGE_VERSION = "bridge-version";

    private final KafkaTemplate<String, GenericRecord> kafkaTemplate;
    private final TopicRouter topicRouter;
    private final ReplayStore replayStore;
    private final AvroRecordConverter avroRecordConverter;
    private final String bridgeVersion;
    private final long sendTimeoutSeconds;

    public DefaultKafkaEventPublisher(
            KafkaTemplate<String, GenericRecord> kafkaTemplate,
            TopicRouter topicRouter,
            ReplayStore replayStore,
            AvroRecordConverter avroRecordConverter,
            @Value("${spring.application.version:0.0.1-SNAPSHOT}") String bridgeVersion,
            @Value("${bridge.kafka.send-timeout-seconds:30}") long sendTimeoutSeconds) {
        this.kafkaTemplate = kafkaTemplate;
        this.topicRouter = topicRouter;
        this.replayStore = replayStore;
        this.avroRecordConverter = avroRecordConverter;
        this.bridgeVersion = bridgeVersion;
        this.sendTimeoutSeconds = sendTimeoutSeconds;
    }

    @Override
    public PublishResult publishBatch(EventBatch batch) {
        if (batch.isEmpty()) {
            return handleEmptyBatch(batch);
        }

        Optional<String> kafkaTopicOpt = topicRouter.getKafkaTopic(batch.getOrg(), batch.getSalesforceTopic());
        if (kafkaTopicOpt.isEmpty()) {
            log.warn("No Kafka topic mapping for org={}, salesforceTopic={}. Discarding {} events.",
                    batch.getOrg(), batch.getSalesforceTopic(), batch.getEvents().size());
            return PublishResult.success(0);
        }

        String kafkaTopic = kafkaTopicOpt.get();
        int publishedCount = 0;

        try {
            for (PlatformEvent event : batch.getEvents()) {
                publishEvent(batch, event, kafkaTopic);
                publishedCount++;
            }

            checkpoint(batch);
            log.debug("Published {} events to {} for org={}", publishedCount, kafkaTopic, batch.getOrg());
            return PublishResult.success(publishedCount);

        } catch (Exception e) {
            log.error("Failed to publish batch for org={}, topic={}. Published {} of {} events before failure.",
                    batch.getOrg(), batch.getSalesforceTopic(), publishedCount, batch.getEvents().size(), e);
            return PublishResult.failure(e);
        }
    }


    private PublishResult handleEmptyBatch(EventBatch batch) {
        log.debug("Received empty batch for org={}, topic={}. Checkpointing replay ID.",
                batch.getOrg(), batch.getSalesforceTopic());
        try {
            checkpoint(batch);
            return PublishResult.success(0);
        } catch (CheckpointException e) {
            log.error("Failed to checkpoint empty batch for org={}, topic={}",
                    batch.getOrg(), batch.getSalesforceTopic(), e);
            return PublishResult.failure(e);
        }
    }

    private void publishEvent(EventBatch batch, PlatformEvent event, String kafkaTopic)
            throws ExecutionException, InterruptedException, TimeoutException {

        GenericRecord avroRecord = avroRecordConverter.toGenericRecord(batch, event);
        String key = buildCompositeKey(batch.getOrg(), batch.getSalesforceTopic(), event);
        Headers headers = buildHeaders(batch, event);

        ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(kafkaTopic, null, key, avroRecord, headers);

        kafkaTemplate.send(record)
                .get(sendTimeoutSeconds, TimeUnit.SECONDS);
    }

    private String buildCompositeKey(String org, String salesforceTopic, PlatformEvent event) {
        String eventId = extractEventId(event);
        return org + ":" + salesforceTopic + ":" + eventId;
    }

    private String extractEventId(PlatformEvent event) {
        if (event.getPayload().has("Id")) {
            return event.getPayload().get("Id").asText();
        }
        if (event.getPayload().has("EventUuid")) {
            return event.getPayload().get("EventUuid").asText();
        }
        return UUID.randomUUID().toString();
    }

    private Headers buildHeaders(EventBatch batch, PlatformEvent event) {
        RecordHeaders headers = new RecordHeaders();
        headers.add(HEADER_REPLAY_ID, event.getReplayId().getValue());
        headers.add(HEADER_ORG, batch.getOrg().getBytes(StandardCharsets.UTF_8));
        headers.add(HEADER_EVENT_TYPE, event.getEventType().getBytes(StandardCharsets.UTF_8));
        headers.add(HEADER_RECEIVED_AT, batch.getReceivedAt().toString().getBytes(StandardCharsets.UTF_8));
        headers.add(HEADER_BRIDGE_VERSION, bridgeVersion.getBytes(StandardCharsets.UTF_8));
        return headers;
    }

    private void checkpoint(EventBatch batch) throws CheckpointException {
        replayStore.checkpoint(batch.getOrg(), batch.getSalesforceTopic(), batch.getReplayId());
    }

    @Override
    public ConnectionStatus getKafkaStatus() {
        try {
            kafkaTemplate.getProducerFactory().createProducer().partitionsFor("__consumer_offsets");
            return ConnectionStatus.up("Kafka producer connected");
        } catch (Exception e) {
            log.warn("Kafka health check failed", e);
            return ConnectionStatus.down("Kafka producer unavailable: " + e.getMessage());
        }
    }
}
