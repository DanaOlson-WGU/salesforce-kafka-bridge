package com.example.bridge.kafka;

import com.example.bridge.avro.AvroRecordConverter;
import com.example.bridge.model.EventBatch;
import com.example.bridge.model.PlatformEvent;
import com.example.bridge.model.PublishResult;
import com.example.bridge.model.ReplayId;
import com.example.bridge.replay.CheckpointException;
import com.example.bridge.replay.ReplayStore;
import com.example.bridge.routing.TopicRouter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import net.jqwik.api.*;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Property 3: Serialization Failure Skips Checkpoint
 *
 * For any event batch where Avro serialization fails for any event, the bridge
 * should not checkpoint the replay ID for that batch, and should return a failure
 * result containing the org identifier and event type.
 *
 * Validates: Requirements 3.5
 */
class AvroSerializationFailurePropertyTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Schema schema;

    AvroSerializationFailurePropertyTest() {
        try {
            this.schema = new Schema.Parser().parse(
                    getClass().getResourceAsStream("/avro/SalesforcePlatformEvent.avsc"));
        } catch (IOException e) {
            throw new RuntimeException("Failed to load Avro schema for tests", e);
        }
    }

    /**
     * Creates an AvroRecordConverter subclass that always throws on toGenericRecord.
     * Used instead of Mockito mock to avoid Java 25 inline mock limitations.
     */
    private AvroRecordConverter createFailingConverter(String errorMessage) {
        return new AvroRecordConverter(schema, objectMapper) {
            @Override
            public GenericRecord toGenericRecord(EventBatch batch, PlatformEvent event) {
                throw new IllegalStateException(errorMessage);
            }
        };
    }

    // -----------------------------------------------------------------------
    // Property 3: Serialization failure on any event skips checkpoint
    // -----------------------------------------------------------------------

    /**
     * Property 3: Serialization Failure Skips Checkpoint
     *
     * When AvroRecordConverter.toGenericRecord() throws an exception for any event
     * in a batch, the publisher should:
     * 1. Return a failure result (isSuccess() == false)
     * 2. Have an error present in the result
     * 3. Never call ReplayStore.checkpoint()
     *
     * Validates: Requirements 3.5
     */
    @Property(tries = 100)
    void serializationFailureSkipsCheckpoint(
            @ForAll("orgIdentifiers") String org,
            @ForAll("salesforceTopics") String salesforceTopic,
            @ForAll("eventTypes") String eventType,
            @ForAll("replayIdBytes") byte[] replayIdBytes,
            @ForAll("timestamps") Instant receivedAt) throws CheckpointException {

        // AvroRecordConverter subclass that throws on toGenericRecord
        AvroRecordConverter failingConverter = createFailingConverter(
                "Avro serialization failed for event type: " + eventType);

        // TopicRouter and ReplayStore are interfaces — safe to mock
        TopicRouter topicRouter = mock(TopicRouter.class);
        String kafkaTopic = "salesforce." + org + ".events";
        when(topicRouter.getKafkaTopic(org, salesforceTopic)).thenReturn(Optional.of(kafkaTopic));

        ReplayStore replayStore = mock(ReplayStore.class);

        // Capturing KafkaTemplate (should never be called)
        List<ProducerRecord<String, GenericRecord>> capturedRecords = new ArrayList<>();
        KafkaTemplate<String, GenericRecord> kafkaTemplate = createCapturingKafkaTemplate(capturedRecords);

        DefaultKafkaEventPublisher publisher = new DefaultKafkaEventPublisher(
                kafkaTemplate, topicRouter, replayStore, failingConverter, "1.0.0", 30);

        // Build the batch
        ReplayId replayId = new ReplayId(replayIdBytes);
        JsonNode payload = createPayload(eventType);
        PlatformEvent event = new PlatformEvent(eventType, payload, replayId);
        EventBatch batch = new EventBatch(org, salesforceTopic, List.of(event), replayId, receivedAt);

        // Act
        PublishResult result = publisher.publishBatch(batch);

        // Assert: result is failure
        assertFalse(result.isSuccess(),
                "Result should be failure when Avro serialization fails");

        // Assert: error is present
        assertTrue(result.getError().isPresent(),
                "Error should be present in failure result");

        // Assert: checkpoint is NEVER called
        verify(replayStore, never()).checkpoint(any(), any(), any());

        // Assert: no records were sent to Kafka
        assertEquals(0, capturedRecords.size(),
                "No records should be sent to Kafka when serialization fails");
    }

    // -----------------------------------------------------------------------
    // Property 3 variant: Multi-event batch with serialization failure
    // -----------------------------------------------------------------------

    @Property(tries = 100)
    void multiEventBatchSerializationFailureSkipsCheckpoint(
            @ForAll("orgIdentifiers") String org,
            @ForAll("salesforceTopics") String salesforceTopic,
            @ForAll("replayIdBytes") byte[] replayIdBytes,
            @ForAll("timestamps") Instant receivedAt) throws CheckpointException {

        AvroRecordConverter failingConverter = createFailingConverter("Avro serialization failed");

        TopicRouter topicRouter = mock(TopicRouter.class);
        String kafkaTopic = "salesforce." + org + ".events";
        when(topicRouter.getKafkaTopic(org, salesforceTopic)).thenReturn(Optional.of(kafkaTopic));

        ReplayStore replayStore = mock(ReplayStore.class);

        List<ProducerRecord<String, GenericRecord>> capturedRecords = new ArrayList<>();
        KafkaTemplate<String, GenericRecord> kafkaTemplate = createCapturingKafkaTemplate(capturedRecords);

        DefaultKafkaEventPublisher publisher = new DefaultKafkaEventPublisher(
                kafkaTemplate, topicRouter, replayStore, failingConverter, "1.0.0", 30);

        // Build a multi-event batch
        List<PlatformEvent> events = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            ReplayId eventReplayId = new ReplayId(("replay-" + i).getBytes(StandardCharsets.UTF_8));
            String evtType = "Event_" + i + "__e";
            JsonNode payload = createPayload(evtType);
            events.add(new PlatformEvent(evtType, payload, eventReplayId));
        }

        ReplayId batchReplayId = new ReplayId(replayIdBytes);
        EventBatch batch = new EventBatch(org, salesforceTopic, events, batchReplayId, receivedAt);

        PublishResult result = publisher.publishBatch(batch);

        assertFalse(result.isSuccess(),
                "Result should be failure when Avro serialization fails for multi-event batch");
        assertTrue(result.getError().isPresent(),
                "Error should be present");
        verify(replayStore, never()).checkpoint(any(), any(), any());
    }

    // -----------------------------------------------------------------------
    // Generators
    // -----------------------------------------------------------------------

    @Provide
    Arbitrary<String> orgIdentifiers() {
        return Arbitraries.of("SRM", "ACAD", "org-alpha", "org-beta", "test-org");
    }

    @Provide
    Arbitrary<String> salesforceTopics() {
        return Arbitraries.of(
                "/event/Enrollment_Event__e",
                "/event/Student_Event__e",
                "/event/Course_Event__e",
                "/event/Custom_Event__e");
    }

    @Provide
    Arbitrary<String> eventTypes() {
        return Arbitraries.of(
                "Enrollment_Event__e",
                "Student_Event__e",
                "Course_Event__e",
                "Custom_Event__e");
    }

    @Provide
    Arbitrary<byte[]> replayIdBytes() {
        return Arbitraries.bytes()
                .array(byte[].class)
                .ofMinSize(16)
                .ofMaxSize(32);
    }

    @Provide
    Arbitrary<Instant> timestamps() {
        return Arbitraries.longs()
                .between(0, Instant.now().getEpochSecond())
                .map(Instant::ofEpochSecond);
    }

    // -----------------------------------------------------------------------
    // Helper methods
    // -----------------------------------------------------------------------

    @SuppressWarnings({"unchecked", "rawtypes"})
    private KafkaTemplate<String, GenericRecord> createCapturingKafkaTemplate(
            List<ProducerRecord<String, GenericRecord>> capturedRecords) {
        return new KafkaTemplate<String, GenericRecord>(mock(ProducerFactory.class)) {
            @Override
            public CompletableFuture<SendResult<String, GenericRecord>> send(ProducerRecord record) {
                capturedRecords.add(record);
                return CompletableFuture.completedFuture(null);
            }
        };
    }

    private JsonNode createPayload(String eventType) {
        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("Id", "a" + Math.abs(eventType.hashCode()));
        payload.put("EventUuid", UUID.randomUUID().toString());
        payload.put("Type", eventType);
        return payload;
    }
}
