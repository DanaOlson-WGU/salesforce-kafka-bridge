package com.example.bridge.kafka;

import com.example.bridge.model.ConnectionStatus;
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
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Unit tests for KafkaEventPublisher.
 * 
 * Tests cover:
 * - Successful batch publish returns success result
 * - Kafka failure returns failure result without checkpoint
 * - Headers are set correctly on published messages
 * - Composite key format
 * 
 * Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6
 */
class KafkaEventPublisherTest {

    private static final String HEADER_REPLAY_ID = "replay-id";
    private static final String HEADER_ORG = "org";
    private static final String HEADER_EVENT_TYPE = "event-type";
    private static final String HEADER_RECEIVED_AT = "received-at";
    private static final String HEADER_BRIDGE_VERSION = "bridge-version";

    private final ObjectMapper objectMapper = new ObjectMapper();
    private TopicRouter topicRouter;
    private ReplayStore replayStore;
    private List<ProducerRecord<String, byte[]>> capturedRecords;

    @BeforeEach
    void setUp() {
        topicRouter = mock(TopicRouter.class);
        replayStore = mock(ReplayStore.class);
        capturedRecords = new ArrayList<>();
    }

    // =========================================================================
    // Successful Batch Publish Tests
    // =========================================================================

    @Nested
    @DisplayName("Successful batch publish")
    class SuccessfulBatchPublish {

        @Test
        @DisplayName("should return success result with correct published count for single event")
        void successfulSingleEventPublish() throws CheckpointException {
            KafkaTemplate<String, byte[]> kafkaTemplate = createCapturingKafkaTemplate(capturedRecords);
            when(topicRouter.getKafkaTopic("srm", "/event/Enrollment_Event__e"))
                    .thenReturn(Optional.of("salesforce.srm.enrollment"));

            DefaultKafkaEventPublisher publisher = new DefaultKafkaEventPublisher(
                    kafkaTemplate, topicRouter, replayStore, objectMapper, "1.0.0", 30);

            EventBatch batch = createSingleEventBatch("srm", "/event/Enrollment_Event__e", "Enrollment_Event__e");

            PublishResult result = publisher.publishBatch(batch);

            assertTrue(result.isSuccess());
            assertEquals(1, result.getPublishedCount());
            assertFalse(result.getError().isPresent());
            assertEquals(1, capturedRecords.size());
            verify(replayStore, times(1)).checkpoint(eq("srm"), eq("/event/Enrollment_Event__e"), any(ReplayId.class));
        }

        @Test
        @DisplayName("should return success result with correct count for multi-event batch")
        void successfulMultiEventPublish() throws CheckpointException {
            KafkaTemplate<String, byte[]> kafkaTemplate = createCapturingKafkaTemplate(capturedRecords);
            when(topicRouter.getKafkaTopic("acad", "/event/Course_Event__e"))
                    .thenReturn(Optional.of("salesforce.acad.course"));

            DefaultKafkaEventPublisher publisher = new DefaultKafkaEventPublisher(
                    kafkaTemplate, topicRouter, replayStore, objectMapper, "1.0.0", 30);

            EventBatch batch = createMultiEventBatch("acad", "/event/Course_Event__e", 5);

            PublishResult result = publisher.publishBatch(batch);

            assertTrue(result.isSuccess());
            assertEquals(5, result.getPublishedCount());
            assertEquals(5, capturedRecords.size());
            verify(replayStore, times(1)).checkpoint(eq("acad"), eq("/event/Course_Event__e"), any(ReplayId.class));
        }

        @Test
        @DisplayName("should return success with zero count for empty batch and still checkpoint")
        void successfulEmptyBatchPublish() throws CheckpointException {
            KafkaTemplate<String, byte[]> kafkaTemplate = createCapturingKafkaTemplate(capturedRecords);

            DefaultKafkaEventPublisher publisher = new DefaultKafkaEventPublisher(
                    kafkaTemplate, topicRouter, replayStore, objectMapper, "1.0.0", 30);

            ReplayId replayId = new ReplayId("empty-batch-replay".getBytes(StandardCharsets.UTF_8));
            EventBatch batch = new EventBatch("srm", "/event/Test__e", List.of(), replayId, Instant.now());

            PublishResult result = publisher.publishBatch(batch);

            assertTrue(result.isSuccess());
            assertEquals(0, result.getPublishedCount());
            assertEquals(0, capturedRecords.size());
            verify(replayStore, times(1)).checkpoint("srm", "/event/Test__e", replayId);
        }

        @Test
        @DisplayName("should return success with zero count for unmapped topic without checkpoint")
        void unmappedTopicReturnsSuccessWithoutCheckpoint() throws CheckpointException {
            KafkaTemplate<String, byte[]> kafkaTemplate = createCapturingKafkaTemplate(capturedRecords);
            when(topicRouter.getKafkaTopic("srm", "/event/Unknown__e"))
                    .thenReturn(Optional.empty());

            DefaultKafkaEventPublisher publisher = new DefaultKafkaEventPublisher(
                    kafkaTemplate, topicRouter, replayStore, objectMapper, "1.0.0", 30);

            EventBatch batch = createSingleEventBatch("srm", "/event/Unknown__e", "Unknown__e");

            PublishResult result = publisher.publishBatch(batch);

            assertTrue(result.isSuccess());
            assertEquals(0, result.getPublishedCount());
            assertEquals(0, capturedRecords.size());
            verify(replayStore, never()).checkpoint(any(), any(), any());
        }
    }

    // =========================================================================
    // Kafka Failure Tests
    // =========================================================================

    @Nested
    @DisplayName("Kafka failure handling")
    class KafkaFailureHandling {

        @Test
        @DisplayName("should return failure result without checkpoint when Kafka send fails")
        void kafkaFailureReturnsFailureWithoutCheckpoint() throws CheckpointException {
            KafkaTemplate<String, byte[]> kafkaTemplate = createFailingKafkaTemplate(
                    new RuntimeException("Kafka broker unavailable"));
            when(topicRouter.getKafkaTopic("srm", "/event/Enrollment_Event__e"))
                    .thenReturn(Optional.of("salesforce.srm.enrollment"));

            DefaultKafkaEventPublisher publisher = new DefaultKafkaEventPublisher(
                    kafkaTemplate, topicRouter, replayStore, objectMapper, "1.0.0", 30);

            EventBatch batch = createSingleEventBatch("srm", "/event/Enrollment_Event__e", "Enrollment_Event__e");

            PublishResult result = publisher.publishBatch(batch);

            assertFalse(result.isSuccess());
            assertTrue(result.getError().isPresent());
            verify(replayStore, never()).checkpoint(any(), any(), any());
        }

        @Test
        @DisplayName("should return failure result when Kafka times out")
        void kafkaTimeoutReturnsFailure() throws CheckpointException {
            KafkaTemplate<String, byte[]> kafkaTemplate = createFailingKafkaTemplate(
                    new org.apache.kafka.common.errors.TimeoutException("Request timed out"));
            when(topicRouter.getKafkaTopic("acad", "/event/Course_Event__e"))
                    .thenReturn(Optional.of("salesforce.acad.course"));

            DefaultKafkaEventPublisher publisher = new DefaultKafkaEventPublisher(
                    kafkaTemplate, topicRouter, replayStore, objectMapper, "1.0.0", 30);

            EventBatch batch = createSingleEventBatch("acad", "/event/Course_Event__e", "Course_Event__e");

            PublishResult result = publisher.publishBatch(batch);

            assertFalse(result.isSuccess());
            assertTrue(result.getError().isPresent());
            verify(replayStore, never()).checkpoint(any(), any(), any());
        }

        @Test
        @DisplayName("should not checkpoint when any event in batch fails")
        void partialBatchFailureNoCheckpoint() throws CheckpointException {
            List<Integer> sendAttempts = new ArrayList<>();
            KafkaTemplate<String, byte[]> kafkaTemplate = createPartiallyFailingKafkaTemplate(
                    2, sendAttempts, new RuntimeException("Third event failed"));
            when(topicRouter.getKafkaTopic("srm", "/event/Test__e"))
                    .thenReturn(Optional.of("salesforce.srm.test"));

            DefaultKafkaEventPublisher publisher = new DefaultKafkaEventPublisher(
                    kafkaTemplate, topicRouter, replayStore, objectMapper, "1.0.0", 30);

            EventBatch batch = createMultiEventBatch("srm", "/event/Test__e", 5);

            PublishResult result = publisher.publishBatch(batch);

            assertFalse(result.isSuccess());
            verify(replayStore, never()).checkpoint(any(), any(), any());
        }

        @Test
        @DisplayName("should return failure when checkpoint fails")
        void checkpointFailureReturnsFailure() throws CheckpointException {
            KafkaTemplate<String, byte[]> kafkaTemplate = createCapturingKafkaTemplate(capturedRecords);
            when(topicRouter.getKafkaTopic("srm", "/event/Test__e"))
                    .thenReturn(Optional.of("salesforce.srm.test"));
            doThrow(new CheckpointException("Database unavailable"))
                    .when(replayStore).checkpoint(any(), any(), any());

            DefaultKafkaEventPublisher publisher = new DefaultKafkaEventPublisher(
                    kafkaTemplate, topicRouter, replayStore, objectMapper, "1.0.0", 30);

            EventBatch batch = createSingleEventBatch("srm", "/event/Test__e", "Test__e");

            PublishResult result = publisher.publishBatch(batch);

            assertFalse(result.isSuccess());
            assertTrue(result.getError().isPresent());
        }
    }

    // =========================================================================
    // Header Tests
    // =========================================================================

    @Nested
    @DisplayName("Message headers")
    class MessageHeaders {

        @Test
        @DisplayName("should include all required headers on published message")
        void allRequiredHeadersPresent() throws CheckpointException {
            KafkaTemplate<String, byte[]> kafkaTemplate = createCapturingKafkaTemplate(capturedRecords);
            when(topicRouter.getKafkaTopic("srm", "/event/Enrollment_Event__e"))
                    .thenReturn(Optional.of("salesforce.srm.enrollment"));

            DefaultKafkaEventPublisher publisher = new DefaultKafkaEventPublisher(
                    kafkaTemplate, topicRouter, replayStore, objectMapper, "1.0.0", 30);

            EventBatch batch = createSingleEventBatch("srm", "/event/Enrollment_Event__e", "Enrollment_Event__e");
            publisher.publishBatch(batch);

            assertEquals(1, capturedRecords.size());
            Headers headers = capturedRecords.get(0).headers();

            assertNotNull(headers.lastHeader(HEADER_REPLAY_ID), "replay-id header should be present");
            assertNotNull(headers.lastHeader(HEADER_ORG), "org header should be present");
            assertNotNull(headers.lastHeader(HEADER_EVENT_TYPE), "event-type header should be present");
            assertNotNull(headers.lastHeader(HEADER_RECEIVED_AT), "received-at header should be present");
            assertNotNull(headers.lastHeader(HEADER_BRIDGE_VERSION), "bridge-version header should be present");
        }

        @Test
        @DisplayName("should set correct org header value")
        void orgHeaderValueCorrect() throws CheckpointException {
            KafkaTemplate<String, byte[]> kafkaTemplate = createCapturingKafkaTemplate(capturedRecords);
            when(topicRouter.getKafkaTopic("acad", "/event/Course_Event__e"))
                    .thenReturn(Optional.of("salesforce.acad.course"));

            DefaultKafkaEventPublisher publisher = new DefaultKafkaEventPublisher(
                    kafkaTemplate, topicRouter, replayStore, objectMapper, "1.0.0", 30);

            EventBatch batch = createSingleEventBatch("acad", "/event/Course_Event__e", "Course_Event__e");
            publisher.publishBatch(batch);

            String orgValue = getHeaderString(capturedRecords.get(0).headers(), HEADER_ORG);
            assertEquals("acad", orgValue);
        }

        @Test
        @DisplayName("should set correct event-type header value")
        void eventTypeHeaderValueCorrect() throws CheckpointException {
            KafkaTemplate<String, byte[]> kafkaTemplate = createCapturingKafkaTemplate(capturedRecords);
            when(topicRouter.getKafkaTopic("srm", "/event/Student_Event__e"))
                    .thenReturn(Optional.of("salesforce.srm.student"));

            DefaultKafkaEventPublisher publisher = new DefaultKafkaEventPublisher(
                    kafkaTemplate, topicRouter, replayStore, objectMapper, "1.0.0", 30);

            EventBatch batch = createSingleEventBatch("srm", "/event/Student_Event__e", "Student_Event__e");
            publisher.publishBatch(batch);

            String eventTypeValue = getHeaderString(capturedRecords.get(0).headers(), HEADER_EVENT_TYPE);
            assertEquals("Student_Event__e", eventTypeValue);
        }

        @Test
        @DisplayName("should set correct replay-id header value")
        void replayIdHeaderValueCorrect() throws CheckpointException {
            KafkaTemplate<String, byte[]> kafkaTemplate = createCapturingKafkaTemplate(capturedRecords);
            when(topicRouter.getKafkaTopic("srm", "/event/Test__e"))
                    .thenReturn(Optional.of("salesforce.srm.test"));

            DefaultKafkaEventPublisher publisher = new DefaultKafkaEventPublisher(
                    kafkaTemplate, topicRouter, replayStore, objectMapper, "1.0.0", 30);

            byte[] replayIdBytes = "test-replay-id-12345".getBytes(StandardCharsets.UTF_8);
            ReplayId replayId = new ReplayId(replayIdBytes);
            JsonNode payload = createPayload("Test__e");
            PlatformEvent event = new PlatformEvent("Test__e", payload, replayId);
            EventBatch batch = new EventBatch("srm", "/event/Test__e", List.of(event), replayId, Instant.now());

            publisher.publishBatch(batch);

            byte[] headerValue = capturedRecords.get(0).headers().lastHeader(HEADER_REPLAY_ID).value();
            assertArrayEquals(replayIdBytes, headerValue);
        }

        @Test
        @DisplayName("should set correct bridge-version header value")
        void bridgeVersionHeaderValueCorrect() throws CheckpointException {
            KafkaTemplate<String, byte[]> kafkaTemplate = createCapturingKafkaTemplate(capturedRecords);
            when(topicRouter.getKafkaTopic("srm", "/event/Test__e"))
                    .thenReturn(Optional.of("salesforce.srm.test"));

            DefaultKafkaEventPublisher publisher = new DefaultKafkaEventPublisher(
                    kafkaTemplate, topicRouter, replayStore, objectMapper, "2.5.0", 30);

            EventBatch batch = createSingleEventBatch("srm", "/event/Test__e", "Test__e");
            publisher.publishBatch(batch);

            String versionValue = getHeaderString(capturedRecords.get(0).headers(), HEADER_BRIDGE_VERSION);
            assertEquals("2.5.0", versionValue);
        }

        @Test
        @DisplayName("should set received-at header with ISO-8601 timestamp")
        void receivedAtHeaderValueCorrect() throws CheckpointException {
            KafkaTemplate<String, byte[]> kafkaTemplate = createCapturingKafkaTemplate(capturedRecords);
            when(topicRouter.getKafkaTopic("srm", "/event/Test__e"))
                    .thenReturn(Optional.of("salesforce.srm.test"));

            DefaultKafkaEventPublisher publisher = new DefaultKafkaEventPublisher(
                    kafkaTemplate, topicRouter, replayStore, objectMapper, "1.0.0", 30);

            Instant receivedAt = Instant.parse("2024-03-15T10:30:00Z");
            ReplayId replayId = new ReplayId("replay".getBytes(StandardCharsets.UTF_8));
            JsonNode payload = createPayload("Test__e");
            PlatformEvent event = new PlatformEvent("Test__e", payload, replayId);
            EventBatch batch = new EventBatch("srm", "/event/Test__e", List.of(event), replayId, receivedAt);

            publisher.publishBatch(batch);

            String receivedAtValue = getHeaderString(capturedRecords.get(0).headers(), HEADER_RECEIVED_AT);
            assertEquals("2024-03-15T10:30:00Z", receivedAtValue);
        }
    }

    // =========================================================================
    // Composite Key Tests
    // =========================================================================

    @Nested
    @DisplayName("Composite key format")
    class CompositeKeyFormat {

        @Test
        @DisplayName("should use format org:topic:eventId for key")
        void compositeKeyFormatCorrect() throws CheckpointException {
            KafkaTemplate<String, byte[]> kafkaTemplate = createCapturingKafkaTemplate(capturedRecords);
            when(topicRouter.getKafkaTopic("srm", "/event/Enrollment_Event__e"))
                    .thenReturn(Optional.of("salesforce.srm.enrollment"));

            DefaultKafkaEventPublisher publisher = new DefaultKafkaEventPublisher(
                    kafkaTemplate, topicRouter, replayStore, objectMapper, "1.0.0", 30);

            ReplayId replayId = new ReplayId("replay".getBytes(StandardCharsets.UTF_8));
            ObjectNode payload = objectMapper.createObjectNode();
            payload.put("Id", "a001234567890ABC");
            payload.put("Type", "Enrollment_Event__e");
            PlatformEvent event = new PlatformEvent("Enrollment_Event__e", payload, replayId);
            EventBatch batch = new EventBatch("srm", "/event/Enrollment_Event__e", List.of(event), replayId, Instant.now());

            publisher.publishBatch(batch);

            String key = capturedRecords.get(0).key();
            assertEquals("srm:/event/Enrollment_Event__e:a001234567890ABC", key);
        }

        @Test
        @DisplayName("should use EventUuid when Id is not present")
        void compositeKeyUsesEventUuidWhenNoId() throws CheckpointException {
            KafkaTemplate<String, byte[]> kafkaTemplate = createCapturingKafkaTemplate(capturedRecords);
            when(topicRouter.getKafkaTopic("acad", "/event/Course_Event__e"))
                    .thenReturn(Optional.of("salesforce.acad.course"));

            DefaultKafkaEventPublisher publisher = new DefaultKafkaEventPublisher(
                    kafkaTemplate, topicRouter, replayStore, objectMapper, "1.0.0", 30);

            ReplayId replayId = new ReplayId("replay".getBytes(StandardCharsets.UTF_8));
            ObjectNode payload = objectMapper.createObjectNode();
            String eventUuid = "550e8400-e29b-41d4-a716-446655440000";
            payload.put("EventUuid", eventUuid);
            payload.put("Type", "Course_Event__e");
            PlatformEvent event = new PlatformEvent("Course_Event__e", payload, replayId);
            EventBatch batch = new EventBatch("acad", "/event/Course_Event__e", List.of(event), replayId, Instant.now());

            publisher.publishBatch(batch);

            String key = capturedRecords.get(0).key();
            assertEquals("acad:/event/Course_Event__e:" + eventUuid, key);
        }

        @Test
        @DisplayName("should generate UUID when neither Id nor EventUuid present")
        void compositeKeyGeneratesUuidWhenNoIdFields() throws CheckpointException {
            KafkaTemplate<String, byte[]> kafkaTemplate = createCapturingKafkaTemplate(capturedRecords);
            when(topicRouter.getKafkaTopic("srm", "/event/Test__e"))
                    .thenReturn(Optional.of("salesforce.srm.test"));

            DefaultKafkaEventPublisher publisher = new DefaultKafkaEventPublisher(
                    kafkaTemplate, topicRouter, replayStore, objectMapper, "1.0.0", 30);

            ReplayId replayId = new ReplayId("replay".getBytes(StandardCharsets.UTF_8));
            ObjectNode payload = objectMapper.createObjectNode();
            payload.put("Type", "Test__e");
            // No Id or EventUuid
            PlatformEvent event = new PlatformEvent("Test__e", payload, replayId);
            EventBatch batch = new EventBatch("srm", "/event/Test__e", List.of(event), replayId, Instant.now());

            publisher.publishBatch(batch);

            String key = capturedRecords.get(0).key();
            assertTrue(key.startsWith("srm:/event/Test__e:"));
            // Verify the last part is a valid UUID format
            String uuidPart = key.substring("srm:/event/Test__e:".length());
            assertDoesNotThrow(() -> UUID.fromString(uuidPart));
        }

        @Test
        @DisplayName("should publish to correct Kafka topic")
        void publishesToCorrectKafkaTopic() throws CheckpointException {
            KafkaTemplate<String, byte[]> kafkaTemplate = createCapturingKafkaTemplate(capturedRecords);
            when(topicRouter.getKafkaTopic("srm", "/event/Enrollment_Event__e"))
                    .thenReturn(Optional.of("salesforce.srm.enrollment"));

            DefaultKafkaEventPublisher publisher = new DefaultKafkaEventPublisher(
                    kafkaTemplate, topicRouter, replayStore, objectMapper, "1.0.0", 30);

            EventBatch batch = createSingleEventBatch("srm", "/event/Enrollment_Event__e", "Enrollment_Event__e");
            publisher.publishBatch(batch);

            assertEquals("salesforce.srm.enrollment", capturedRecords.get(0).topic());
        }
    }

    // =========================================================================
    // Health Check Tests
    // =========================================================================

    @Nested
    @DisplayName("Kafka status health check")
    class KafkaStatusHealthCheck {

        @Test
        @DisplayName("should return healthy status when Kafka is available")
        void healthyWhenKafkaAvailable() {
            // Use a custom KafkaTemplate subclass that simulates healthy Kafka
            KafkaTemplate<String, byte[]> kafkaTemplate = createHealthyKafkaTemplate();

            DefaultKafkaEventPublisher publisher = new DefaultKafkaEventPublisher(
                    kafkaTemplate, topicRouter, replayStore, objectMapper, "1.0.0", 30);

            ConnectionStatus status = publisher.getKafkaStatus();

            assertTrue(status.isHealthy());
            assertTrue(status.getDetail().contains("connected"));
        }

        @Test
        @DisplayName("should return unhealthy status when Kafka is unavailable")
        void unhealthyWhenKafkaUnavailable() {
            // Use a custom KafkaTemplate subclass that simulates unhealthy Kafka
            KafkaTemplate<String, byte[]> kafkaTemplate = createUnhealthyKafkaTemplate();

            DefaultKafkaEventPublisher publisher = new DefaultKafkaEventPublisher(
                    kafkaTemplate, topicRouter, replayStore, objectMapper, "1.0.0", 30);

            ConnectionStatus status = publisher.getKafkaStatus();

            assertFalse(status.isHealthy());
            assertTrue(status.getDetail().contains("unavailable"));
        }
    }

    // =========================================================================
    // Helper Methods
    // =========================================================================

    @SuppressWarnings({"unchecked", "rawtypes"})
    private KafkaTemplate<String, byte[]> createCapturingKafkaTemplate(
            List<ProducerRecord<String, byte[]>> capturedRecords) {
        return new KafkaTemplate<String, byte[]>(mock(ProducerFactory.class)) {
            @Override
            public CompletableFuture<SendResult<String, byte[]>> send(ProducerRecord record) {
                capturedRecords.add(record);
                return CompletableFuture.completedFuture(null);
            }
        };
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private KafkaTemplate<String, byte[]> createFailingKafkaTemplate(Exception failureException) {
        return new KafkaTemplate<String, byte[]>(mock(ProducerFactory.class)) {
            @Override
            public CompletableFuture<SendResult<String, byte[]>> send(ProducerRecord record) {
                CompletableFuture<SendResult<String, byte[]>> future = new CompletableFuture<>();
                future.completeExceptionally(new ExecutionException(failureException));
                return future;
            }
        };
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private KafkaTemplate<String, byte[]> createPartiallyFailingKafkaTemplate(
            int failAtIndex, List<Integer> sendAttempts, Exception failureException) {
        return new KafkaTemplate<String, byte[]>(mock(ProducerFactory.class)) {
            @Override
            public CompletableFuture<SendResult<String, byte[]>> send(ProducerRecord record) {
                int currentIndex = sendAttempts.size();
                sendAttempts.add(currentIndex);

                if (currentIndex == failAtIndex) {
                    CompletableFuture<SendResult<String, byte[]>> future = new CompletableFuture<>();
                    future.completeExceptionally(new ExecutionException(failureException));
                    return future;
                }
                return CompletableFuture.completedFuture(null);
            }
        };
    }

    private EventBatch createSingleEventBatch(String org, String salesforceTopic, String eventType) {
        ReplayId replayId = new ReplayId(("replay-" + UUID.randomUUID()).getBytes(StandardCharsets.UTF_8));
        JsonNode payload = createPayload(eventType);
        PlatformEvent event = new PlatformEvent(eventType, payload, replayId);
        return new EventBatch(org, salesforceTopic, List.of(event), replayId, Instant.now());
    }

    private EventBatch createMultiEventBatch(String org, String salesforceTopic, int eventCount) {
        List<PlatformEvent> events = new ArrayList<>();
        ReplayId lastReplayId = null;
        for (int i = 0; i < eventCount; i++) {
            lastReplayId = new ReplayId(("replay-" + i).getBytes(StandardCharsets.UTF_8));
            String eventType = "Event_" + i + "__e";
            JsonNode payload = createPayload(eventType);
            events.add(new PlatformEvent(eventType, payload, lastReplayId));
        }
        return new EventBatch(org, salesforceTopic, events, lastReplayId, Instant.now());
    }

    private JsonNode createPayload(String eventType) {
        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("Id", "a" + Math.abs(eventType.hashCode()));
        payload.put("EventUuid", UUID.randomUUID().toString());
        payload.put("Type", eventType);
        return payload;
    }

    private String getHeaderString(Headers headers, String headerName) {
        Header header = headers.lastHeader(headerName);
        return header != null ? new String(header.value(), StandardCharsets.UTF_8) : null;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private KafkaTemplate<String, byte[]> createHealthyKafkaTemplate() {
        return new KafkaTemplate<String, byte[]>(mock(ProducerFactory.class)) {
            @Override
            public ProducerFactory<String, byte[]> getProducerFactory() {
                // Return a ProducerFactory that creates a producer that doesn't throw
                return new ProducerFactory<String, byte[]>() {
                    @Override
                    public org.apache.kafka.clients.producer.Producer<String, byte[]> createProducer() {
                        return new org.apache.kafka.clients.producer.MockProducer<>(true, null, null);
                    }
                };
            }
        };
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private KafkaTemplate<String, byte[]> createUnhealthyKafkaTemplate() {
        return new KafkaTemplate<String, byte[]>(mock(ProducerFactory.class)) {
            @Override
            public ProducerFactory<String, byte[]> getProducerFactory() {
                // Return a ProducerFactory that throws when creating producer
                return new ProducerFactory<String, byte[]>() {
                    @Override
                    public org.apache.kafka.clients.producer.Producer<String, byte[]> createProducer() {
                        throw new RuntimeException("Connection refused");
                    }
                };
            }
        };
    }
}
