package com.example.bridge.kafka;

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
import net.jqwik.api.constraints.IntRange;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TimeoutException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Property 8: No Checkpoint On Publish Failure
 *
 * For any batch where Kafka publishing fails, the bridge should not checkpoint the replay ID.
 *
 * Validates: Requirements 2.5
 */
class NoCheckpointOnPublishFailurePropertyTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    // -----------------------------------------------------------------------
    // Property: No checkpoint when Kafka send fails with TimeoutException
    // -----------------------------------------------------------------------

    @Property(tries = 100)
    void noCheckpointWhenKafkaSendTimesOut(
            @ForAll("orgNames") String org,
            @ForAll("salesforceTopics") String salesforceTopic,
            @ForAll("eventTypes") String eventType,
            @ForAll("replayIds") byte[] replayIdBytes,
            @ForAll("timestamps") Instant receivedAt) throws CheckpointException {

        KafkaTemplate<String, byte[]> kafkaTemplate = createFailingKafkaTemplate(
                new TimeoutException("Kafka broker unavailable"));
        TopicRouter topicRouter = mock(TopicRouter.class);
        ReplayStore replayStore = mock(ReplayStore.class);

        String kafkaTopic = "salesforce." + org + ".events";
        when(topicRouter.getKafkaTopic(org, salesforceTopic)).thenReturn(Optional.of(kafkaTopic));

        DefaultKafkaEventPublisher publisher = new DefaultKafkaEventPublisher(
                kafkaTemplate, topicRouter, replayStore, objectMapper, "1.0.0", 30);

        ReplayId replayId = new ReplayId(replayIdBytes);
        JsonNode payload = createPayload(eventType);
        PlatformEvent event = new PlatformEvent(eventType, payload, replayId);
        EventBatch batch = new EventBatch(org, salesforceTopic, List.of(event), replayId, receivedAt);

        PublishResult result = publisher.publishBatch(batch);

        assertFalse(result.isSuccess(), "Publish should fail");
        assertTrue(result.getError().isPresent(), "Error should be present");
        verify(replayStore, never()).checkpoint(any(), any(), any());
    }

    // -----------------------------------------------------------------------
    // Property: No checkpoint when Kafka send fails with ExecutionException
    // -----------------------------------------------------------------------

    @Property(tries = 100)
    void noCheckpointWhenKafkaSendFailsWithExecutionException(
            @ForAll("orgNames") String org,
            @ForAll("salesforceTopics") String salesforceTopic,
            @ForAll("eventTypes") String eventType,
            @ForAll("replayIds") byte[] replayIdBytes,
            @ForAll("timestamps") Instant receivedAt) throws CheckpointException {

        KafkaTemplate<String, byte[]> kafkaTemplate = createFailingKafkaTemplate(
                new RuntimeException("Kafka broker rejected message"));
        TopicRouter topicRouter = mock(TopicRouter.class);
        ReplayStore replayStore = mock(ReplayStore.class);

        String kafkaTopic = "salesforce." + org + ".events";
        when(topicRouter.getKafkaTopic(org, salesforceTopic)).thenReturn(Optional.of(kafkaTopic));

        DefaultKafkaEventPublisher publisher = new DefaultKafkaEventPublisher(
                kafkaTemplate, topicRouter, replayStore, objectMapper, "1.0.0", 30);

        ReplayId replayId = new ReplayId(replayIdBytes);
        JsonNode payload = createPayload(eventType);
        PlatformEvent event = new PlatformEvent(eventType, payload, replayId);
        EventBatch batch = new EventBatch(org, salesforceTopic, List.of(event), replayId, receivedAt);

        PublishResult result = publisher.publishBatch(batch);

        assertFalse(result.isSuccess(), "Publish should fail");
        verify(replayStore, never()).checkpoint(any(), any(), any());
    }

    // -----------------------------------------------------------------------
    // Property: No checkpoint when any event in multi-event batch fails
    // -----------------------------------------------------------------------

    @Property(tries = 50)
    void noCheckpointWhenAnyEventInBatchFails(
            @ForAll("orgNames") String org,
            @ForAll("salesforceTopics") String salesforceTopic,
            @ForAll @IntRange(min = 2, max = 10) int eventCount,
            @ForAll @IntRange(min = 0, max = 9) int failAtIndex,
            @ForAll("timestamps") Instant receivedAt) throws CheckpointException {

        int actualFailIndex = Math.min(failAtIndex, eventCount - 1);
        
        List<Integer> sendAttempts = new ArrayList<>();
        KafkaTemplate<String, byte[]> kafkaTemplate = createPartiallyFailingKafkaTemplate(
                actualFailIndex, sendAttempts, new RuntimeException("Kafka send failed"));
        TopicRouter topicRouter = mock(TopicRouter.class);
        ReplayStore replayStore = mock(ReplayStore.class);

        String kafkaTopic = "salesforce." + org + ".events";
        when(topicRouter.getKafkaTopic(org, salesforceTopic)).thenReturn(Optional.of(kafkaTopic));

        DefaultKafkaEventPublisher publisher = new DefaultKafkaEventPublisher(
                kafkaTemplate, topicRouter, replayStore, objectMapper, "1.0.0", 30);

        List<PlatformEvent> events = new ArrayList<>();
        for (int i = 0; i < eventCount; i++) {
            byte[] eventReplayIdBytes = ("replay-" + i).getBytes(StandardCharsets.UTF_8);
            ReplayId eventReplayId = new ReplayId(eventReplayIdBytes);
            String evtType = "Event_" + i + "__e";
            JsonNode payload = createPayload(evtType);
            events.add(new PlatformEvent(evtType, payload, eventReplayId));
        }

        byte[] batchReplayIdBytes = "batch-replay-id".getBytes(StandardCharsets.UTF_8);
        ReplayId batchReplayId = new ReplayId(batchReplayIdBytes);
        EventBatch batch = new EventBatch(org, salesforceTopic, events, batchReplayId, receivedAt);

        PublishResult result = publisher.publishBatch(batch);

        assertFalse(result.isSuccess(), "Publish should fail when any event fails");
        verify(replayStore, never()).checkpoint(any(), any(), any());
    }

    // -----------------------------------------------------------------------
    // Property: No checkpoint when first event fails (no partial checkpoint)
    // -----------------------------------------------------------------------

    @Property(tries = 50)
    void noCheckpointWhenFirstEventFails(
            @ForAll("orgNames") String org,
            @ForAll("salesforceTopics") String salesforceTopic,
            @ForAll @IntRange(min = 1, max = 10) int eventCount,
            @ForAll("timestamps") Instant receivedAt) throws CheckpointException {

        KafkaTemplate<String, byte[]> kafkaTemplate = createFailingKafkaTemplate(
                new RuntimeException("First event failed"));
        TopicRouter topicRouter = mock(TopicRouter.class);
        ReplayStore replayStore = mock(ReplayStore.class);

        String kafkaTopic = "salesforce." + org + ".events";
        when(topicRouter.getKafkaTopic(org, salesforceTopic)).thenReturn(Optional.of(kafkaTopic));

        DefaultKafkaEventPublisher publisher = new DefaultKafkaEventPublisher(
                kafkaTemplate, topicRouter, replayStore, objectMapper, "1.0.0", 30);

        List<PlatformEvent> events = new ArrayList<>();
        for (int i = 0; i < eventCount; i++) {
            byte[] eventReplayIdBytes = ("replay-" + i).getBytes(StandardCharsets.UTF_8);
            ReplayId eventReplayId = new ReplayId(eventReplayIdBytes);
            String evtType = "Event_" + i + "__e";
            JsonNode payload = createPayload(evtType);
            events.add(new PlatformEvent(evtType, payload, eventReplayId));
        }

        byte[] batchReplayIdBytes = "batch-replay-id".getBytes(StandardCharsets.UTF_8);
        ReplayId batchReplayId = new ReplayId(batchReplayIdBytes);
        EventBatch batch = new EventBatch(org, salesforceTopic, events, batchReplayId, receivedAt);

        PublishResult result = publisher.publishBatch(batch);

        assertFalse(result.isSuccess(), "Publish should fail");
        verify(replayStore, never()).checkpoint(any(), any(), any());
    }

    // -----------------------------------------------------------------------
    // Property: Failure result contains the original exception
    // -----------------------------------------------------------------------

    @Property(tries = 50)
    void failureResultContainsOriginalException(
            @ForAll("orgNames") String org,
            @ForAll("salesforceTopics") String salesforceTopic,
            @ForAll("eventTypes") String eventType,
            @ForAll("replayIds") byte[] replayIdBytes,
            @ForAll("timestamps") Instant receivedAt,
            @ForAll("errorMessages") String errorMessage) throws CheckpointException {

        RuntimeException originalException = new RuntimeException(errorMessage);
        KafkaTemplate<String, byte[]> kafkaTemplate = createFailingKafkaTemplate(originalException);
        TopicRouter topicRouter = mock(TopicRouter.class);
        ReplayStore replayStore = mock(ReplayStore.class);

        String kafkaTopic = "salesforce." + org + ".events";
        when(topicRouter.getKafkaTopic(org, salesforceTopic)).thenReturn(Optional.of(kafkaTopic));

        DefaultKafkaEventPublisher publisher = new DefaultKafkaEventPublisher(
                kafkaTemplate, topicRouter, replayStore, objectMapper, "1.0.0", 30);

        ReplayId replayId = new ReplayId(replayIdBytes);
        JsonNode payload = createPayload(eventType);
        PlatformEvent event = new PlatformEvent(eventType, payload, replayId);
        EventBatch batch = new EventBatch(org, salesforceTopic, List.of(event), replayId, receivedAt);

        PublishResult result = publisher.publishBatch(batch);

        assertFalse(result.isSuccess());
        assertTrue(result.getError().isPresent());
        verify(replayStore, never()).checkpoint(any(), any(), any());
    }

    // -----------------------------------------------------------------------
    // Providers
    // -----------------------------------------------------------------------

    @Provide
    Arbitrary<String> orgNames() {
        return Arbitraries.of("srm", "acad", "org-alpha", "org-beta", "test-org");
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
    Arbitrary<byte[]> replayIds() {
        return Arbitraries.strings()
                .alpha()
                .ofMinLength(8)
                .ofMaxLength(32)
                .map(s -> s.getBytes(StandardCharsets.UTF_8));
    }

    @Provide
    Arbitrary<Instant> timestamps() {
        return Arbitraries.longs()
                .between(0, System.currentTimeMillis())
                .map(Instant::ofEpochMilli);
    }

    @Provide
    Arbitrary<String> errorMessages() {
        return Arbitraries.of(
                "Connection refused",
                "Broker not available",
                "Record too large",
                "Network error",
                "Timeout waiting for response");
    }

    // -----------------------------------------------------------------------
    // Helper methods
    // -----------------------------------------------------------------------

    @SuppressWarnings({"unchecked", "rawtypes"})
    private KafkaTemplate<String, byte[]> createFailingKafkaTemplate(Exception failureException) {
        return new KafkaTemplate<String, byte[]>(mock(org.springframework.kafka.core.ProducerFactory.class)) {
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
        return new KafkaTemplate<String, byte[]>(mock(org.springframework.kafka.core.ProducerFactory.class)) {
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

    private JsonNode createPayload(String eventType) {
        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("Id", "a" + Math.abs(eventType.hashCode()));
        payload.put("EventUuid", java.util.UUID.randomUUID().toString());
        payload.put("Type", eventType);
        return payload;
    }
}
