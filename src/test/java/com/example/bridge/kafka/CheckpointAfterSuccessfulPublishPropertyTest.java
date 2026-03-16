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
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Property 7: Checkpoint After Successful Publish
 *
 * For any batch where all events are successfully published to Kafka,
 * the bridge should persist the latest replay ID from that batch to the Replay Store.
 *
 * Validates: Requirements 2.4
 */
class CheckpointAfterSuccessfulPublishPropertyTest {

    private final ObjectMapper objectMapper = new ObjectMapper();


    // -----------------------------------------------------------------------
    // Property: Checkpoint called exactly once after successful single-event batch
    // -----------------------------------------------------------------------

    @Property(tries = 100)
    void checkpointShouldBeCalledAfterSuccessfulSingleEventPublish(
            @ForAll("orgNames") String org,
            @ForAll("salesforceTopics") String salesforceTopic,
            @ForAll("eventTypes") String eventType,
            @ForAll("replayIds") byte[] replayIdBytes,
            @ForAll("timestamps") Instant receivedAt) throws CheckpointException {

        List<ProducerRecord<String, byte[]>> capturedRecords = new ArrayList<>();
        KafkaTemplate<String, byte[]> kafkaTemplate = createCapturingKafkaTemplate(capturedRecords);
        TopicRouter topicRouter = mock(TopicRouter.class);
        ReplayStore replayStore = mock(ReplayStore.class);

        String kafkaTopic = "salesforce." + org + ".events";
        when(topicRouter.getKafkaTopic(org, salesforceTopic)).thenReturn(Optional.of(kafkaTopic));

        DefaultKafkaEventPublisher publisher = new DefaultKafkaEventPublisher(
                kafkaTemplate, topicRouter, replayStore, objectMapper, "1.0.0", 30);

        // Create event batch
        ReplayId replayId = new ReplayId(replayIdBytes);
        JsonNode payload = createPayload(eventType);
        PlatformEvent event = new PlatformEvent(eventType, payload, replayId);
        EventBatch batch = new EventBatch(org, salesforceTopic, List.of(event), replayId, receivedAt);

        // Publish
        PublishResult result = publisher.publishBatch(batch);

        // Verify checkpoint was called exactly once with correct parameters
        assertTrue(result.isSuccess(), "Publish should succeed");
        assertEquals(1, result.getPublishedCount(), "Should publish exactly one event");
        verify(replayStore, times(1)).checkpoint(eq(org), eq(salesforceTopic), eq(replayId));
    }

    // -----------------------------------------------------------------------
    // Property: Checkpoint uses batch replay ID (latest) for multi-event batches
    // -----------------------------------------------------------------------

    @Property(tries = 50)
    void checkpointShouldUseBatchReplayIdForMultiEventBatch(
            @ForAll("orgNames") String org,
            @ForAll("salesforceTopics") String salesforceTopic,
            @ForAll @IntRange(min = 2, max = 10) int eventCount,
            @ForAll("timestamps") Instant receivedAt) throws CheckpointException {

        List<ProducerRecord<String, byte[]>> capturedRecords = new ArrayList<>();
        KafkaTemplate<String, byte[]> kafkaTemplate = createCapturingKafkaTemplate(capturedRecords);
        TopicRouter topicRouter = mock(TopicRouter.class);
        ReplayStore replayStore = mock(ReplayStore.class);

        String kafkaTopic = "salesforce." + org + ".events";
        when(topicRouter.getKafkaTopic(org, salesforceTopic)).thenReturn(Optional.of(kafkaTopic));

        DefaultKafkaEventPublisher publisher = new DefaultKafkaEventPublisher(
                kafkaTemplate, topicRouter, replayStore, objectMapper, "1.0.0", 30);

        // Create batch with multiple events, each with different replay IDs
        List<PlatformEvent> events = new ArrayList<>();
        ReplayId lastReplayId = null;
        for (int i = 0; i < eventCount; i++) {
            byte[] eventReplayIdBytes = ("replay-event-" + i).getBytes(StandardCharsets.UTF_8);
            ReplayId eventReplayId = new ReplayId(eventReplayIdBytes);
            String evtType = "Event_Type_" + i + "__e";
            JsonNode payload = createPayload(evtType);
            events.add(new PlatformEvent(evtType, payload, eventReplayId));
            lastReplayId = eventReplayId;
        }

        // Batch replay ID is the latest (last event's replay ID)
        byte[] batchReplayIdBytes = ("batch-final-replay-id").getBytes(StandardCharsets.UTF_8);
        ReplayId batchReplayId = new ReplayId(batchReplayIdBytes);
        EventBatch batch = new EventBatch(org, salesforceTopic, events, batchReplayId, receivedAt);

        // Publish
        PublishResult result = publisher.publishBatch(batch);

        // Verify checkpoint uses the batch's replay ID (not individual event replay IDs)
        assertTrue(result.isSuccess(), "Publish should succeed");
        assertEquals(eventCount, result.getPublishedCount(), "Should publish all events");
        verify(replayStore, times(1)).checkpoint(eq(org), eq(salesforceTopic), eq(batchReplayId));
    }

    // -----------------------------------------------------------------------
    // Property: Checkpoint called for empty batches (to advance replay position)
    // -----------------------------------------------------------------------

    @Property(tries = 50)
    void checkpointShouldBeCalledForEmptyBatch(
            @ForAll("orgNames") String org,
            @ForAll("salesforceTopics") String salesforceTopic,
            @ForAll("replayIds") byte[] replayIdBytes,
            @ForAll("timestamps") Instant receivedAt) throws CheckpointException {

        List<ProducerRecord<String, byte[]>> capturedRecords = new ArrayList<>();
        KafkaTemplate<String, byte[]> kafkaTemplate = createCapturingKafkaTemplate(capturedRecords);
        TopicRouter topicRouter = mock(TopicRouter.class);
        ReplayStore replayStore = mock(ReplayStore.class);

        DefaultKafkaEventPublisher publisher = new DefaultKafkaEventPublisher(
                kafkaTemplate, topicRouter, replayStore, objectMapper, "1.0.0", 30);

        // Create empty batch
        ReplayId replayId = new ReplayId(replayIdBytes);
        EventBatch batch = new EventBatch(org, salesforceTopic, List.of(), replayId, receivedAt);

        // Publish
        PublishResult result = publisher.publishBatch(batch);

        // Verify checkpoint was called even for empty batch
        assertTrue(result.isSuccess(), "Publish should succeed for empty batch");
        assertEquals(0, result.getPublishedCount(), "Should publish zero events");
        assertEquals(0, capturedRecords.size(), "No Kafka records should be sent");
        verify(replayStore, times(1)).checkpoint(eq(org), eq(salesforceTopic), eq(replayId));
    }


    // -----------------------------------------------------------------------
    // Property: Checkpoint org and topic match the batch org and topic
    // -----------------------------------------------------------------------

    @Property(tries = 100)
    void checkpointOrgAndTopicShouldMatchBatch(
            @ForAll("orgNames") String org,
            @ForAll("salesforceTopics") String salesforceTopic,
            @ForAll("eventTypes") String eventType,
            @ForAll("replayIds") byte[] replayIdBytes,
            @ForAll("timestamps") Instant receivedAt) throws CheckpointException {

        List<ProducerRecord<String, byte[]>> capturedRecords = new ArrayList<>();
        KafkaTemplate<String, byte[]> kafkaTemplate = createCapturingKafkaTemplate(capturedRecords);
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

        assertTrue(result.isSuccess());
        // Verify checkpoint was called with the exact org and topic from the batch
        verify(replayStore).checkpoint(org, salesforceTopic, replayId);
        verifyNoMoreInteractions(replayStore);
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

    // -----------------------------------------------------------------------
    // Helper methods
    // -----------------------------------------------------------------------

    @SuppressWarnings({"unchecked", "rawtypes"})
    private KafkaTemplate<String, byte[]> createCapturingKafkaTemplate(
            List<ProducerRecord<String, byte[]>> capturedRecords) {
        return new KafkaTemplate<String, byte[]>(mock(org.springframework.kafka.core.ProducerFactory.class)) {
            @Override
            public CompletableFuture<SendResult<String, byte[]>> send(ProducerRecord record) {
                capturedRecords.add(record);
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
