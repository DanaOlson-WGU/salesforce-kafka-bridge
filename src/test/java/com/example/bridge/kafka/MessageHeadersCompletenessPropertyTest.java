package com.example.bridge.kafka;

import com.example.bridge.avro.AvroRecordConverter;
import com.example.bridge.model.EventBatch;
import com.example.bridge.model.PlatformEvent;
import com.example.bridge.model.PublishResult;
import com.example.bridge.model.ReplayId;
import com.example.bridge.replay.ReplayStore;
import com.example.bridge.routing.TopicRouter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import net.jqwik.api.*;
import net.jqwik.api.constraints.IntRange;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Property 6: Message Headers Completeness
 *
 * For any event published to Kafka, headers should include replay ID,
 * event type, org identifier, and reception timestamp.
 *
 * Validates: Requirements 2.3, 5.4
 */
class MessageHeadersCompletenessPropertyTest {

    private static final String HEADER_REPLAY_ID = "replay-id";
    private static final String HEADER_ORG = "org";
    private static final String HEADER_EVENT_TYPE = "event-type";
    private static final String HEADER_RECEIVED_AT = "received-at";
    private static final String HEADER_BRIDGE_VERSION = "bridge-version";

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final AvroRecordConverter avroRecordConverter;

    MessageHeadersCompletenessPropertyTest() {
        try {
            Schema schema = new Schema.Parser().parse(
                    getClass().getResourceAsStream("/avro/SalesforcePlatformEvent.avsc"));
            this.avroRecordConverter = new AvroRecordConverter(schema, objectMapper);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load Avro schema for tests", e);
        }
    }

    // -----------------------------------------------------------------------
    // Property: All required headers present for any published event
    // -----------------------------------------------------------------------

    @Property(tries = 100)
    @SuppressWarnings("unchecked")
    void allRequiredHeadersShouldBePresentForAnyPublishedEvent(
            @ForAll("orgNames") String org,
            @ForAll("salesforceTopics") String salesforceTopic,
            @ForAll("eventTypes") String eventType,
            @ForAll("replayIds") byte[] replayIdBytes,
            @ForAll("timestamps") Instant receivedAt) throws Exception {

        // Use a capturing list instead of mocking KafkaTemplate
        List<ProducerRecord<String, GenericRecord>> capturedRecords = new ArrayList<>();
        KafkaTemplate<String, GenericRecord> kafkaTemplate = createCapturingKafkaTemplate(capturedRecords);
        TopicRouter topicRouter = mock(TopicRouter.class);
        ReplayStore replayStore = mock(ReplayStore.class);

        String kafkaTopic = "salesforce." + org + ".events";
        when(topicRouter.getKafkaTopic(org, salesforceTopic)).thenReturn(Optional.of(kafkaTopic));

        DefaultKafkaEventPublisher publisher = new DefaultKafkaEventPublisher(
                kafkaTemplate, topicRouter, replayStore, avroRecordConverter, "1.0.0", 30);

        // Create event batch
        ReplayId replayId = new ReplayId(replayIdBytes);
        JsonNode payload = createPayload(eventType);
        PlatformEvent event = new PlatformEvent(eventType, payload, replayId);
        EventBatch batch = new EventBatch(org, salesforceTopic, List.of(event), replayId, receivedAt);

        // Publish
        PublishResult result = publisher.publishBatch(batch);

        // Verify all required headers are present
        assertEquals(1, capturedRecords.size(), "Should publish exactly one record");
        Headers headers = capturedRecords.get(0).headers();

        assertHeaderPresent(headers, HEADER_REPLAY_ID, "replay-id header must be present");
        assertHeaderPresent(headers, HEADER_ORG, "org header must be present");
        assertHeaderPresent(headers, HEADER_EVENT_TYPE, "event-type header must be present");
        assertHeaderPresent(headers, HEADER_RECEIVED_AT, "received-at header must be present");
        assertHeaderPresent(headers, HEADER_BRIDGE_VERSION, "bridge-version header must be present");

        assertTrue(result.isSuccess(), "Publish should succeed");
    }

    // -----------------------------------------------------------------------
    // Property: Header values match event data
    // -----------------------------------------------------------------------

    @Property(tries = 100)
    @SuppressWarnings("unchecked")
    void headerValuesShouldMatchEventData(
            @ForAll("orgNames") String org,
            @ForAll("salesforceTopics") String salesforceTopic,
            @ForAll("eventTypes") String eventType,
            @ForAll("replayIds") byte[] replayIdBytes,
            @ForAll("timestamps") Instant receivedAt) throws Exception {

        List<ProducerRecord<String, GenericRecord>> capturedRecords = new ArrayList<>();
        KafkaTemplate<String, GenericRecord> kafkaTemplate = createCapturingKafkaTemplate(capturedRecords);
        TopicRouter topicRouter = mock(TopicRouter.class);
        ReplayStore replayStore = mock(ReplayStore.class);

        String kafkaTopic = "salesforce." + org + ".events";
        when(topicRouter.getKafkaTopic(org, salesforceTopic)).thenReturn(Optional.of(kafkaTopic));

        DefaultKafkaEventPublisher publisher = new DefaultKafkaEventPublisher(
                kafkaTemplate, topicRouter, replayStore, avroRecordConverter, "1.0.0", 30);

        // Create event batch
        ReplayId replayId = new ReplayId(replayIdBytes);
        JsonNode payload = createPayload(eventType);
        PlatformEvent event = new PlatformEvent(eventType, payload, replayId);
        EventBatch batch = new EventBatch(org, salesforceTopic, List.of(event), replayId, receivedAt);

        // Publish
        publisher.publishBatch(batch);

        Headers headers = capturedRecords.get(0).headers();

        // Verify header values match event data
        assertArrayEquals(replayIdBytes, getHeaderValue(headers, HEADER_REPLAY_ID),
                "replay-id header value should match event replay ID");
        assertEquals(org, getHeaderString(headers, HEADER_ORG),
                "org header value should match batch org");
        assertEquals(eventType, getHeaderString(headers, HEADER_EVENT_TYPE),
                "event-type header value should match event type");
        assertEquals(receivedAt.toString(), getHeaderString(headers, HEADER_RECEIVED_AT),
                "received-at header value should match batch receivedAt timestamp");
    }

    // -----------------------------------------------------------------------
    // Property: Headers present for all events in a multi-event batch
    // -----------------------------------------------------------------------

    @Property(tries = 50)
    @SuppressWarnings("unchecked")
    void headersShouldBePresentForAllEventsInBatch(
            @ForAll("orgNames") String org,
            @ForAll("salesforceTopics") String salesforceTopic,
            @ForAll @IntRange(min = 1, max = 10) int eventCount,
            @ForAll("timestamps") Instant receivedAt) throws Exception {

        List<ProducerRecord<String, GenericRecord>> capturedRecords = new ArrayList<>();
        KafkaTemplate<String, GenericRecord> kafkaTemplate = createCapturingKafkaTemplate(capturedRecords);
        TopicRouter topicRouter = mock(TopicRouter.class);
        ReplayStore replayStore = mock(ReplayStore.class);

        String kafkaTopic = "salesforce." + org + ".events";
        when(topicRouter.getKafkaTopic(org, salesforceTopic)).thenReturn(Optional.of(kafkaTopic));

        DefaultKafkaEventPublisher publisher = new DefaultKafkaEventPublisher(
                kafkaTemplate, topicRouter, replayStore, avroRecordConverter, "1.0.0", 30);

        // Create batch with multiple events
        List<PlatformEvent> events = new ArrayList<>();
        ReplayId lastReplayId = null;
        for (int i = 0; i < eventCount; i++) {
            byte[] replayIdBytes = ("replay-" + i).getBytes(StandardCharsets.UTF_8);
            lastReplayId = new ReplayId(replayIdBytes);
            String evtType = "Event_Type_" + i + "__e";
            JsonNode payload = createPayload(evtType);
            events.add(new PlatformEvent(evtType, payload, lastReplayId));
        }

        EventBatch batch = new EventBatch(org, salesforceTopic, events, lastReplayId, receivedAt);

        // Publish
        PublishResult result = publisher.publishBatch(batch);

        assertEquals(eventCount, capturedRecords.size(), "Should publish all events");

        // Verify headers for each event
        for (int i = 0; i < eventCount; i++) {
            Headers headers = capturedRecords.get(i).headers();

            assertHeaderPresent(headers, HEADER_REPLAY_ID,
                    "replay-id header must be present for event " + i);
            assertHeaderPresent(headers, HEADER_ORG,
                    "org header must be present for event " + i);
            assertHeaderPresent(headers, HEADER_EVENT_TYPE,
                    "event-type header must be present for event " + i);
            assertHeaderPresent(headers, HEADER_RECEIVED_AT,
                    "received-at header must be present for event " + i);

            // Verify org is consistent across all events
            assertEquals(org, getHeaderString(headers, HEADER_ORG),
                    "org header should be consistent for event " + i);
        }

        assertTrue(result.isSuccess(), "Publish should succeed");
        assertEquals(eventCount, result.getPublishedCount());
    }

    // -----------------------------------------------------------------------
    // Property: Org header matches originating org for multi-org scenarios
    // -----------------------------------------------------------------------

    @Property(tries = 50)
    @SuppressWarnings("unchecked")
    void orgHeaderShouldMatchOriginatingOrg(
            @ForAll("orgNames") String org1,
            @ForAll("orgNames") String org2,
            @ForAll("salesforceTopics") String salesforceTopic,
            @ForAll("eventTypes") String eventType,
            @ForAll("replayIds") byte[] replayIdBytes,
            @ForAll("timestamps") Instant receivedAt) throws Exception {

        Assume.that(!org1.equals(org2));

        List<ProducerRecord<String, GenericRecord>> capturedRecords = new ArrayList<>();
        KafkaTemplate<String, GenericRecord> kafkaTemplate = createCapturingKafkaTemplate(capturedRecords);
        TopicRouter topicRouter = mock(TopicRouter.class);
        ReplayStore replayStore = mock(ReplayStore.class);

        when(topicRouter.getKafkaTopic(eq(org1), eq(salesforceTopic)))
                .thenReturn(Optional.of("salesforce." + org1 + ".events"));
        when(topicRouter.getKafkaTopic(eq(org2), eq(salesforceTopic)))
                .thenReturn(Optional.of("salesforce." + org2 + ".events"));

        DefaultKafkaEventPublisher publisher = new DefaultKafkaEventPublisher(
                kafkaTemplate, topicRouter, replayStore, avroRecordConverter, "1.0.0", 30);

        ReplayId replayId = new ReplayId(replayIdBytes);
        JsonNode payload = createPayload(eventType);
        PlatformEvent event = new PlatformEvent(eventType, payload, replayId);

        EventBatch batch1 = new EventBatch(org1, salesforceTopic, List.of(event), replayId, receivedAt);
        EventBatch batch2 = new EventBatch(org2, salesforceTopic, List.of(event), replayId, receivedAt);

        // Publish both batches
        publisher.publishBatch(batch1);
        publisher.publishBatch(batch2);

        assertEquals(2, capturedRecords.size());

        // Verify org headers match their respective batches
        assertEquals(org1, getHeaderString(capturedRecords.get(0).headers(), HEADER_ORG),
                "First event org header should match org1");
        assertEquals(org2, getHeaderString(capturedRecords.get(1).headers(), HEADER_ORG),
                "Second event org header should match org2");
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

    /**
     * Creates a KafkaTemplate subclass that captures ProducerRecords instead of
     * actually sending them. This avoids Mockito issues with mocking KafkaTemplate
     * on newer JVMs.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private KafkaTemplate<String, GenericRecord> createCapturingKafkaTemplate(
            List<ProducerRecord<String, GenericRecord>> capturedRecords) {
        return new KafkaTemplate<String, GenericRecord>(mock(org.springframework.kafka.core.ProducerFactory.class)) {
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
        payload.put("EventUuid", java.util.UUID.randomUUID().toString());
        payload.put("Type", eventType);
        return payload;
    }

    private void assertHeaderPresent(Headers headers, String headerName, String message) {
        Header header = headers.lastHeader(headerName);
        assertNotNull(header, message);
        assertNotNull(header.value(), message + " (value should not be null)");
        assertTrue(header.value().length > 0, message + " (value should not be empty)");
    }

    private byte[] getHeaderValue(Headers headers, String headerName) {
        Header header = headers.lastHeader(headerName);
        return header != null ? header.value() : null;
    }

    private String getHeaderString(Headers headers, String headerName) {
        byte[] value = getHeaderValue(headers, headerName);
        return value != null ? new String(value, StandardCharsets.UTF_8) : null;
    }
}
