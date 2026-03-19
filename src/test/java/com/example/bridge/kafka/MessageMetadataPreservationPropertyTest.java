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
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Property 4: Message Metadata Preservation
 *
 * For any PlatformEvent published to Kafka using Avro serialization, the Kafka
 * message should include the same headers (replay-id, org, event-type, received-at,
 * bridge-version) and the same composite key format (org:salesforceTopic:eventId)
 * as the current ByteArraySerializer implementation.
 *
 * Validates: Requirements 8.1, 8.2
 */
class MessageMetadataPreservationPropertyTest {

    private static final String HEADER_REPLAY_ID = "replay-id";
    private static final String HEADER_ORG = "org";
    private static final String HEADER_EVENT_TYPE = "event-type";
    private static final String HEADER_RECEIVED_AT = "received-at";
    private static final String HEADER_BRIDGE_VERSION = "bridge-version";

    private static final String BRIDGE_VERSION = "1.0.0-test";

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Schema schema;
    private final AvroRecordConverter avroRecordConverter;

    MessageMetadataPreservationPropertyTest() {
        try {
            this.schema = new Schema.Parser().parse(
                    getClass().getResourceAsStream("/avro/SalesforcePlatformEvent.avsc"));
        } catch (IOException e) {
            throw new RuntimeException("Failed to load Avro schema for tests", e);
        }
        this.avroRecordConverter = new AvroRecordConverter(schema, objectMapper);
    }

    // -----------------------------------------------------------------------
    // Property 4: All 5 headers present with correct values
    // -----------------------------------------------------------------------

    /**
     * Property 4: Message Metadata Preservation
     *
     * For any generated event, all 5 required headers must be present and their
     * values must match the event data. The composite key must follow the format
     * org:salesforceTopic:eventId.
     *
     * Validates: Requirements 8.1, 8.2
     */
    @Property(tries = 100)
    void publishedMessagePreservesAllHeadersAndCompositeKey(
            @ForAll("orgIdentifiers") String org,
            @ForAll("salesforceTopics") String salesforceTopic,
            @ForAll("eventTypes") String eventType,
            @ForAll("replayIdBytes") byte[] replayIdBytes,
            @ForAll("timestamps") Instant receivedAt,
            @ForAll("eventIds") String eventId) {

        // Set up capturing KafkaTemplate
        List<ProducerRecord<String, GenericRecord>> capturedRecords = new ArrayList<>();
        KafkaTemplate<String, GenericRecord> kafkaTemplate = createCapturingKafkaTemplate(capturedRecords);

        // Mock TopicRouter and ReplayStore (interfaces)
        TopicRouter topicRouter = mock(TopicRouter.class);
        String kafkaTopic = "salesforce." + org + ".events";
        when(topicRouter.getKafkaTopic(org, salesforceTopic)).thenReturn(Optional.of(kafkaTopic));

        ReplayStore replayStore = mock(ReplayStore.class);

        DefaultKafkaEventPublisher publisher = new DefaultKafkaEventPublisher(
                kafkaTemplate, topicRouter, replayStore, avroRecordConverter, BRIDGE_VERSION, 30);

        // Build event with a known Id field for deterministic key assertion
        ReplayId replayId = new ReplayId(replayIdBytes);
        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("Id", eventId);
        payload.put("Type", eventType);
        PlatformEvent event = new PlatformEvent(eventType, payload, replayId);
        EventBatch batch = new EventBatch(org, salesforceTopic, List.of(event), replayId, receivedAt);

        // Act
        PublishResult result = publisher.publishBatch(batch);

        // Assert: publish succeeded
        assertTrue(result.isSuccess(), "Publish should succeed");
        assertEquals(1, capturedRecords.size(), "Exactly one record should be captured");

        ProducerRecord<String, GenericRecord> record = capturedRecords.get(0);
        Headers headers = record.headers();

        // Assert: all 5 headers are present
        assertNotNull(headers.lastHeader(HEADER_REPLAY_ID), "replay-id header must be present");
        assertNotNull(headers.lastHeader(HEADER_ORG), "org header must be present");
        assertNotNull(headers.lastHeader(HEADER_EVENT_TYPE), "event-type header must be present");
        assertNotNull(headers.lastHeader(HEADER_RECEIVED_AT), "received-at header must be present");
        assertNotNull(headers.lastHeader(HEADER_BRIDGE_VERSION), "bridge-version header must be present");

        // Assert: header values match event data
        assertArrayEquals(replayIdBytes, headers.lastHeader(HEADER_REPLAY_ID).value(),
                "replay-id header value must match event replayId bytes");
        assertEquals(org, headerToString(headers, HEADER_ORG),
                "org header value must match batch org");
        assertEquals(eventType, headerToString(headers, HEADER_EVENT_TYPE),
                "event-type header value must match event type");
        assertEquals(receivedAt.toString(), headerToString(headers, HEADER_RECEIVED_AT),
                "received-at header value must match batch receivedAt ISO-8601 string");
        assertEquals(BRIDGE_VERSION, headerToString(headers, HEADER_BRIDGE_VERSION),
                "bridge-version header value must match configured bridge version");

        // Assert: composite key format is org:salesforceTopic:eventId
        String expectedKey = org + ":" + salesforceTopic + ":" + eventId;
        assertEquals(expectedKey, record.key(),
                "Composite key must follow format org:salesforceTopic:eventId");
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

    @Provide
    Arbitrary<String> eventIds() {
        return Arbitraries.strings().alpha().numeric().ofMinLength(10).ofMaxLength(18)
                .map(s -> "a" + s);
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

    private String headerToString(Headers headers, String headerName) {
        Header header = headers.lastHeader(headerName);
        return header != null ? new String(header.value(), StandardCharsets.UTF_8) : null;
    }
}
