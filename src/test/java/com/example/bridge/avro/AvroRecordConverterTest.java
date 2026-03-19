package com.example.bridge.avro;

import com.example.bridge.model.EventBatch;
import com.example.bridge.model.PlatformEvent;
import com.example.bridge.model.ReplayId;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for AvroRecordConverter.
 *
 * Validates: Requirements 3.1, 5.1
 */
class AvroRecordConverterTest {

    private static Schema schema;
    private static AvroRecordConverter converter;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeAll
    static void setUp() throws IOException {
        try (InputStream is = AvroRecordConverterTest.class
                .getResourceAsStream("/avro/SalesforcePlatformEvent.avsc")) {
            schema = new Schema.Parser().parse(is);
        }
        converter = new AvroRecordConverter(schema, objectMapper);
    }

    // --- Helper ---

    private EventBatch buildBatch(String org, PlatformEvent event, Instant receivedAt) {
        return new EventBatch(org, "/event/" + event.getEventType(),
                List.of(event), event.getReplayId(), receivedAt);
    }

    private PlatformEvent buildEvent(String eventType, String json, byte[] replayBytes) throws Exception {
        JsonNode payload = objectMapper.readTree(json);
        return new PlatformEvent(eventType, payload, new ReplayId(replayBytes));
    }

    // -----------------------------------------------------------------------
    // Basic conversion with known values
    // -----------------------------------------------------------------------

    @Test
    void convertsAllFiveFieldsCorrectly() throws Exception {
        byte[] replayBytes = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
        Instant timestamp = Instant.parse("2024-06-15T10:30:00Z");
        String json = "{\"studentId\":\"S12345\",\"status\":\"enrolled\"}";

        PlatformEvent event = buildEvent("Enrollment_Event__e", json, replayBytes);
        EventBatch batch = buildBatch("SRM", event, timestamp);

        GenericRecord record = converter.toGenericRecord(batch, event);

        assertEquals("SRM", record.get("org").toString());
        assertEquals("Enrollment_Event__e", record.get("eventType").toString());
        assertEquals("2024-06-15T10:30:00Z", record.get("timestamp").toString());

        // replayId as ByteBuffer
        ByteBuffer buf = (ByteBuffer) record.get("replayId");
        byte[] extracted = new byte[buf.remaining()];
        buf.get(extracted);
        assertArrayEquals(replayBytes, extracted);

        // payload as JSON string
        JsonNode extractedPayload = objectMapper.readTree(record.get("payload").toString());
        assertEquals("S12345", extractedPayload.get("studentId").asText());
        assertEquals("enrolled", extractedPayload.get("status").asText());
    }

    // -----------------------------------------------------------------------
    // replayId is correctly converted to ByteBuffer
    // -----------------------------------------------------------------------

    @Test
    void replayIdIsConvertedToByteBuffer() throws Exception {
        byte[] replayBytes = {(byte) 0xFF, 0x00, (byte) 0xAB, (byte) 0xCD};
        PlatformEvent event = buildEvent("Test__e", "{}", replayBytes);
        EventBatch batch = buildBatch("ACAD", event, Instant.now());

        GenericRecord record = converter.toGenericRecord(batch, event);

        Object replayIdField = record.get("replayId");
        assertInstanceOf(ByteBuffer.class, replayIdField);

        ByteBuffer buf = (ByteBuffer) replayIdField;
        byte[] extracted = new byte[buf.remaining()];
        buf.get(extracted);
        assertArrayEquals(replayBytes, extracted);
    }

    // -----------------------------------------------------------------------
    // timestamp is ISO-8601 formatted string
    // -----------------------------------------------------------------------

    @Test
    void timestampIsIso8601FormattedString() throws Exception {
        Instant timestamp = Instant.parse("2023-12-25T00:00:00Z");
        PlatformEvent event = buildEvent("Holiday__e", "{\"key\":\"val\"}", new byte[]{1, 2, 3});
        EventBatch batch = buildBatch("SRM", event, timestamp);

        GenericRecord record = converter.toGenericRecord(batch, event);

        String ts = record.get("timestamp").toString();
        // Instant.toString() produces ISO-8601
        assertEquals("2023-12-25T00:00:00Z", ts);
        // Verify it parses back to the same Instant
        assertEquals(timestamp, Instant.parse(ts));
    }

    // -----------------------------------------------------------------------
    // Edge case: empty JSON payload ({})
    // -----------------------------------------------------------------------

    @Test
    void handlesEmptyJsonPayload() throws Exception {
        PlatformEvent event = buildEvent("Empty_Event__e", "{}", new byte[]{1, 2, 3, 4});
        EventBatch batch = buildBatch("SRM", event, Instant.now());

        GenericRecord record = converter.toGenericRecord(batch, event);

        String payload = record.get("payload").toString();
        assertEquals("{}", payload);
    }

    // -----------------------------------------------------------------------
    // Edge case: large payload
    // -----------------------------------------------------------------------

    @Test
    void handlesLargePayload() throws Exception {
        // Build a large JSON object with many fields
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < 500; i++) {
            if (i > 0) sb.append(",");
            sb.append("\"field").append(i).append("\":\"value").append(i).append("\"");
        }
        sb.append("}");
        String largeJson = sb.toString();

        PlatformEvent event = buildEvent("Large_Event__e", largeJson, new byte[]{9, 8, 7});
        EventBatch batch = buildBatch("ACAD", event, Instant.now());

        GenericRecord record = converter.toGenericRecord(batch, event);

        String payload = record.get("payload").toString();
        JsonNode extracted = objectMapper.readTree(payload);
        assertEquals(500, extracted.size());
        assertEquals("value0", extracted.get("field0").asText());
        assertEquals("value499", extracted.get("field499").asText());
    }

    // -----------------------------------------------------------------------
    // Edge case: special characters in event type
    // -----------------------------------------------------------------------

    @Test
    void handlesSpecialCharactersInEventType() throws Exception {
        String eventType = "Special_Event__e";
        PlatformEvent event = buildEvent(eventType, "{\"data\":\"test\"}", new byte[]{1});
        EventBatch batch = buildBatch("SRM", event, Instant.now());

        GenericRecord record = converter.toGenericRecord(batch, event);

        assertEquals(eventType, record.get("eventType").toString());
    }

    @Test
    void handlesEventTypeWithNumbers() throws Exception {
        String eventType = "Event123__e";
        PlatformEvent event = buildEvent(eventType, "{}", new byte[]{1, 2});
        EventBatch batch = buildBatch("ACAD", event, Instant.now());

        GenericRecord record = converter.toGenericRecord(batch, event);

        assertEquals(eventType, record.get("eventType").toString());
    }

    // -----------------------------------------------------------------------
    // Payload with special characters (unicode, escapes)
    // -----------------------------------------------------------------------

    @Test
    void handlesPayloadWithUnicodeCharacters() throws Exception {
        String json = "{\"name\":\"José García\",\"city\":\"München\"}";
        PlatformEvent event = buildEvent("Unicode_Event__e", json, new byte[]{5, 6});
        EventBatch batch = buildBatch("SRM", event, Instant.now());

        GenericRecord record = converter.toGenericRecord(batch, event);

        JsonNode extracted = objectMapper.readTree(record.get("payload").toString());
        assertEquals("José García", extracted.get("name").asText());
        assertEquals("München", extracted.get("city").asText());
    }

    // -----------------------------------------------------------------------
    // Record conforms to schema
    // -----------------------------------------------------------------------

    @Test
    void recordConformsToSalesforcePlatformEventSchema() throws Exception {
        PlatformEvent event = buildEvent("Test__e", "{\"a\":1}", new byte[]{1});
        EventBatch batch = buildBatch("SRM", event, Instant.now());

        GenericRecord record = converter.toGenericRecord(batch, event);

        assertEquals("SalesforcePlatformEvent", record.getSchema().getName());
        assertEquals("com.example.bridge.avro", record.getSchema().getNamespace());
    }
}
