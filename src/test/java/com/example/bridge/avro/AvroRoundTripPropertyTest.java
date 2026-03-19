package com.example.bridge.avro;

import com.example.bridge.model.EventBatch;
import com.example.bridge.model.PlatformEvent;
import com.example.bridge.model.ReplayId;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import net.jqwik.api.*;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Property 1: PlatformEvent Avro Round Trip
 *
 * For any valid PlatformEvent with any org identifier, event type, replay ID,
 * timestamp, and JSON payload, converting the event into a GenericRecord via
 * AvroRecordConverter.toGenericRecord() and then extracting the fields back
 * should produce values equivalent to the original event's org, eventType,
 * replayId, timestamp, and payload.
 *
 * Validates: Requirements 5.1, 3.1, 4.3, 4.4, 5.3
 */
class AvroRoundTripPropertyTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Schema schema;
    private final AvroRecordConverter converter;

    AvroRoundTripPropertyTest() throws IOException {
        try (InputStream is = getClass().getResourceAsStream("/avro/SalesforcePlatformEvent.avsc")) {
            this.schema = new Schema.Parser().parse(is);
        }
        this.converter = new AvroRecordConverter(schema, objectMapper);
    }

    /**
     * Property 1: PlatformEvent Avro Round Trip
     *
     * Validates: Requirements 5.1, 3.1, 4.3, 4.4, 5.3
     */
    @Property(tries = 100)
    void platformEventAvroRoundTrip(
            @ForAll("orgIdentifiers") String org,
            @ForAll("eventTypes") String eventType,
            @ForAll("replayIdBytes") byte[] replayIdBytes,
            @ForAll("timestamps") Instant timestamp,
            @ForAll("jsonPayloads") String jsonPayload) throws Exception {

        // Build model objects
        JsonNode payloadNode = objectMapper.readTree(jsonPayload);
        ReplayId replayId = new ReplayId(replayIdBytes);
        PlatformEvent event = new PlatformEvent(eventType, payloadNode, replayId);
        EventBatch batch = new EventBatch(org, "/event/" + eventType, List.of(event),
                replayId, timestamp);

        // Convert to GenericRecord
        GenericRecord record = converter.toGenericRecord(batch, event);

        // Extract fields back from GenericRecord
        String extractedOrg = record.get("org").toString();
        String extractedEventType = record.get("eventType").toString();
        ByteBuffer extractedReplayIdBuffer = (ByteBuffer) record.get("replayId");
        String extractedTimestamp = record.get("timestamp").toString();
        String extractedPayload = record.get("payload").toString();

        // Assert org matches
        assertEquals(org, extractedOrg,
                "org field should round-trip through GenericRecord");

        // Assert eventType matches
        assertEquals(eventType, extractedEventType,
                "eventType field should round-trip through GenericRecord");

        // Assert replayId bytes match
        byte[] extractedReplayBytes = new byte[extractedReplayIdBuffer.remaining()];
        extractedReplayIdBuffer.get(extractedReplayBytes);
        assertArrayEquals(replayIdBytes, extractedReplayBytes,
                "replayId bytes should round-trip through GenericRecord");

        // Assert timestamp matches (ISO-8601 string representation)
        assertEquals(timestamp.toString(), extractedTimestamp,
                "timestamp should round-trip through GenericRecord as ISO-8601 string");

        // Assert payload JSON is equivalent
        JsonNode extractedPayloadNode = objectMapper.readTree(extractedPayload);
        assertEquals(payloadNode, extractedPayloadNode,
                "payload JSON should round-trip through GenericRecord");
    }

    /**
     * Property 2: Avro Binary Serialization Round Trip
     *
     * For any valid GenericRecord conforming to the SalesforcePlatformEvent schema,
     * serializing to Avro binary bytes using KafkaAvroSerializer and deserializing
     * using KafkaAvroDeserializer should produce a GenericRecord with field values
     * equivalent to the original.
     *
     * Validates: Requirements 5.2
     */
    @Property(tries = 100)
    void avroBinarySerializationRoundTrip(
            @ForAll("orgIdentifiers") String org,
            @ForAll("eventTypes") String eventType,
            @ForAll("replayIdBytes") byte[] replayIdBytes,
            @ForAll("timestamps") Instant timestamp,
            @ForAll("jsonPayloads") String jsonPayload) throws Exception {

        // Build a GenericRecord from generated data
        GenericRecord original = new GenericData.Record(schema);
        original.put("org", org);
        original.put("eventType", eventType);
        original.put("replayId", ByteBuffer.wrap(replayIdBytes));
        original.put("timestamp", timestamp.toString());
        original.put("payload", jsonPayload);

        // Configure serializer and deserializer with shared MockSchemaRegistryClient
        MockSchemaRegistryClient mockRegistryClient = new MockSchemaRegistryClient();
        String topic = "test-topic";

        Map<String, Object> serializerConfig = new HashMap<>();
        serializerConfig.put("schema.registry.url", "mock://test");
        serializerConfig.put("auto.register.schemas", true);

        Map<String, Object> deserializerConfig = new HashMap<>();
        deserializerConfig.put("schema.registry.url", "mock://test");
        deserializerConfig.put("specific.avro.reader", false);

        try (KafkaAvroSerializer serializer = new KafkaAvroSerializer(mockRegistryClient);
             KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer(mockRegistryClient)) {

            serializer.configure(serializerConfig, false);
            deserializer.configure(deserializerConfig, false);

            // Serialize
            byte[] bytes = serializer.serialize(topic, original);
            assertNotNull(bytes, "Serialized bytes should not be null");

            // Deserialize
            Object deserialized = deserializer.deserialize(topic, bytes);
            assertNotNull(deserialized, "Deserialized object should not be null");
            assertInstanceOf(GenericRecord.class, deserialized,
                    "Deserialized object should be a GenericRecord");

            GenericRecord result = (GenericRecord) deserialized;

            // Assert all 5 fields match
            assertEquals(org, result.get("org").toString(),
                    "org field should survive binary serialization round trip");
            assertEquals(eventType, result.get("eventType").toString(),
                    "eventType field should survive binary serialization round trip");

            ByteBuffer resultReplayIdBuffer = (ByteBuffer) result.get("replayId");
            byte[] resultReplayBytes = new byte[resultReplayIdBuffer.remaining()];
            resultReplayIdBuffer.get(resultReplayBytes);
            assertArrayEquals(replayIdBytes, resultReplayBytes,
                    "replayId bytes should survive binary serialization round trip");

            assertEquals(timestamp.toString(), result.get("timestamp").toString(),
                    "timestamp should survive binary serialization round trip");
            assertEquals(jsonPayload, result.get("payload").toString(),
                    "payload should survive binary serialization round trip");
        }
    }

    // -----------------------------------------------------------------------
    // Generators
    // -----------------------------------------------------------------------

    @Provide
    Arbitrary<String> orgIdentifiers() {
        return Arbitraries.of("SRM", "ACAD");
    }

    @Provide
    Arbitrary<String> eventTypes() {
        return Arbitraries.of("Enrollment_Event__e", "Student_Event__e", "Course_Event__e");
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
    @SuppressWarnings("unchecked")
    Arbitrary<String> jsonPayloads() {
        return Arbitraries.maps(
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(20),
                Arbitraries.oneOf(
                        Arbitraries.strings().alpha().ofMaxLength(100).map(s -> (Object) s),
                        Arbitraries.integers().map(i -> (Object) i),
                        Arbitraries.of(true, false).map(b -> (Object) b)
                )
        ).ofMinSize(1).ofMaxSize(10)
         .map(map -> {
             try {
                 return objectMapper.writeValueAsString(map);
             } catch (Exception e) {
                 throw new RuntimeException(e);
             }
         });
    }
}
