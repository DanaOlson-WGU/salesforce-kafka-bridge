package com.example.bridge.avro;

import com.example.bridge.model.EventBatch;
import com.example.bridge.model.PlatformEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;

/**
 * Converts PlatformEvent and EventBatch metadata into Avro GenericRecord instances
 * conforming to the SalesforcePlatformEvent schema.
 */
@Component
public class AvroRecordConverter {

    private final Schema schema;
    private final ObjectMapper objectMapper;

    public AvroRecordConverter(Schema schema, ObjectMapper objectMapper) {
        this.schema = schema;
        this.objectMapper = objectMapper;
    }

    /**
     * Converts a PlatformEvent and its batch context into an Avro GenericRecord.
     *
     * @param batch the event batch providing org and receivedAt
     * @param event the individual platform event
     * @return GenericRecord conforming to SalesforcePlatformEvent schema
     */
    public GenericRecord toGenericRecord(EventBatch batch, PlatformEvent event) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("org", batch.getOrg());
        record.put("eventType", event.getEventType());
        record.put("replayId", ByteBuffer.wrap(event.getReplayId().getValue()));
        record.put("timestamp", batch.getReceivedAt().toString());
        try {
            record.put("payload", objectMapper.writeValueAsString(event.getPayload()));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(
                "Failed to serialize payload for event type: " + event.getEventType(), e);
        }
        return record;
    }
}
