package com.example.bridge.avro;

import org.apache.avro.Schema;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Validates the structure of the SalesforcePlatformEvent Avro schema (.avsc file).
 *
 * Validates: Requirements 2.1, 2.2
 */
class SchemaStructureTest {

    private static Schema schema;

    @BeforeAll
    static void loadSchema() throws IOException {
        try (InputStream is = SchemaStructureTest.class
                .getResourceAsStream("/avro/SalesforcePlatformEvent.avsc")) {
            assertNotNull(is, "Schema file should exist on classpath");
            schema = new Schema.Parser().parse(is);
        }
    }

    @Test
    void schemaNameIsSalesforcePlatformEvent() {
        assertEquals("SalesforcePlatformEvent", schema.getName());
    }

    @Test
    void namespaceIsComExampleBridgeAvro() {
        assertEquals("com.example.bridge.avro", schema.getNamespace());
    }

    @Test
    void schemaHasExactlyFiveFields() {
        assertEquals(5, schema.getFields().size());
    }

    @Test
    void orgFieldIsString() {
        Schema.Field field = schema.getField("org");
        assertNotNull(field, "org field should exist");
        assertEquals(Schema.Type.STRING, field.schema().getType());
    }

    @Test
    void eventTypeFieldIsString() {
        Schema.Field field = schema.getField("eventType");
        assertNotNull(field, "eventType field should exist");
        assertEquals(Schema.Type.STRING, field.schema().getType());
    }

    @Test
    void replayIdFieldIsBytes() {
        Schema.Field field = schema.getField("replayId");
        assertNotNull(field, "replayId field should exist");
        assertEquals(Schema.Type.BYTES, field.schema().getType());
    }

    @Test
    void timestampFieldIsString() {
        Schema.Field field = schema.getField("timestamp");
        assertNotNull(field, "timestamp field should exist");
        assertEquals(Schema.Type.STRING, field.schema().getType());
    }

    @Test
    void payloadFieldIsString() {
        Schema.Field field = schema.getField("payload");
        assertNotNull(field, "payload field should exist");
        assertEquals(Schema.Type.STRING, field.schema().getType());
    }

    @Test
    void fieldOrderMatchesExpected() {
        List<Schema.Field> fields = schema.getFields();
        assertEquals("org", fields.get(0).name());
        assertEquals("eventType", fields.get(1).name());
        assertEquals("replayId", fields.get(2).name());
        assertEquals("timestamp", fields.get(3).name());
        assertEquals("payload", fields.get(4).name());
    }
}
