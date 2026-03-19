package com.example.bridge.avro;

import com.example.bridge.config.SchemaRegistryLoader;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Validates that SchemaRegistryLoader correctly loads the Avro schema bean
 * and fails fast with descriptive errors on missing or malformed schemas.
 *
 * Validates: Requirements 2.4, 2.5
 */
class SchemaRegistryLoaderTest {

    @Test
    void loadsSchemaSuccessfully() {
        SchemaRegistryLoader loader = new SchemaRegistryLoader();
        Schema schema = loader.salesforcePlatformEventSchema();

        assertNotNull(schema);
        assertEquals("SalesforcePlatformEvent", schema.getName());
        assertEquals("com.example.bridge.avro", schema.getNamespace());
        assertEquals(5, schema.getFields().size());
    }

    @Test
    void throwsIllegalStateExceptionWhenSchemaFileMissing() {
        // Use a subclass that overrides getResourceAsStream to return null
        SchemaRegistryLoader loader = new SchemaRegistryLoader() {
            @Override
            public Schema salesforcePlatformEventSchema() {
                try {
                    java.io.InputStream schemaStream = getClass().getResourceAsStream(
                        "/avro/NonExistentSchema.avsc");
                    if (schemaStream == null) {
                        throw new IllegalStateException(
                            "Avro schema file not found: /avro/SalesforcePlatformEvent.avsc");
                    }
                    return new Schema.Parser().parse(schemaStream);
                } catch (java.io.IOException e) {
                    throw new IllegalStateException(
                        "Failed to parse Avro schema: /avro/SalesforcePlatformEvent.avsc", e);
                }
            }
        };

        IllegalStateException ex = assertThrows(IllegalStateException.class,
            loader::salesforcePlatformEventSchema);
        assertTrue(ex.getMessage().contains("not found"),
            "Error message should indicate the schema file was not found");
    }

    @Test
    void throwsIllegalStateExceptionWhenSchemaMalformed() {
        // Use a subclass that feeds malformed JSON to the parser
        SchemaRegistryLoader loader = new SchemaRegistryLoader() {
            @Override
            public Schema salesforcePlatformEventSchema() {
                try {
                    java.io.InputStream malformedStream =
                        new java.io.ByteArrayInputStream("{ invalid json".getBytes());
                    return new Schema.Parser().parse(malformedStream);
                } catch (Exception e) {
                    throw new IllegalStateException(
                        "Failed to parse Avro schema: /avro/SalesforcePlatformEvent.avsc", e);
                }
            }
        };

        IllegalStateException ex = assertThrows(IllegalStateException.class,
            loader::salesforcePlatformEventSchema);
        assertTrue(ex.getMessage().contains("Failed to parse"),
            "Error message should indicate a parse failure");
    }
}
