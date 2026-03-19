package com.example.bridge.config;

import org.apache.avro.Schema;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.io.InputStream;

/**
 * Loads the SalesforcePlatformEvent Avro schema from the classpath at application startup
 * and exposes it as a Spring bean.
 */
@Configuration
public class SchemaRegistryLoader {

    /**
     * Loads the SalesforcePlatformEvent Avro schema from classpath.
     * Fails fast with a descriptive error if the schema file is missing or malformed.
     */
    @Bean
    public Schema salesforcePlatformEventSchema() {
        try {
            InputStream schemaStream = getClass().getResourceAsStream(
                "/avro/SalesforcePlatformEvent.avsc");
            if (schemaStream == null) {
                throw new IllegalStateException(
                    "Avro schema file not found: /avro/SalesforcePlatformEvent.avsc");
            }
            return new Schema.Parser().parse(schemaStream);
        } catch (IOException e) {
            throw new IllegalStateException(
                "Failed to parse Avro schema: /avro/SalesforcePlatformEvent.avsc", e);
        }
    }
}
