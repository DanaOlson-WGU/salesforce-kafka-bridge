package com.example.bridge.kafka;

import com.example.bridge.config.BridgeConfiguration;
import com.example.bridge.config.SchemaRegistryProperties;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;

import java.lang.reflect.Field;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for Kafka Avro producer configuration in BridgeConfiguration.
 *
 * Validates: Requirements 3.2, 3.4, 6.1, 6.3
 */
class KafkaAvroProducerConfigTest {

    private BridgeConfiguration createConfig() throws Exception {
        BridgeConfiguration config = new BridgeConfiguration();
        Field bootstrapField = BridgeConfiguration.class.getDeclaredField("bootstrapServers");
        bootstrapField.setAccessible(true);
        bootstrapField.set(config, "localhost:9092");
        return config;
    }

    private SchemaRegistryProperties createProperties(String url, String authSource, String userInfo) {
        SchemaRegistryProperties props = new SchemaRegistryProperties();
        props.setUrl(url);
        props.setAuthSource(authSource);
        props.setUserInfo(userInfo);
        return props;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getProducerConfig(SchemaRegistryProperties schemaProps) throws Exception {
        BridgeConfiguration config = createConfig();
        ProducerFactory<String, GenericRecord> factory = config.producerFactory(schemaProps);
        return ((DefaultKafkaProducerFactory<String, GenericRecord>) factory).getConfigurationProperties();
    }

    @Test
    void valueSerializerIsKafkaAvroSerializer() throws Exception {
        Map<String, Object> props = getProducerConfig(
                createProperties("http://localhost:8081", null, null));

        assertEquals(KafkaAvroSerializer.class,
                props.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
    }

    @Test
    void keySerializerIsStringSerializer() throws Exception {
        Map<String, Object> props = getProducerConfig(
                createProperties("http://localhost:8081", null, null));

        assertEquals(StringSerializer.class,
                props.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
    }

    @Test
    void schemaRegistryUrlIsConfigured() throws Exception {
        String expectedUrl = "http://my-registry:8081";
        Map<String, Object> props = getProducerConfig(
                createProperties(expectedUrl, null, null));

        assertEquals(expectedUrl, props.get("schema.registry.url"));
    }

    @Test
    void basicAuthPropertiesSetWhenAuthSourceConfigured() throws Exception {
        String url = "http://registry:8081";
        String authSource = "USER_INFO";
        String userInfo = "apiKey:apiSecret";

        Map<String, Object> props = getProducerConfig(
                createProperties(url, authSource, userInfo));

        assertEquals(authSource, props.get("basic.auth.credentials.source"));
        assertEquals(userInfo, props.get("basic.auth.user.info"));
    }

    @Test
    void basicAuthPropertiesNotSetWhenAuthSourceNull() throws Exception {
        Map<String, Object> props = getProducerConfig(
                createProperties("http://localhost:8081", null, null));

        assertNull(props.get("basic.auth.credentials.source"));
        assertNull(props.get("basic.auth.user.info"));
    }

    @Test
    void basicAuthPropertiesNotSetWhenAuthSourceEmpty() throws Exception {
        Map<String, Object> props = getProducerConfig(
                createProperties("http://localhost:8081", "", null));

        assertNull(props.get("basic.auth.credentials.source"));
        assertNull(props.get("basic.auth.user.info"));
    }

    @Test
    void acksAllAndIdempotenceEnabled() throws Exception {
        Map<String, Object> props = getProducerConfig(
                createProperties("http://localhost:8081", null, null));

        assertEquals("all", props.get(ProducerConfig.ACKS_CONFIG));
        assertEquals(true, props.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
    }
}
