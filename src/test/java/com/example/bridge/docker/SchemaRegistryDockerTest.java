package com.example.bridge.docker;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for Docker Compose Schema Registry configuration.
 * Parses docker-compose.yml with SnakeYAML and verifies the schema-registry
 * service and bridge service integration.
 *
 * Requirements: 7.1, 7.2, 7.3, 7.4, 7.5
 */
class SchemaRegistryDockerTest {

    private static Map<String, Object> composeConfig;
    private static Map<String, Object> services;

    @SuppressWarnings("unchecked")
    @BeforeAll
    static void loadDockerCompose() throws IOException {
        Path composePath = Path.of("docker-compose.yml");
        assertTrue(Files.exists(composePath), "docker-compose.yml should exist");

        Yaml yaml = new Yaml();
        composeConfig = yaml.load(Files.readString(composePath));
        assertNotNull(composeConfig, "docker-compose.yml should parse successfully");

        services = (Map<String, Object>) composeConfig.get("services");
        assertNotNull(services, "docker-compose.yml should define services");
    }

    // --- schema-registry service tests ---

    @Test
    void schemaRegistryServiceShouldExist() {
        assertTrue(services.containsKey("schema-registry"),
                "docker-compose.yml should define a schema-registry service");
    }

    @SuppressWarnings("unchecked")
    @Test
    void schemaRegistryServiceShouldUseCorrectImage() {
        Map<String, Object> sr = (Map<String, Object>) services.get("schema-registry");
        assertEquals("confluentinc/cp-schema-registry:7.6.0", sr.get("image"),
                "schema-registry should use confluentinc/cp-schema-registry:7.6.0 image");
    }

    @SuppressWarnings("unchecked")
    @Test
    void schemaRegistryServiceShouldExposePort8081() {
        Map<String, Object> sr = (Map<String, Object>) services.get("schema-registry");
        List<String> ports = (List<String>) sr.get("ports");
        assertNotNull(ports, "schema-registry should define ports");
        assertTrue(ports.stream().anyMatch(p -> p.toString().contains("8081")),
                "schema-registry should expose port 8081");
    }

    @SuppressWarnings("unchecked")
    @Test
    void schemaRegistryServiceShouldDependOnKafkaHealthy() {
        Map<String, Object> sr = (Map<String, Object>) services.get("schema-registry");
        Map<String, Object> dependsOn = (Map<String, Object>) sr.get("depends_on");
        assertNotNull(dependsOn, "schema-registry should have depends_on");
        assertTrue(dependsOn.containsKey("kafka"),
                "schema-registry should depend on kafka");

        Map<String, Object> kafkaDep = (Map<String, Object>) dependsOn.get("kafka");
        assertEquals("service_healthy", kafkaDep.get("condition"),
                "schema-registry kafka dependency should have condition: service_healthy");
    }

    @SuppressWarnings("unchecked")
    @Test
    void schemaRegistryServiceShouldConfigureBootstrapServers() {
        Map<String, Object> sr = (Map<String, Object>) services.get("schema-registry");
        Map<String, Object> env = (Map<String, Object>) sr.get("environment");
        assertNotNull(env, "schema-registry should have environment variables");
        assertEquals("kafka:29092", env.get("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS"),
                "schema-registry should connect to kafka:29092");
    }

    @SuppressWarnings("unchecked")
    @Test
    void schemaRegistryServiceShouldHaveHealthcheck() {
        Map<String, Object> sr = (Map<String, Object>) services.get("schema-registry");
        Map<String, Object> healthcheck = (Map<String, Object>) sr.get("healthcheck");
        assertNotNull(healthcheck, "schema-registry should have a healthcheck");

        Object test = healthcheck.get("test");
        assertNotNull(test, "healthcheck should have a test command");
        String testStr = test.toString();
        assertTrue(testStr.contains("curl") && testStr.contains("8081"),
                "healthcheck should use curl against port 8081");
    }

    // --- bridge service depends on schema-registry ---

    @SuppressWarnings("unchecked")
    @Test
    void bridgeServiceShouldDependOnSchemaRegistryHealthy() {
        Map<String, Object> bridge = (Map<String, Object>) services.get("bridge");
        assertNotNull(bridge, "bridge service should exist");

        Map<String, Object> dependsOn = (Map<String, Object>) bridge.get("depends_on");
        assertNotNull(dependsOn, "bridge should have depends_on");
        assertTrue(dependsOn.containsKey("schema-registry"),
                "bridge should depend on schema-registry");

        Map<String, Object> srDep = (Map<String, Object>) dependsOn.get("schema-registry");
        assertEquals("service_healthy", srDep.get("condition"),
                "bridge schema-registry dependency should have condition: service_healthy");
    }

    @SuppressWarnings("unchecked")
    @Test
    void bridgeServiceShouldHaveSchemaRegistryUrlEnvVar() {
        Map<String, Object> bridge = (Map<String, Object>) services.get("bridge");
        Map<String, Object> env = (Map<String, Object>) bridge.get("environment");
        assertNotNull(env, "bridge should have environment variables");
        assertEquals("http://schema-registry:8081", env.get("SCHEMA_REGISTRY_URL"),
                "bridge should have SCHEMA_REGISTRY_URL pointing to schema-registry:8081");
    }
}
