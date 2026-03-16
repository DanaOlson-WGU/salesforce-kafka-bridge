package com.example.bridge.docker;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for Docker configuration files.
 * Validates Dockerfile and docker-compose.yml meet requirements.
 *
 * Requirements: 8.1, 8.3, 8.4
 */
class DockerConfigurationTest {

    private static String dockerfileContent;
    private static String dockerComposeContent;

    @BeforeAll
    static void loadFiles() throws IOException {
        Path dockerfilePath = Path.of("Dockerfile");
        Path composePath = Path.of("docker-compose.yml");

        dockerfileContent = Files.exists(dockerfilePath)
                ? Files.readString(dockerfilePath) : "";
        dockerComposeContent = Files.exists(composePath)
                ? Files.readString(composePath) : "";
    }

    // --- Dockerfile tests ---

    @Test
    void dockerfileShouldExist() {
        assertFalse(dockerfileContent.isEmpty(), "Dockerfile should exist and not be empty");
    }

    @Test
    void dockerfileShouldUseMultiStageBuild() {
        // Multi-stage build has at least two FROM instructions
        long fromCount = dockerfileContent.lines()
                .filter(line -> line.trim().toUpperCase().startsWith("FROM "))
                .count();
        assertTrue(fromCount >= 2,
                "Dockerfile should use multi-stage build (expected >= 2 FROM instructions, found " + fromCount + ")");
    }

    @Test
    void dockerfileShouldUseMavenBuildStage() {
        assertTrue(dockerfileContent.contains("maven:3.9-eclipse-temurin-17"),
                "Dockerfile build stage should use maven:3.9-eclipse-temurin-17");
    }

    @Test
    void dockerfileShouldUseJreAlpineRuntimeStage() {
        assertTrue(dockerfileContent.contains("eclipse-temurin:17-jre-alpine"),
                "Dockerfile runtime stage should use eclipse-temurin:17-jre-alpine");
    }

    @Test
    void dockerfileShouldRunAsNonRootUser() {
        assertTrue(dockerfileContent.contains("USER bridge"),
                "Dockerfile should switch to non-root user 'bridge'");
        assertTrue(dockerfileContent.contains("adduser") || dockerfileContent.contains("useradd"),
                "Dockerfile should create the bridge user");
        assertTrue(dockerfileContent.contains("addgroup") || dockerfileContent.contains("groupadd"),
                "Dockerfile should create the bridge group");
    }

    @Test
    void dockerfileShouldIncludeHealthcheck() {
        assertTrue(dockerfileContent.contains("HEALTHCHECK"),
                "Dockerfile should include a HEALTHCHECK instruction");
        assertTrue(dockerfileContent.contains("/actuator/health"),
                "HEALTHCHECK should invoke /actuator/health endpoint");
    }

    @Test
    void dockerfileShouldExposePort8080() {
        assertTrue(dockerfileContent.contains("EXPOSE 8080"),
                "Dockerfile should expose port 8080");
    }

    @Test
    void dockerfileShouldHaveEntrypoint() {
        assertTrue(dockerfileContent.contains("ENTRYPOINT"),
                "Dockerfile should define an ENTRYPOINT");
    }

    @Test
    void dockerfileShouldChangeOwnership() {
        assertTrue(dockerfileContent.contains("chown") && dockerfileContent.contains("bridge:bridge"),
                "Dockerfile should change ownership to bridge:bridge");
    }

    // --- docker-compose.yml tests ---

    @Test
    void dockerComposeShouldExist() {
        assertFalse(dockerComposeContent.isEmpty(),
                "docker-compose.yml should exist and not be empty");
    }

    @Test
    void dockerComposeShouldDefinePostgresService() {
        assertTrue(dockerComposeContent.contains("postgres:"),
                "docker-compose.yml should define a postgres service");
        assertTrue(dockerComposeContent.contains("bridge_dev"),
                "PostgreSQL should be configured with bridge_dev database");
    }

    @Test
    void dockerComposeShouldDefineZookeeperService() {
        assertTrue(dockerComposeContent.contains("zookeeper:"),
                "docker-compose.yml should define a zookeeper service");
    }

    @Test
    void dockerComposeShouldDefineKafkaService() {
        assertTrue(dockerComposeContent.contains("kafka:"),
                "docker-compose.yml should define a kafka service");
    }

    @Test
    void dockerComposeShouldDefineBridgeService() {
        assertTrue(dockerComposeContent.contains("bridge:"),
                "docker-compose.yml should define a bridge service");
    }

    @Test
    void dockerComposeShouldHaveHealthChecks() {
        // Count healthcheck occurrences — should have at least one per service
        long healthcheckCount = dockerComposeContent.lines()
                .filter(line -> line.trim().startsWith("healthcheck:"))
                .count();
        assertTrue(healthcheckCount >= 4,
                "docker-compose.yml should have health checks for all services (found " + healthcheckCount + ")");
    }

    @Test
    void dockerComposeShouldHavePostgresVolume() {
        assertTrue(dockerComposeContent.contains("postgres_data"),
                "docker-compose.yml should define a volume for PostgreSQL data persistence");
    }

    @Test
    void dockerComposeShouldConfigureBridgeEnvironmentVariables() {
        assertTrue(dockerComposeContent.contains("DB_URL"),
                "Bridge service should have DB_URL environment variable");
        assertTrue(dockerComposeContent.contains("KAFKA_BOOTSTRAP_SERVERS"),
                "Bridge service should have KAFKA_BOOTSTRAP_SERVERS environment variable");
        assertTrue(dockerComposeContent.contains("SF_SRM_CLIENT_ID"),
                "Bridge service should have Salesforce SRM credential variables");
        assertTrue(dockerComposeContent.contains("SF_ACAD_CLIENT_ID"),
                "Bridge service should have Salesforce ACAD credential variables");
    }
}
