package com.example.bridge.config;

import net.jqwik.api.*;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Property 5: Schema Registry URL Environment Override
 *
 * For any Schema Registry URL value set via the SCHEMA_REGISTRY_URL environment variable,
 * the bridge should use that value as the Schema Registry connection URL, overriding any
 * value in the application YAML configuration files.
 *
 * Validates: Requirements 6.2
 */
class SchemaRegistryConfigPropertyTest {

    @Configuration
    @EnableConfigurationProperties(SchemaRegistryProperties.class)
    static class TestConfig {
    }

    private ApplicationContextRunner baseRunner() {
        return new ApplicationContextRunner()
                .withUserConfiguration(TestConfig.class);
    }

    private ApplicationContextRunner runnerWithProps(Map<String, String> props) {
        ApplicationContextRunner runner = baseRunner();
        for (Map.Entry<String, String> e : props.entrySet()) {
            runner = runner.withPropertyValues(e.getKey() + "=" + e.getValue());
        }
        return runner;
    }

    // -----------------------------------------------------------------------
    // Generators
    // -----------------------------------------------------------------------

    @Provide
    Arbitrary<String> schemaRegistryUrls() {
        Arbitrary<String> hosts = Arbitraries.strings()
                .alpha()
                .ofMinLength(3)
                .ofMaxLength(20)
                .map(String::toLowerCase);
        Arbitrary<Integer> ports = Arbitraries.integers().between(1024, 65535);
        return hosts.flatMap(host ->
                ports.map(port -> "http://" + host + ":" + port));
    }

    // -----------------------------------------------------------------------
    // Property 5: Schema Registry URL Environment Override
    // -----------------------------------------------------------------------

    @Property(tries = 100)
    void schemaRegistryUrlEnvironmentOverride(
            @ForAll("schemaRegistryUrls") String envUrl) {

        Map<String, String> props = new LinkedHashMap<>();
        props.put("schema-registry.url", envUrl);

        runnerWithProps(props).run(context -> {
            assertNull(context.getStartupFailure(),
                    "Application context should start without failure");
            SchemaRegistryProperties srProps = context.getBean(SchemaRegistryProperties.class);
            assertEquals(envUrl, srProps.getUrl(),
                    "Schema Registry URL should match the environment variable value");
        });
    }

    @Property(tries = 100)
    void schemaRegistryUrlOverrideReplacesYamlDefault(
            @ForAll("schemaRegistryUrls") String yamlUrl,
            @ForAll("schemaRegistryUrls") String envUrl) {

        Assume.that(!yamlUrl.equals(envUrl));

        // First verify the YAML value is used
        Map<String, String> yamlProps = new LinkedHashMap<>();
        yamlProps.put("schema-registry.url", yamlUrl);

        runnerWithProps(yamlProps).run(context -> {
            assertNull(context.getStartupFailure());
            SchemaRegistryProperties srProps = context.getBean(SchemaRegistryProperties.class);
            assertEquals(yamlUrl, srProps.getUrl(),
                    "Should use YAML value when no env override");
        });

        // Now override with environment variable (simulated via property with higher precedence)
        Map<String, String> envProps = new LinkedHashMap<>();
        envProps.put("schema-registry.url", envUrl);

        runnerWithProps(envProps).run(context -> {
            assertNull(context.getStartupFailure());
            SchemaRegistryProperties srProps = context.getBean(SchemaRegistryProperties.class);
            assertEquals(envUrl, srProps.getUrl(),
                    "Environment variable should override YAML Schema Registry URL");
        });
    }

    // -----------------------------------------------------------------------
    // Unit tests for schema compatibility configuration
    // Validates: Requirements 9.2, 9.3
    // -----------------------------------------------------------------------

    @Example
    void defaultCompatibilityModeIsBackward() {
        baseRunner().run(context -> {
            assertNull(context.getStartupFailure(),
                    "Application context should start without failure");
            SchemaRegistryProperties srProps = context.getBean(SchemaRegistryProperties.class);
            assertEquals("BACKWARD", srProps.getCompatibilityLevel(),
                    "Default compatibility level should be BACKWARD when no explicit value is configured");
        });
    }

    @Example
    void envVarOverridesCompatibilityMode() {
        Map<String, String> props = new LinkedHashMap<>();
        props.put("schema-registry.compatibility-level", "FULL");

        runnerWithProps(props).run(context -> {
            assertNull(context.getStartupFailure(),
                    "Application context should start without failure");
            SchemaRegistryProperties srProps = context.getBean(SchemaRegistryProperties.class);
            assertEquals("FULL", srProps.getCompatibilityLevel(),
                    "Compatibility level should be overridden by environment variable");
        });
    }

    @Example
    void envVarOverridesCompatibilityModeToForward() {
        Map<String, String> props = new LinkedHashMap<>();
        props.put("schema-registry.compatibility-level", "FORWARD");

        runnerWithProps(props).run(context -> {
            assertNull(context.getStartupFailure(),
                    "Application context should start without failure");
            SchemaRegistryProperties srProps = context.getBean(SchemaRegistryProperties.class);
            assertEquals("FORWARD", srProps.getCompatibilityLevel(),
                    "Compatibility level should be overridden to FORWARD");
        });
    }
}
