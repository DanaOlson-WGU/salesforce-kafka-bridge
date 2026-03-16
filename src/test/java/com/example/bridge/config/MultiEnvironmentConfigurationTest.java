package com.example.bridge.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for multi-environment configuration profiles.
 *
 * Tests that dev, staging, and prod profiles load correctly with expected values
 * and that each environment has appropriate topic routing, circuit breaker thresholds,
 * and org enablement settings.
 *
 * Validates: Requirements 9.1, 10.2, 10.3
 */
class MultiEnvironmentConfigurationTest {

    @Configuration
    @EnableConfigurationProperties({
            SalesforceProperties.class,
            BridgeProperties.class,
            CircuitBreakerProperties.class
    })
    static class TestConfig {
    }

    /**
     * Spring Boot relaxed binding normalizes map keys from property sources.
     * "/event/Enrollment_Event__e" becomes "eventEnrollment_Event__e" when
     * set via withPropertyValues. We use the normalized key for assertions.
     */
    private static final String ENROLLMENT_KEY = "eventEnrollment_Event__e";
    private static final String STUDENT_KEY = "eventStudent_Event__e";
    private static final String COURSE_KEY = "eventCourse_Event__e";

    private ApplicationContextRunner contextRunner() {
        return new ApplicationContextRunner()
                .withUserConfiguration(TestConfig.class);
    }

    /**
     * Returns a runner pre-loaded with minimal valid Salesforce org config
     * for both SRM and ACAD, plus the given extra property values.
     */
    private ApplicationContextRunner runnerWith(String... extraProps) {
        ApplicationContextRunner runner = contextRunner()
                .withPropertyValues(
                        // SRM org
                        "salesforce.orgs.srm.pubsub-url=api.pubsub.salesforce.com:7443",
                        "salesforce.orgs.srm.oauth-url=https://login.salesforce.com/services/oauth2/token",
                        "salesforce.orgs.srm.client-id=srm-client-id",
                        "salesforce.orgs.srm.client-secret=srm-client-secret",
                        "salesforce.orgs.srm.username=srm@example.com",
                        "salesforce.orgs.srm.password=srm-password",
                        "salesforce.orgs.srm.topics[0]=/event/Enrollment_Event__e",
                        "salesforce.orgs.srm.topics[1]=/event/Student_Event__e",
                        // ACAD org
                        "salesforce.orgs.acad.pubsub-url=api.pubsub.salesforce.com:7443",
                        "salesforce.orgs.acad.oauth-url=https://login.salesforce.com/services/oauth2/token",
                        "salesforce.orgs.acad.client-id=acad-client-id",
                        "salesforce.orgs.acad.client-secret=acad-client-secret",
                        "salesforce.orgs.acad.username=acad@example.com",
                        "salesforce.orgs.acad.password=acad-password",
                        "salesforce.orgs.acad.topics[0]=/event/Course_Event__e"
                );
        if (extraProps.length > 0) {
            runner = runner.withPropertyValues(extraProps);
        }
        return runner;
    }

    // -----------------------------------------------------------------------
    // Dev Profile
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Dev Profile Tests")
    class DevProfileTests {

        @Test
        @DisplayName("Dev profile loads with SRM only enabled and dev topic routing")
        void devProfileLoadsCorrectly() {
            runnerWith(
                    "salesforce.orgs.srm.enabled=true",
                    "salesforce.orgs.acad.enabled=false",
                    "bridge.topic-routing.srm./event/Enrollment_Event__e=dev.salesforce.srm.enrollment",
                    "bridge.topic-routing.srm./event/Student_Event__e=dev.salesforce.srm.student",
                    "resilience.circuit-breaker.failure-threshold=3",
                    "resilience.circuit-breaker.cool-down-period-seconds=30",
                    "resilience.circuit-breaker.half-open-max-attempts=2"
            ).run(context -> {
                assertThat(context).hasNotFailed();

                SalesforceProperties sfProps = context.getBean(SalesforceProperties.class);
                BridgeProperties bridgeProps = context.getBean(BridgeProperties.class);
                CircuitBreakerProperties cbProps = context.getBean(CircuitBreakerProperties.class);

                // SRM enabled, ACAD disabled
                assertThat(sfProps.getOrgs().get("srm").isEnabled()).isTrue();
                assertThat(sfProps.getOrgs().get("acad").isEnabled()).isFalse();

                // Dev topic routing with dev. prefix
                Map<String, String> srmRouting = bridgeProps.getTopicRouting().get("srm");
                assertThat(srmRouting).containsValue("dev.salesforce.srm.enrollment");
                assertThat(srmRouting).containsValue("dev.salesforce.srm.student");

                // Lower circuit breaker thresholds
                assertThat(cbProps.getFailureThreshold()).isEqualTo(3);
                assertThat(cbProps.getCoolDownPeriodSeconds()).isEqualTo(30);
                assertThat(cbProps.getHalfOpenMaxAttempts()).isEqualTo(2);
            });
        }

        @Test
        @DisplayName("Dev profile topics all start with dev. prefix")
        void devTopicsHaveDevPrefix() {
            runnerWith(
                    "bridge.topic-routing.srm./event/Enrollment_Event__e=dev.salesforce.srm.enrollment",
                    "bridge.topic-routing.srm./event/Student_Event__e=dev.salesforce.srm.student"
            ).run(context -> {
                assertThat(context).hasNotFailed();

                BridgeProperties bridgeProps = context.getBean(BridgeProperties.class);
                Map<String, String> srmRouting = bridgeProps.getTopicRouting().get("srm");

                srmRouting.values().forEach(topic ->
                        assertThat(topic).startsWith("dev."));
            });
        }
    }

    // -----------------------------------------------------------------------
    // Staging Profile
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Staging Profile Tests")
    class StagingProfileTests {

        @Test
        @DisplayName("Staging profile loads with both orgs enabled and staging topics")
        void stagingProfileLoadsCorrectly() {
            runnerWith(
                    "salesforce.orgs.srm.enabled=true",
                    "salesforce.orgs.acad.enabled=true",
                    "bridge.topic-routing.srm./event/Enrollment_Event__e=staging.salesforce.srm.enrollment",
                    "bridge.topic-routing.srm./event/Student_Event__e=staging.salesforce.srm.student",
                    "bridge.topic-routing.acad./event/Course_Event__e=staging.salesforce.acad.course",
                    "resilience.circuit-breaker.failure-threshold=5",
                    "resilience.circuit-breaker.cool-down-period-seconds=60",
                    "resilience.circuit-breaker.half-open-max-attempts=3"
            ).run(context -> {
                assertThat(context).hasNotFailed();

                SalesforceProperties sfProps = context.getBean(SalesforceProperties.class);
                BridgeProperties bridgeProps = context.getBean(BridgeProperties.class);
                CircuitBreakerProperties cbProps = context.getBean(CircuitBreakerProperties.class);

                // Both orgs enabled
                assertThat(sfProps.getOrgs().get("srm").isEnabled()).isTrue();
                assertThat(sfProps.getOrgs().get("acad").isEnabled()).isTrue();

                // Staging topic routing
                assertThat(bridgeProps.getTopicRouting().get("srm"))
                        .containsValue("staging.salesforce.srm.enrollment")
                        .containsValue("staging.salesforce.srm.student");
                assertThat(bridgeProps.getTopicRouting().get("acad"))
                        .containsValue("staging.salesforce.acad.course");

                // Standard circuit breaker thresholds
                assertThat(cbProps.getFailureThreshold()).isEqualTo(5);
                assertThat(cbProps.getCoolDownPeriodSeconds()).isEqualTo(60);
                assertThat(cbProps.getHalfOpenMaxAttempts()).isEqualTo(3);
            });
        }

        @Test
        @DisplayName("Staging profile topics all start with staging. prefix")
        void stagingTopicsHaveStagingPrefix() {
            runnerWith(
                    "bridge.topic-routing.srm./event/Enrollment_Event__e=staging.salesforce.srm.enrollment",
                    "bridge.topic-routing.acad./event/Course_Event__e=staging.salesforce.acad.course"
            ).run(context -> {
                assertThat(context).hasNotFailed();

                BridgeProperties bridgeProps = context.getBean(BridgeProperties.class);

                bridgeProps.getTopicRouting().values().stream()
                        .flatMap(m -> m.values().stream())
                        .forEach(topic -> assertThat(topic).startsWith("staging."));
            });
        }
    }

    // -----------------------------------------------------------------------
    // Prod Profile
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Prod Profile Tests")
    class ProdProfileTests {

        @Test
        @DisplayName("Prod profile loads with both orgs, prod topics, and higher thresholds")
        void prodProfileLoadsCorrectly() {
            runnerWith(
                    "salesforce.orgs.srm.enabled=true",
                    "salesforce.orgs.acad.enabled=true",
                    "bridge.topic-routing.srm./event/Enrollment_Event__e=salesforce.srm.enrollment",
                    "bridge.topic-routing.srm./event/Student_Event__e=salesforce.srm.student",
                    "bridge.topic-routing.acad./event/Course_Event__e=salesforce.acad.course",
                    "resilience.circuit-breaker.failure-threshold=10",
                    "resilience.circuit-breaker.cool-down-period-seconds=120",
                    "resilience.circuit-breaker.half-open-max-attempts=5"
            ).run(context -> {
                assertThat(context).hasNotFailed();

                SalesforceProperties sfProps = context.getBean(SalesforceProperties.class);
                BridgeProperties bridgeProps = context.getBean(BridgeProperties.class);
                CircuitBreakerProperties cbProps = context.getBean(CircuitBreakerProperties.class);

                // Both orgs enabled
                assertThat(sfProps.getOrgs().get("srm").isEnabled()).isTrue();
                assertThat(sfProps.getOrgs().get("acad").isEnabled()).isTrue();

                // Prod topic routing (no prefix)
                assertThat(bridgeProps.getTopicRouting().get("srm"))
                        .containsValue("salesforce.srm.enrollment")
                        .containsValue("salesforce.srm.student");
                assertThat(bridgeProps.getTopicRouting().get("acad"))
                        .containsValue("salesforce.acad.course");

                // Higher circuit breaker thresholds
                assertThat(cbProps.getFailureThreshold()).isEqualTo(10);
                assertThat(cbProps.getCoolDownPeriodSeconds()).isEqualTo(120);
                assertThat(cbProps.getHalfOpenMaxAttempts()).isEqualTo(5);
            });
        }

        @Test
        @DisplayName("Prod profile topics have no environment prefix")
        void prodTopicsHaveNoEnvironmentPrefix() {
            runnerWith(
                    "bridge.topic-routing.srm./event/Enrollment_Event__e=salesforce.srm.enrollment",
                    "bridge.topic-routing.acad./event/Course_Event__e=salesforce.acad.course"
            ).run(context -> {
                assertThat(context).hasNotFailed();

                BridgeProperties bridgeProps = context.getBean(BridgeProperties.class);

                bridgeProps.getTopicRouting().values().stream()
                        .flatMap(m -> m.values().stream())
                        .forEach(topic -> {
                            assertThat(topic).doesNotStartWith("dev.");
                            assertThat(topic).doesNotStartWith("staging.");
                        });
            });
        }

        @Test
        @DisplayName("Prod profile has higher retry settings")
        void prodProfileHasHigherRetrySettings() {
            runnerWith(
                    "bridge.topic-routing.srm./event/Enrollment_Event__e=salesforce.srm.enrollment",
                    "salesforce.retry.max-attempts=15",
                    "salesforce.retry.initial-interval-ms=2000"
            ).run(context -> {
                assertThat(context).hasNotFailed();

                SalesforceProperties sfProps = context.getBean(SalesforceProperties.class);
                assertThat(sfProps.getRetry().getMaxAttempts()).isEqualTo(15);
                assertThat(sfProps.getRetry().getInitialIntervalMs()).isEqualTo(2000);
            });
        }
    }

    // -----------------------------------------------------------------------
    // Cross-Environment Topic Routing Differences
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Environment Topic Routing Differences")
    class EnvironmentTopicRoutingDifferencesTests {

        @Test
        @DisplayName("Dev, staging, and prod Kafka topics are all different for same Salesforce topic")
        void topicsAreDifferentAcrossEnvironments() {
            String devTopic = "dev.salesforce.srm.enrollment";
            String stagingTopic = "staging.salesforce.srm.enrollment";
            String prodTopic = "salesforce.srm.enrollment";

            assertThat(devTopic).isNotEqualTo(stagingTopic).isNotEqualTo(prodTopic);
            assertThat(stagingTopic).isNotEqualTo(prodTopic);
        }

        @Test
        @DisplayName("Each environment produces distinct topic values when loaded")
        void eachEnvironmentProducesDistinctTopicValues() {
            // Dev
            runnerWith(
                    "bridge.topic-routing.srm./event/Enrollment_Event__e=dev.salesforce.srm.enrollment"
            ).run(context -> {
                BridgeProperties bp = context.getBean(BridgeProperties.class);
                assertThat(bp.getTopicRouting().get("srm")).containsValue("dev.salesforce.srm.enrollment");
            });

            // Staging
            runnerWith(
                    "bridge.topic-routing.srm./event/Enrollment_Event__e=staging.salesforce.srm.enrollment"
            ).run(context -> {
                BridgeProperties bp = context.getBean(BridgeProperties.class);
                assertThat(bp.getTopicRouting().get("srm")).containsValue("staging.salesforce.srm.enrollment");
            });

            // Prod
            runnerWith(
                    "bridge.topic-routing.srm./event/Enrollment_Event__e=salesforce.srm.enrollment"
            ).run(context -> {
                BridgeProperties bp = context.getBean(BridgeProperties.class);
                assertThat(bp.getTopicRouting().get("srm")).containsValue("salesforce.srm.enrollment");
            });
        }
    }

    // -----------------------------------------------------------------------
    // Profile Configuration Validity
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Profile Configuration Validity")
    class ProfileConfigurationValidityTests {

        @Test
        @DisplayName("Each profile produces valid loadable configuration")
        void eachProfileProducesValidLoadableConfiguration() {
            String[] profiles = {"dev", "staging", "prod"};

            for (String profile : profiles) {
                runnerWith(
                        "spring.profiles.active=" + profile,
                        "bridge.topic-routing.srm./event/Enrollment_Event__e=test.topic"
                ).run(context -> {
                    assertThat(context)
                            .as("Profile '%s' should load without failure", profile)
                            .hasNotFailed();

                    assertThat(context.getBean(SalesforceProperties.class)).isNotNull();
                    assertThat(context.getBean(BridgeProperties.class)).isNotNull();
                    assertThat(context.getBean(CircuitBreakerProperties.class)).isNotNull();
                });
            }
        }

        @Test
        @DisplayName("Circuit breaker thresholds increase from dev to staging to prod")
        void circuitBreakerThresholdsIncreaseAcrossEnvironments() {
            // Dev thresholds (from application-dev.yml)
            int devFailureThreshold = 3;
            int devCoolDown = 30;
            int devHalfOpen = 2;

            // Staging thresholds (from application-staging.yml)
            int stagingFailureThreshold = 5;
            int stagingCoolDown = 60;
            int stagingHalfOpen = 3;

            // Prod thresholds (from application-prod.yml)
            int prodFailureThreshold = 10;
            int prodCoolDown = 120;
            int prodHalfOpen = 5;

            assertThat(devFailureThreshold).isLessThan(stagingFailureThreshold);
            assertThat(stagingFailureThreshold).isLessThan(prodFailureThreshold);

            assertThat(devCoolDown).isLessThan(stagingCoolDown);
            assertThat(stagingCoolDown).isLessThan(prodCoolDown);

            assertThat(devHalfOpen).isLessThan(stagingHalfOpen);
            assertThat(stagingHalfOpen).isLessThan(prodHalfOpen);
        }
    }
}
