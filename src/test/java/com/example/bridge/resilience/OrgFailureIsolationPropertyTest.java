package com.example.bridge.resilience;

import com.example.bridge.config.CircuitBreakerProperties;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import net.jqwik.api.*;
import net.jqwik.api.constraints.IntRange;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Property 15: Org Failure Isolation
 *
 * For any org that experiences a failure, all other configured orgs should
 * continue processing events without interruption.
 *
 * Validates: Requirements 5.3
 */
class OrgFailureIsolationPropertyTest {

    // -----------------------------------------------------------------------
    // Property: Failures in one org do not change the state of other orgs
    // -----------------------------------------------------------------------

    @Property(tries = 50)
    void failuresInOneOrgShouldNotAffectOtherOrgs(
            @ForAll("orgNames") String failingOrg,
            @ForAll @IntRange(min = 2, max = 10) int threshold,
            @ForAll @IntRange(min = 1, max = 20) int failureCount) {

        Resilience4jCircuitBreaker cb = createCircuitBreaker(threshold);
        List<String> allOrgs = List.of("srm", "acad", "org-alpha", "org-beta");

        // Record failures only for the failing org
        for (int i = 0; i < failureCount; i++) {
            cb.recordFailure(failingOrg, new RuntimeException("failure " + i));
        }

        // All other orgs must remain CLOSED
        for (String org : allOrgs) {
            if (!org.equals(failingOrg)) {
                assertEquals(CircuitState.CLOSED, cb.getState(org),
                        "Org '" + org + "' should remain CLOSED when '" + failingOrg + "' has failures");
            }
        }
    }

    // -----------------------------------------------------------------------
    // Property: Opening circuit for one org still allows execute on others
    // -----------------------------------------------------------------------

    @Property(tries = 50)
    void openCircuitForOneOrgShouldNotBlockExecuteOnOthers(
            @ForAll("orgNames") String failingOrg,
            @ForAll @IntRange(min = 2, max = 10) int threshold) {

        Resilience4jCircuitBreaker cb = createCircuitBreaker(threshold);
        List<String> allOrgs = List.of("srm", "acad", "org-alpha", "org-beta");

        // Force circuit open for the failing org
        for (int i = 0; i < threshold; i++) {
            cb.recordFailure(failingOrg, new RuntimeException("failure " + i));
        }
        assertEquals(CircuitState.OPEN, cb.getState(failingOrg));

        // All other orgs should still allow execution
        for (String org : allOrgs) {
            if (!org.equals(failingOrg)) {
                String result = assertDoesNotThrow(
                        () -> cb.execute(org, () -> "ok-" + org),
                        "execute() should succeed for org '" + org
                                + "' when '" + failingOrg + "' circuit is OPEN");
                assertEquals("ok-" + org, result);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Property: Multiple orgs can fail independently
    // -----------------------------------------------------------------------

    @Property(tries = 30)
    void multipleOrgsCanFailIndependently(
            @ForAll @IntRange(min = 2, max = 8) int threshold) {

        Resilience4jCircuitBreaker cb = createCircuitBreaker(threshold);

        // Open circuit for "srm" only
        for (int i = 0; i < threshold; i++) {
            cb.recordFailure("srm", new RuntimeException("srm-fail " + i));
        }

        // Record some failures for "acad" but stay below threshold
        for (int i = 0; i < threshold - 1; i++) {
            cb.recordFailure("acad", new RuntimeException("acad-fail " + i));
        }

        assertEquals(CircuitState.OPEN, cb.getState("srm"),
                "srm circuit should be OPEN");
        assertEquals(CircuitState.CLOSED, cb.getState("acad"),
                "acad circuit should remain CLOSED (below threshold)");
        assertEquals(CircuitState.CLOSED, cb.getState("org-alpha"),
                "org-alpha circuit should remain CLOSED (no failures)");
    }

    // -----------------------------------------------------------------------
    // Property: Recovery of one org does not affect other orgs' states
    // -----------------------------------------------------------------------

    @Property(tries = 20)
    void recoveryOfOneOrgShouldNotAffectOthers(
            @ForAll @IntRange(min = 2, max = 5) int threshold) throws InterruptedException {

        Resilience4jCircuitBreaker cb = createCircuitBreaker(threshold, 1, 3);

        // Open circuits for both srm and acad
        for (int i = 0; i < threshold; i++) {
            cb.recordFailure("srm", new RuntimeException("srm-fail " + i));
            cb.recordFailure("acad", new RuntimeException("acad-fail " + i));
        }
        assertEquals(CircuitState.OPEN, cb.getState("srm"));
        assertEquals(CircuitState.OPEN, cb.getState("acad"));

        // Wait for cool-down so both transition to HALF_OPEN
        Thread.sleep(1200);
        assertEquals(CircuitState.HALF_OPEN, cb.getState("srm"));
        assertEquals(CircuitState.HALF_OPEN, cb.getState("acad"));

        // Recover srm only
        for (int i = 0; i < 3; i++) {
            cb.execute("srm", () -> "recovered");
        }

        assertEquals(CircuitState.CLOSED, cb.getState("srm"),
                "srm should be CLOSED after successful recovery");
        assertEquals(CircuitState.HALF_OPEN, cb.getState("acad"),
                "acad should still be HALF_OPEN — srm recovery must not affect it");
    }

    // -----------------------------------------------------------------------
    // Property: Metrics are isolated per org
    // -----------------------------------------------------------------------

    @Property(tries = 30)
    void metricsAreIsolatedPerOrg(
            @ForAll @IntRange(min = 2, max = 10) int threshold) {

        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        Resilience4jCircuitBreaker cb = createCircuitBreaker(threshold, meterRegistry);

        // Open circuit for srm
        for (int i = 0; i < threshold; i++) {
            cb.recordFailure("srm", new RuntimeException("srm-fail " + i));
        }

        // Touch acad so its gauge is registered
        cb.getState("acad");

        double srmGauge = meterRegistry.get("bridge.circuit.state")
                .tag("org", "srm").gauge().value();
        double acadGauge = meterRegistry.get("bridge.circuit.state")
                .tag("org", "acad").gauge().value();

        assertEquals(1.0, srmGauge,
                "srm gauge should report 1.0 (OPEN)");
        assertEquals(0.0, acadGauge,
                "acad gauge should report 0.0 (CLOSED)");
    }

    // -----------------------------------------------------------------------
    // Providers & helpers
    // -----------------------------------------------------------------------

    @Provide
    Arbitrary<String> orgNames() {
        return Arbitraries.of("srm", "acad", "org-alpha", "org-beta");
    }

    private Resilience4jCircuitBreaker createCircuitBreaker(int threshold) {
        return createCircuitBreaker(threshold, new SimpleMeterRegistry());
    }

    private Resilience4jCircuitBreaker createCircuitBreaker(int threshold, SimpleMeterRegistry meterRegistry) {
        return createCircuitBreaker(threshold, 60, 3, meterRegistry);
    }

    private Resilience4jCircuitBreaker createCircuitBreaker(int threshold, int coolDownSeconds, int halfOpenMaxAttempts) {
        return createCircuitBreaker(threshold, coolDownSeconds, halfOpenMaxAttempts, new SimpleMeterRegistry());
    }

    private Resilience4jCircuitBreaker createCircuitBreaker(int threshold, int coolDownSeconds,
                                                             int halfOpenMaxAttempts, SimpleMeterRegistry meterRegistry) {
        CircuitBreakerProperties props = new CircuitBreakerProperties();
        props.setFailureThreshold(threshold);
        props.setCoolDownPeriodSeconds(coolDownSeconds);
        props.setHalfOpenMaxAttempts(halfOpenMaxAttempts);
        return new Resilience4jCircuitBreaker(props, meterRegistry);
    }
}
