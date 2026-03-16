package com.example.bridge.resilience;

import com.example.bridge.config.CircuitBreakerProperties;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import net.jqwik.api.*;
import net.jqwik.api.constraints.IntRange;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Property 4: Circuit Breaker Opens After Max Retries
 *
 * For any org, when reconnection attempts exceed the configured maximum
 * retry count, the circuit breaker should transition to the open state
 * and the health endpoint should report the org as unhealthy.
 *
 * Validates: Requirement 1.6
 */
class CircuitBreakerOpensAfterMaxRetriesPropertyTest {

    // -----------------------------------------------------------------------
    // Property: circuit breaker stays CLOSED while failures < threshold
    // -----------------------------------------------------------------------

    @Property(tries = 50)
    void circuitShouldStayClosedBelowThreshold(
            @ForAll("orgNames") String org,
            @ForAll @IntRange(min = 2, max = 10) int threshold) {

        Resilience4jCircuitBreaker cb = createCircuitBreaker(threshold);

        // Record (threshold - 1) failures — should remain CLOSED
        for (int i = 0; i < threshold - 1; i++) {
            cb.recordFailure(org, new RuntimeException("failure " + i));
        }

        assertEquals(CircuitState.CLOSED, cb.getState(org),
                "Circuit should remain CLOSED after " + (threshold - 1)
                        + " failures (threshold=" + threshold + ")");
    }

    // -----------------------------------------------------------------------
    // Property: circuit breaker opens exactly at threshold failures
    // -----------------------------------------------------------------------

    @Property(tries = 50)
    void circuitShouldOpenAtExactlyThresholdFailures(
            @ForAll("orgNames") String org,
            @ForAll @IntRange(min = 2, max = 10) int threshold) {

        Resilience4jCircuitBreaker cb = createCircuitBreaker(threshold);

        for (int i = 0; i < threshold; i++) {
            cb.recordFailure(org, new RuntimeException("failure " + i));
        }

        assertEquals(CircuitState.OPEN, cb.getState(org),
                "Circuit should be OPEN after " + threshold
                        + " failures (threshold=" + threshold + ")");
    }

    // -----------------------------------------------------------------------
    // Property: execute rejects with CircuitOpenException when circuit is open
    // -----------------------------------------------------------------------

    @Property(tries = 50)
    void executeShouldThrowWhenCircuitIsOpen(
            @ForAll("orgNames") String org,
            @ForAll @IntRange(min = 2, max = 10) int threshold) {

        Resilience4jCircuitBreaker cb = createCircuitBreaker(threshold);

        // Force circuit open
        for (int i = 0; i < threshold; i++) {
            cb.recordFailure(org, new RuntimeException("failure " + i));
        }

        assertThrows(CircuitOpenException.class,
                () -> cb.execute(org, () -> "should not run"),
                "execute() should throw CircuitOpenException when circuit is OPEN");
    }

    // -----------------------------------------------------------------------
    // Property: per-org isolation — opening one org does not affect another
    // -----------------------------------------------------------------------

    @Property(tries = 30)
    void circuitOpenForOneOrgShouldNotAffectAnother(
            @ForAll @IntRange(min = 2, max = 10) int threshold) {

        Resilience4jCircuitBreaker cb = createCircuitBreaker(threshold);
        String failingOrg = "failing-org";
        String healthyOrg = "healthy-org";

        // Open circuit for failingOrg
        for (int i = 0; i < threshold; i++) {
            cb.recordFailure(failingOrg, new RuntimeException("failure " + i));
        }

        assertEquals(CircuitState.OPEN, cb.getState(failingOrg));
        assertEquals(CircuitState.CLOSED, cb.getState(healthyOrg),
                "Healthy org circuit should remain CLOSED when another org's circuit opens");
    }

    // -----------------------------------------------------------------------
    // Property: metrics gauge reports non-zero (OPEN) state after threshold
    // -----------------------------------------------------------------------

    @Property(tries = 30)
    void metricsGaugeShouldReportOpenState(
            @ForAll("orgNames") String org,
            @ForAll @IntRange(min = 2, max = 10) int threshold) {

        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        Resilience4jCircuitBreaker cb = createCircuitBreaker(threshold, meterRegistry);

        // Force circuit open
        for (int i = 0; i < threshold; i++) {
            cb.recordFailure(org, new RuntimeException("failure " + i));
        }

        // The gauge bridge.circuit.state{org=<org>} should report 1.0 (OPEN)
        Double gaugeValue = meterRegistry.get("bridge.circuit.state")
                .tag("org", org)
                .gauge()
                .value();

        assertEquals(1.0, gaugeValue,
                "bridge.circuit.state gauge should report 1.0 (OPEN) for org=" + org);
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

    private Resilience4jCircuitBreaker createCircuitBreaker(int threshold, MeterRegistry meterRegistry) {
        CircuitBreakerProperties props = new CircuitBreakerProperties();
        props.setFailureThreshold(threshold);
        props.setCoolDownPeriodSeconds(60);
        props.setHalfOpenMaxAttempts(3);
        return new Resilience4jCircuitBreaker(props, meterRegistry);
    }
}
