package com.example.bridge.resilience;

import com.example.bridge.config.CircuitBreakerProperties;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import net.jqwik.api.*;
import net.jqwik.api.constraints.IntRange;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Property 12: Circuit Breaker Recovery
 *
 * For any org with an open circuit breaker, after the configured cool-down
 * period, the bridge should attempt to re-establish the subscription.
 *
 * Validates: Requirements 4.4
 */
class CircuitBreakerRecoveryPropertyTest {

    private static final int COOL_DOWN_SECONDS = 1;

    // -----------------------------------------------------------------------
    // Property: After cool-down, circuit transitions from OPEN to HALF_OPEN
    // -----------------------------------------------------------------------

    @Property(tries = 20)
    void circuitShouldTransitionToHalfOpenAfterCoolDown(
            @ForAll("orgNames") String org,
            @ForAll @IntRange(min = 2, max = 5) int threshold) throws InterruptedException {

        Resilience4jCircuitBreaker cb = createCircuitBreaker(threshold, COOL_DOWN_SECONDS, 3);

        // Force circuit open
        for (int i = 0; i < threshold; i++) {
            cb.recordFailure(org, new RuntimeException("failure " + i));
        }
        assertEquals(CircuitState.OPEN, cb.getState(org),
                "Circuit should be OPEN after threshold failures");

        // Wait for cool-down period + buffer for automatic transition
        Thread.sleep(1200);

        assertEquals(CircuitState.HALF_OPEN, cb.getState(org),
                "Circuit should transition to HALF_OPEN after cool-down period");
    }

    // -----------------------------------------------------------------------
    // Property: In HALF_OPEN state, execute permits calls (recovery attempt)
    // -----------------------------------------------------------------------

    @Property(tries = 20)
    void executeShouldBePermittedInHalfOpenState(
            @ForAll("orgNames") String org,
            @ForAll @IntRange(min = 2, max = 5) int threshold) throws InterruptedException {

        Resilience4jCircuitBreaker cb = createCircuitBreaker(threshold, COOL_DOWN_SECONDS, 3);

        // Force circuit open
        for (int i = 0; i < threshold; i++) {
            cb.recordFailure(org, new RuntimeException("failure " + i));
        }
        assertEquals(CircuitState.OPEN, cb.getState(org));

        // Wait for cool-down
        Thread.sleep(1200);
        assertEquals(CircuitState.HALF_OPEN, cb.getState(org));

        // execute() should NOT throw — the call is permitted as a recovery probe
        String result = assertDoesNotThrow(
                () -> cb.execute(org, () -> "recovery-probe"),
                "execute() should be permitted in HALF_OPEN state");
        assertEquals("recovery-probe", result);
    }

    // -----------------------------------------------------------------------
    // Property: Successful recovery in HALF_OPEN closes circuit
    // -----------------------------------------------------------------------

    @Property(tries = 20)
    void successfulRecoveryInHalfOpenShouldCloseCircuit(
            @ForAll("orgNames") String org,
            @ForAll @IntRange(min = 2, max = 5) int threshold,
            @ForAll @IntRange(min = 1, max = 3) int halfOpenMaxAttempts) throws InterruptedException {

        Resilience4jCircuitBreaker cb = createCircuitBreaker(threshold, COOL_DOWN_SECONDS, halfOpenMaxAttempts);

        // Force circuit open
        for (int i = 0; i < threshold; i++) {
            cb.recordFailure(org, new RuntimeException("failure " + i));
        }
        assertEquals(CircuitState.OPEN, cb.getState(org));

        // Wait for cool-down
        Thread.sleep(1200);
        assertEquals(CircuitState.HALF_OPEN, cb.getState(org));

        // Record enough successful calls to close the circuit
        for (int i = 0; i < halfOpenMaxAttempts; i++) {
            cb.execute(org, () -> "success");
        }

        assertEquals(CircuitState.CLOSED, cb.getState(org),
                "Circuit should transition back to CLOSED after "
                        + halfOpenMaxAttempts + " successful calls in HALF_OPEN");
    }

    // -----------------------------------------------------------------------
    // Property: Failed recovery in HALF_OPEN re-opens circuit
    // -----------------------------------------------------------------------

    @Property(tries = 20)
    void failedRecoveryInHalfOpenShouldReopenCircuit(
            @ForAll("orgNames") String org,
            @ForAll @IntRange(min = 2, max = 5) int threshold,
            @ForAll @IntRange(min = 1, max = 3) int halfOpenMaxAttempts) throws InterruptedException {

        Resilience4jCircuitBreaker cb = createCircuitBreaker(threshold, COOL_DOWN_SECONDS, halfOpenMaxAttempts);

        // Force circuit open
        for (int i = 0; i < threshold; i++) {
            cb.recordFailure(org, new RuntimeException("failure " + i));
        }
        assertEquals(CircuitState.OPEN, cb.getState(org));

        // Wait for cool-down
        Thread.sleep(1200);
        assertEquals(CircuitState.HALF_OPEN, cb.getState(org));

        // Fill all permitted half-open slots with failures to trigger re-open
        for (int i = 0; i < halfOpenMaxAttempts; i++) {
            try {
                cb.execute(org, () -> {
                    throw new RuntimeException("recovery failed");
                });
            } catch (RuntimeException ignored) {
                // Expected — the supplier throws
            }
        }

        assertEquals(CircuitState.OPEN, cb.getState(org),
                "Circuit should transition back to OPEN after all "
                        + halfOpenMaxAttempts + " recovery attempts fail in HALF_OPEN");
    }

    // -----------------------------------------------------------------------
    // Providers & helpers
    // -----------------------------------------------------------------------

    @Provide
    Arbitrary<String> orgNames() {
        return Arbitraries.of("srm", "acad", "org-alpha", "org-beta");
    }

    private Resilience4jCircuitBreaker createCircuitBreaker(int threshold, int coolDownSeconds, int halfOpenMaxAttempts) {
        CircuitBreakerProperties props = new CircuitBreakerProperties();
        props.setFailureThreshold(threshold);
        props.setCoolDownPeriodSeconds(coolDownSeconds);
        props.setHalfOpenMaxAttempts(halfOpenMaxAttempts);
        return new Resilience4jCircuitBreaker(props, new SimpleMeterRegistry());
    }
}
