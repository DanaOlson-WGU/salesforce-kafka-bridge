package com.example.bridge.resilience;

import com.example.bridge.config.CircuitBreakerProperties;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link Resilience4jCircuitBreaker}.
 * <p>
 * Covers:
 * - Circuit transitions: CLOSED → OPEN → HALF_OPEN → CLOSED
 * - Per-org isolation
 * - Cool-down period timing
 * - Metrics emission on state change
 * <p>
 * Requirements: 1.6, 4.3, 4.4, 4.5, 5.3
 */
class CircuitBreakerTest {

    private static final int FAILURE_THRESHOLD = 3;
    private static final int COOL_DOWN_SECONDS = 1; // short for test speed
    private static final int HALF_OPEN_MAX_ATTEMPTS = 2;

    private SimpleMeterRegistry meterRegistry;
    private Resilience4jCircuitBreaker circuitBreaker;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        CircuitBreakerProperties props = new CircuitBreakerProperties();
        props.setFailureThreshold(FAILURE_THRESHOLD);
        props.setCoolDownPeriodSeconds(COOL_DOWN_SECONDS);
        props.setHalfOpenMaxAttempts(HALF_OPEN_MAX_ATTEMPTS);
        circuitBreaker = new Resilience4jCircuitBreaker(props, meterRegistry);
    }

    // -------------------------------------------------------------------
    // Circuit state transitions
    // -------------------------------------------------------------------

    @Nested
    @DisplayName("Circuit state transitions")
    class StateTransitions {

        @Test
        @DisplayName("New circuit starts in CLOSED state")
        void newCircuitStartsClosed() {
            assertEquals(CircuitState.CLOSED, circuitBreaker.getState("srm"));
        }

        @Test
        @DisplayName("Circuit transitions to OPEN after threshold failures")
        void circuitOpensAfterThresholdFailures() {
            for (int i = 0; i < FAILURE_THRESHOLD; i++) {
                circuitBreaker.recordFailure("srm", new RuntimeException("fail " + i));
            }
            assertEquals(CircuitState.OPEN, circuitBreaker.getState("srm"));
        }

        @Test
        @DisplayName("Circuit stays CLOSED below threshold failures")
        void circuitStaysClosedBelowThreshold() {
            for (int i = 0; i < FAILURE_THRESHOLD - 1; i++) {
                circuitBreaker.recordFailure("srm", new RuntimeException("fail " + i));
            }
            assertEquals(CircuitState.CLOSED, circuitBreaker.getState("srm"));
        }

        @Test
        @DisplayName("Circuit transitions OPEN → HALF_OPEN after cool-down")
        void circuitTransitionsToHalfOpenAfterCoolDown() throws InterruptedException {
            // Open the circuit
            for (int i = 0; i < FAILURE_THRESHOLD; i++) {
                circuitBreaker.recordFailure("srm", new RuntimeException("fail " + i));
            }
            assertEquals(CircuitState.OPEN, circuitBreaker.getState("srm"));

            // Wait for cool-down (automaticTransitionFromOpenToHalfOpenEnabled = true)
            Thread.sleep((COOL_DOWN_SECONDS * 1000) + 500);

            assertEquals(CircuitState.HALF_OPEN, circuitBreaker.getState("srm"));
        }

        @Test
        @DisplayName("Circuit transitions HALF_OPEN → CLOSED after successful calls")
        void circuitClosesAfterSuccessInHalfOpen() throws InterruptedException {
            // Open the circuit
            for (int i = 0; i < FAILURE_THRESHOLD; i++) {
                circuitBreaker.recordFailure("srm", new RuntimeException("fail " + i));
            }

            // Wait for half-open
            Thread.sleep((COOL_DOWN_SECONDS * 1000) + 500);
            assertEquals(CircuitState.HALF_OPEN, circuitBreaker.getState("srm"));

            // Successful calls in half-open should close the circuit
            for (int i = 0; i < HALF_OPEN_MAX_ATTEMPTS; i++) {
                circuitBreaker.execute("srm", () -> "ok");
            }

            assertEquals(CircuitState.CLOSED, circuitBreaker.getState("srm"));
        }

        @Test
        @DisplayName("Full lifecycle: CLOSED → OPEN → HALF_OPEN → CLOSED")
        void fullLifecycleTransition() throws InterruptedException {
            // CLOSED
            assertEquals(CircuitState.CLOSED, circuitBreaker.getState("srm"));

            // → OPEN
            for (int i = 0; i < FAILURE_THRESHOLD; i++) {
                circuitBreaker.recordFailure("srm", new RuntimeException("fail " + i));
            }
            assertEquals(CircuitState.OPEN, circuitBreaker.getState("srm"));

            // → HALF_OPEN
            Thread.sleep((COOL_DOWN_SECONDS * 1000) + 500);
            assertEquals(CircuitState.HALF_OPEN, circuitBreaker.getState("srm"));

            // → CLOSED
            for (int i = 0; i < HALF_OPEN_MAX_ATTEMPTS; i++) {
                circuitBreaker.execute("srm", () -> "ok");
            }
            assertEquals(CircuitState.CLOSED, circuitBreaker.getState("srm"));
        }

        @Test
        @DisplayName("execute() throws CircuitOpenException when circuit is OPEN")
        void executeThrowsWhenOpen() {
            for (int i = 0; i < FAILURE_THRESHOLD; i++) {
                circuitBreaker.recordFailure("srm", new RuntimeException("fail " + i));
            }

            CircuitOpenException ex = assertThrows(CircuitOpenException.class,
                    () -> circuitBreaker.execute("srm", () -> "should not run"));
            assertEquals("srm", ex.getOrg());
        }

        @Test
        @DisplayName("execute() runs operation when circuit is CLOSED")
        void executeRunsWhenClosed() {
            String result = circuitBreaker.execute("srm", () -> "hello");
            assertEquals("hello", result);
        }
    }

    // -------------------------------------------------------------------
    // Per-org isolation
    // -------------------------------------------------------------------

    @Nested
    @DisplayName("Per-org isolation")
    class OrgIsolation {

        @Test
        @DisplayName("Failure in one org does not affect another org")
        void failureInOneOrgDoesNotAffectAnother() {
            // Open circuit for srm
            for (int i = 0; i < FAILURE_THRESHOLD; i++) {
                circuitBreaker.recordFailure("srm", new RuntimeException("fail"));
            }

            assertEquals(CircuitState.OPEN, circuitBreaker.getState("srm"));
            assertEquals(CircuitState.CLOSED, circuitBreaker.getState("acad"));
        }

        @Test
        @DisplayName("Each org maintains independent failure counts")
        void independentFailureCounts() {
            // Record (threshold - 1) failures for each org — neither should open
            for (int i = 0; i < FAILURE_THRESHOLD - 1; i++) {
                circuitBreaker.recordFailure("srm", new RuntimeException("fail"));
                circuitBreaker.recordFailure("acad", new RuntimeException("fail"));
            }

            assertEquals(CircuitState.CLOSED, circuitBreaker.getState("srm"));
            assertEquals(CircuitState.CLOSED, circuitBreaker.getState("acad"));

            // One more failure for srm only
            circuitBreaker.recordFailure("srm", new RuntimeException("final fail"));

            assertEquals(CircuitState.OPEN, circuitBreaker.getState("srm"));
            assertEquals(CircuitState.CLOSED, circuitBreaker.getState("acad"));
        }

        @Test
        @DisplayName("Execute succeeds for healthy org while another is open")
        void executeSucceedsForHealthyOrgWhileAnotherIsOpen() {
            for (int i = 0; i < FAILURE_THRESHOLD; i++) {
                circuitBreaker.recordFailure("srm", new RuntimeException("fail"));
            }

            // srm is open, acad should still work
            assertThrows(CircuitOpenException.class,
                    () -> circuitBreaker.execute("srm", () -> "nope"));
            assertEquals("ok", circuitBreaker.execute("acad", () -> "ok"));
        }
    }

    // -------------------------------------------------------------------
    // Cool-down period timing
    // -------------------------------------------------------------------

    @Nested
    @DisplayName("Cool-down period timing")
    class CoolDownTiming {

        @Test
        @DisplayName("Circuit remains OPEN before cool-down expires")
        void circuitRemainsOpenBeforeCoolDown() throws InterruptedException {
            for (int i = 0; i < FAILURE_THRESHOLD; i++) {
                circuitBreaker.recordFailure("srm", new RuntimeException("fail"));
            }
            assertEquals(CircuitState.OPEN, circuitBreaker.getState("srm"));

            // Wait less than cool-down
            Thread.sleep(200);
            assertEquals(CircuitState.OPEN, circuitBreaker.getState("srm"));
        }

        @Test
        @DisplayName("Circuit transitions to HALF_OPEN after cool-down expires")
        void circuitTransitionsAfterCoolDown() throws InterruptedException {
            for (int i = 0; i < FAILURE_THRESHOLD; i++) {
                circuitBreaker.recordFailure("srm", new RuntimeException("fail"));
            }

            Thread.sleep((COOL_DOWN_SECONDS * 1000) + 500);
            assertEquals(CircuitState.HALF_OPEN, circuitBreaker.getState("srm"));
        }

        @Test
        @DisplayName("Circuit re-opens if all permitted half-open calls fail")
        void circuitReOpensOnHalfOpenFailure() throws InterruptedException {
            // Open
            for (int i = 0; i < FAILURE_THRESHOLD; i++) {
                circuitBreaker.recordFailure("srm", new RuntimeException("fail"));
            }

            // Wait for half-open
            Thread.sleep((COOL_DOWN_SECONDS * 1000) + 500);
            assertEquals(CircuitState.HALF_OPEN, circuitBreaker.getState("srm"));

            // Fail all permitted half-open calls to trigger re-open
            for (int i = 0; i < HALF_OPEN_MAX_ATTEMPTS; i++) {
                try {
                    circuitBreaker.execute("srm", () -> { throw new RuntimeException("still broken"); });
                } catch (RuntimeException ignored) {}
            }

            assertEquals(CircuitState.OPEN, circuitBreaker.getState("srm"));
        }
    }

    // -------------------------------------------------------------------
    // Metrics emission
    // -------------------------------------------------------------------

    @Nested
    @DisplayName("Metrics emission on state change")
    class MetricsEmission {

        @Test
        @DisplayName("State gauge reports 0.0 for CLOSED circuit")
        void gaugeReportsClosedState() {
            // Trigger breaker creation by querying state
            circuitBreaker.getState("srm");

            double value = meterRegistry.get("bridge.circuit.state")
                    .tag("org", "srm")
                    .gauge()
                    .value();
            assertEquals(0.0, value);
        }

        @Test
        @DisplayName("State gauge reports 1.0 for OPEN circuit")
        void gaugeReportsOpenState() {
            for (int i = 0; i < FAILURE_THRESHOLD; i++) {
                circuitBreaker.recordFailure("srm", new RuntimeException("fail"));
            }

            double value = meterRegistry.get("bridge.circuit.state")
                    .tag("org", "srm")
                    .gauge()
                    .value();
            assertEquals(1.0, value);
        }

        @Test
        @DisplayName("State gauge reports 2.0 for HALF_OPEN circuit")
        void gaugeReportsHalfOpenState() throws InterruptedException {
            for (int i = 0; i < FAILURE_THRESHOLD; i++) {
                circuitBreaker.recordFailure("srm", new RuntimeException("fail"));
            }

            Thread.sleep((COOL_DOWN_SECONDS * 1000) + 500);

            double value = meterRegistry.get("bridge.circuit.state")
                    .tag("org", "srm")
                    .gauge()
                    .value();
            assertEquals(2.0, value);
        }

        @Test
        @DisplayName("Transition counter increments on state change to OPEN")
        void transitionCounterIncrementsOnOpen() {
            for (int i = 0; i < FAILURE_THRESHOLD; i++) {
                circuitBreaker.recordFailure("srm", new RuntimeException("fail"));
            }

            Counter counter = meterRegistry.find("bridge.circuit.state.transitions")
                    .tag("org", "srm")
                    .tag("to_state", "OPEN")
                    .counter();

            assertNotNull(counter, "Transition counter for OPEN should exist");
            assertTrue(counter.count() >= 1.0,
                    "Transition counter should have been incremented at least once");
        }

        @Test
        @DisplayName("Separate gauge per org")
        void separateGaugePerOrg() {
            // Open srm, leave acad closed
            for (int i = 0; i < FAILURE_THRESHOLD; i++) {
                circuitBreaker.recordFailure("srm", new RuntimeException("fail"));
            }
            circuitBreaker.getState("acad"); // trigger creation

            double srmValue = meterRegistry.get("bridge.circuit.state")
                    .tag("org", "srm")
                    .gauge()
                    .value();
            double acadValue = meterRegistry.get("bridge.circuit.state")
                    .tag("org", "acad")
                    .gauge()
                    .value();

            assertEquals(1.0, srmValue, "srm gauge should report OPEN (1.0)");
            assertEquals(0.0, acadValue, "acad gauge should report CLOSED (0.0)");
        }
    }
}
