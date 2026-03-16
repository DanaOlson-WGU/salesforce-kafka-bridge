package com.example.bridge.resilience;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.example.bridge.config.CircuitBreakerProperties;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import net.jqwik.api.*;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.lifecycle.AfterProperty;
import net.jqwik.api.lifecycle.BeforeProperty;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Property 13: Circuit Breaker Observability
 *
 * For any org where the circuit breaker transitions to the open state,
 * the bridge should log an ERROR message including the org identifier
 * and topic name, and the health endpoint should report that org as unhealthy.
 *
 * Validates: Requirements 4.3, 4.5
 */
class CircuitBreakerObservabilityPropertyTest {

    private ListAppender<ILoggingEvent> logAppender;
    private Logger cbLogger;

    @BeforeProperty
    void setupLogCapture() {
        cbLogger = (Logger) LoggerFactory.getLogger(Resilience4jCircuitBreaker.class);
        logAppender = new ListAppender<>();
        logAppender.start();
        cbLogger.addAppender(logAppender);
    }

    @AfterProperty
    void tearDownLogCapture() {
        cbLogger.detachAppender(logAppender);
        logAppender.stop();
    }

    // -----------------------------------------------------------------------
    // Property: ERROR log emitted containing org when circuit opens
    // -----------------------------------------------------------------------

    @Property(tries = 50)
    void shouldLogErrorWithOrgWhenCircuitOpens(
            @ForAll("orgNames") String org,
            @ForAll @IntRange(min = 2, max = 10) int threshold) {

        logAppender.list.clear();
        Resilience4jCircuitBreaker cb = createCircuitBreaker(threshold);

        // Force circuit open
        for (int i = 0; i < threshold; i++) {
            cb.recordFailure(org, new RuntimeException("failure " + i));
        }

        assertEquals(CircuitState.OPEN, cb.getState(org));

        List<ILoggingEvent> errorLogs = logAppender.list.stream()
                .filter(e -> e.getLevel() == Level.ERROR)
                .filter(e -> e.getFormattedMessage().contains(org))
                .toList();

        assertFalse(errorLogs.isEmpty(),
                "Expected at least one ERROR log containing org='" + org
                        + "' when circuit opens");
    }

    // -----------------------------------------------------------------------
    // Property: No ERROR log emitted while circuit stays closed
    // -----------------------------------------------------------------------

    @Property(tries = 50)
    void shouldNotLogErrorWhileCircuitRemainsClosed(
            @ForAll("orgNames") String org,
            @ForAll @IntRange(min = 3, max = 10) int threshold) {

        logAppender.list.clear();
        Resilience4jCircuitBreaker cb = createCircuitBreaker(threshold);

        // Record failures below threshold
        for (int i = 0; i < threshold - 1; i++) {
            cb.recordFailure(org, new RuntimeException("failure " + i));
        }

        assertEquals(CircuitState.CLOSED, cb.getState(org));

        List<ILoggingEvent> errorLogs = logAppender.list.stream()
                .filter(e -> e.getLevel() == Level.ERROR)
                .filter(e -> e.getFormattedMessage().contains(org))
                .toList();

        assertTrue(errorLogs.isEmpty(),
                "No ERROR log should be emitted while circuit remains CLOSED");
    }

    // -----------------------------------------------------------------------
    // Property: Metrics gauge reports OPEN (unhealthy) state
    // -----------------------------------------------------------------------

    @Property(tries = 50)
    void metricsShouldReportUnhealthyWhenCircuitOpens(
            @ForAll("orgNames") String org,
            @ForAll @IntRange(min = 2, max = 10) int threshold) {

        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        Resilience4jCircuitBreaker cb = createCircuitBreaker(threshold, meterRegistry);

        // Force circuit open
        for (int i = 0; i < threshold; i++) {
            cb.recordFailure(org, new RuntimeException("failure " + i));
        }

        Double gaugeValue = meterRegistry.get("bridge.circuit.state")
                .tag("org", org)
                .gauge()
                .value();

        assertEquals(1.0, gaugeValue,
                "bridge.circuit.state gauge should report 1.0 (OPEN/unhealthy) for org=" + org);
    }

    // -----------------------------------------------------------------------
    // Property: State transition counter incremented on open
    // -----------------------------------------------------------------------

    @Property(tries = 50)
    void stateTransitionCounterShouldIncrementOnOpen(
            @ForAll("orgNames") String org,
            @ForAll @IntRange(min = 2, max = 10) int threshold) {

        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        Resilience4jCircuitBreaker cb = createCircuitBreaker(threshold, meterRegistry);

        // Force circuit open
        for (int i = 0; i < threshold; i++) {
            cb.recordFailure(org, new RuntimeException("failure " + i));
        }

        double transitionCount = meterRegistry.get("bridge.circuit.state.transitions")
                .tag("org", org)
                .tag("to_state", "OPEN")
                .counter()
                .count();

        assertTrue(transitionCount >= 1.0,
                "Transition counter to OPEN should be >= 1 for org=" + org);
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
        CircuitBreakerProperties props = new CircuitBreakerProperties();
        props.setFailureThreshold(threshold);
        props.setCoolDownPeriodSeconds(60);
        props.setHalfOpenMaxAttempts(3);
        return new Resilience4jCircuitBreaker(props, meterRegistry);
    }
}
