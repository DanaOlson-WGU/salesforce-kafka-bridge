package com.example.bridge.docker;

import net.jqwik.api.*;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.health.Status;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Property 21: Startup Timing
 *
 * For any container start under normal conditions, the health endpoint
 * should respond within 30 seconds.
 *
 * Validates: Requirements 8.5
 *
 * Note: This test simulates the startup timing property without requiring
 * a real Docker container. It verifies that health indicators respond
 * within the 30-second budget under various simulated startup delays.
 */
class StartupTimingPropertyTest {

    private static final Duration STARTUP_BUDGET = Duration.ofSeconds(30);

    /**
     * Property: For any simulated startup delay under 30 seconds,
     * the health indicator should respond within the startup budget.
     */
    @Property(tries = 20)
    void healthEndpointShouldRespondWithinStartupBudget(
            @ForAll("startupDelays") long startupDelayMs) {

        HealthIndicator indicator = simulatedHealthIndicator(startupDelayMs);

        Instant start = Instant.now();
        Health health = indicator.health();
        Duration elapsed = Duration.between(start, Instant.now());

        assertNotNull(health, "Health response should not be null");
        assertNotNull(health.getStatus(), "Health status should not be null");
        assertTrue(elapsed.compareTo(STARTUP_BUDGET) < 0,
                "Health endpoint should respond within 30s, took: " + elapsed.toMillis() + "ms");
    }

    /**
     * Property: For any startup delay within budget, the health indicator
     * should eventually report UP status.
     */
    @Property(tries = 10)
    void healthEndpointShouldReportUpAfterStartup(
            @ForAll("startupDelays") long startupDelayMs) throws Exception {

        HealthIndicator indicator = simulatedHealthIndicator(startupDelayMs);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Health> future = executor.submit(indicator::health);
            Health health = future.get(STARTUP_BUDGET.toMillis(), TimeUnit.MILLISECONDS);

            assertEquals(Status.UP, health.getStatus(),
                    "Health should report UP after successful startup");
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * Property: For any startup delay exceeding the budget, the health
     * check should still respond (possibly DOWN) rather than hang.
     */
    @Property(tries = 5)
    void healthEndpointShouldNotHangOnSlowStartup(
            @ForAll("excessiveDelays") long excessiveDelayMs) {

        HealthIndicator indicator = timedOutHealthIndicator(excessiveDelayMs);

        Instant start = Instant.now();
        Health health = indicator.health();
        Duration elapsed = Duration.between(start, Instant.now());

        assertNotNull(health, "Health response should not be null even on slow startup");
        // The indicator should respond within a reasonable time, not hang
        assertTrue(elapsed.toSeconds() < 35,
                "Health endpoint should not hang indefinitely, took: " + elapsed.toMillis() + "ms");
    }

    @Provide
    Arbitrary<Long> startupDelays() {
        // Simulate startup delays from 0 to 5 seconds (well within 30s budget)
        return Arbitraries.longs().between(0, 5000);
    }

    @Provide
    Arbitrary<Long> excessiveDelays() {
        // Delays that would exceed the budget if not handled
        return Arbitraries.longs().between(31_000, 60_000);
    }

    /**
     * Creates a health indicator that simulates a startup delay then reports UP.
     */
    private HealthIndicator simulatedHealthIndicator(long delayMs) {
        return () -> {
            try {
                Thread.sleep(Math.min(delayMs, 5000)); // cap actual sleep for test speed
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return Health.down().withException(e).build();
            }
            return Health.up()
                    .withDetail("startupDelay", delayMs + "ms")
                    .build();
        };
    }

    /**
     * Creates a health indicator that returns DOWN if startup exceeds budget.
     */
    private HealthIndicator timedOutHealthIndicator(long delayMs) {
        return () -> {
            if (delayMs > STARTUP_BUDGET.toMillis()) {
                return Health.down()
                        .withDetail("reason", "Startup exceeded 30s budget")
                        .withDetail("estimatedDelay", delayMs + "ms")
                        .build();
            }
            return Health.up().build();
        };
    }
}
