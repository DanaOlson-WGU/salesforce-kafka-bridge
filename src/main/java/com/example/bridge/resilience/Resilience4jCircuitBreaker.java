package com.example.bridge.resilience;

import com.example.bridge.config.CircuitBreakerProperties;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

/**
 * Resilience4j-backed circuit breaker with per-org instances,
 * Micrometer metrics emission, and ERROR logging on circuit open.
 */
@Component
public class Resilience4jCircuitBreaker implements CircuitBreaker {

    private static final Logger log = LoggerFactory.getLogger(Resilience4jCircuitBreaker.class);

    private final CircuitBreakerRegistry registry;
    private final MeterRegistry meterRegistry;
    private final ConcurrentMap<String, io.github.resilience4j.circuitbreaker.CircuitBreaker> breakers =
            new ConcurrentHashMap<>();

    public Resilience4jCircuitBreaker(CircuitBreakerProperties properties, MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;

        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .failureRateThreshold(100f) // we use count-based, so this is effectively "all in window"
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(properties.getFailureThreshold())
                .minimumNumberOfCalls(properties.getFailureThreshold())
                .waitDurationInOpenState(Duration.ofSeconds(properties.getCoolDownPeriodSeconds()))
                .permittedNumberOfCallsInHalfOpenState(properties.getHalfOpenMaxAttempts())
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .build();

        this.registry = CircuitBreakerRegistry.of(config);
    }

    @Override
    public <T> T execute(String org, Supplier<T> operation) throws CircuitOpenException {
        io.github.resilience4j.circuitbreaker.CircuitBreaker cb = getOrCreate(org);
        try {
            return cb.executeSupplier(operation);
        } catch (io.github.resilience4j.circuitbreaker.CallNotPermittedException e) {
            throw new CircuitOpenException(org, e);
        }
    }

    @Override
    public void recordSuccess(String org) {
        getOrCreate(org).onSuccess(0, java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    @Override
    public void recordFailure(String org, Exception error) {
        getOrCreate(org).onError(0, java.util.concurrent.TimeUnit.MILLISECONDS, error);
    }

    @Override
    public CircuitState getState(String org) {
        return mapState(getOrCreate(org).getState());
    }

    private io.github.resilience4j.circuitbreaker.CircuitBreaker getOrCreate(String org) {
        return breakers.computeIfAbsent(org, this::createBreaker);
    }

    private io.github.resilience4j.circuitbreaker.CircuitBreaker createBreaker(String org) {
        io.github.resilience4j.circuitbreaker.CircuitBreaker cb = registry.circuitBreaker("bridge-" + org);

        // Register state-change gauge metric
        meterRegistry.gauge("bridge.circuit.state", Tags.of("org", org), cb,
                b -> mapStateToDouble(b.getState()));

        // Listen for state transitions and log/emit metrics
        cb.getEventPublisher().onStateTransition(event -> {
            CircuitState newState = mapState(event.getStateTransition().getToState());
            if (newState == CircuitState.OPEN) {
                log.error("Circuit breaker OPEN for org={}: transition={}",
                        org, event.getStateTransition());
            } else {
                log.info("Circuit breaker state change for org={}: transition={}",
                        org, event.getStateTransition());
            }
            meterRegistry.counter("bridge.circuit.state.transitions",
                    "org", org, "to_state", newState.name()).increment();
        });

        return cb;
    }

    private static CircuitState mapState(io.github.resilience4j.circuitbreaker.CircuitBreaker.State state) {
        return switch (state) {
            case OPEN, FORCED_OPEN -> CircuitState.OPEN;
            case HALF_OPEN -> CircuitState.HALF_OPEN;
            default -> CircuitState.CLOSED;
        };
    }

    private static double mapStateToDouble(io.github.resilience4j.circuitbreaker.CircuitBreaker.State state) {
        return switch (state) {
            case OPEN, FORCED_OPEN -> 1.0;
            case HALF_OPEN -> 2.0;
            default -> 0.0;
        };
    }
}
