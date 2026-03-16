package com.example.bridge.resilience;

/**
 * Represents the state of a circuit breaker.
 */
public enum CircuitState {
    /**
     * Circuit is closed - normal operation, requests are allowed.
     */
    CLOSED,

    /**
     * Circuit is open - failures exceeded threshold, requests are rejected.
     */
    OPEN,

    /**
     * Circuit is half-open - testing if service has recovered.
     */
    HALF_OPEN
}
