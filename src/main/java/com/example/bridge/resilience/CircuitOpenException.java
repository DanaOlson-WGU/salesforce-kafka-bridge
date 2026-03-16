package com.example.bridge.resilience;

/**
 * Thrown when an operation is rejected because the circuit breaker is open.
 */
public class CircuitOpenException extends RuntimeException {

    private final String org;

    public CircuitOpenException(String org) {
        super("Circuit breaker is open for org: " + org);
        this.org = org;
    }

    public CircuitOpenException(String org, Throwable cause) {
        super("Circuit breaker is open for org: " + org, cause);
        this.org = org;
    }

    public String getOrg() {
        return org;
    }
}
