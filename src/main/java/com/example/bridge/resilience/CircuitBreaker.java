package com.example.bridge.resilience;

import java.util.function.Supplier;

/**
 * Circuit breaker interface for managing per-org failure isolation.
 */
public interface CircuitBreaker {

    /**
     * Executes the operation if the circuit is closed or half-open.
     * Rejects with {@link CircuitOpenException} if the circuit is open.
     *
     * @param org       the Salesforce org identifier
     * @param operation the operation to execute
     * @param <T>       the return type
     * @return the result of the operation
     * @throws CircuitOpenException if the circuit is open
     */
    <T> T execute(String org, Supplier<T> operation) throws CircuitOpenException;

    /**
     * Records a successful operation for the given org, potentially closing the circuit.
     */
    void recordSuccess(String org);

    /**
     * Records a failed operation for the given org, potentially opening the circuit.
     */
    void recordFailure(String org, Exception error);

    /**
     * Returns the current circuit state for the given org.
     */
    CircuitState getState(String org);
}
