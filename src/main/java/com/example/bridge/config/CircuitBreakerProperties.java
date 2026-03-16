package com.example.bridge.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "resilience.circuit-breaker")
public class CircuitBreakerProperties {

    private int failureThreshold = 5;
    private int coolDownPeriodSeconds = 60;
    private int halfOpenMaxAttempts = 3;

    public int getFailureThreshold() {
        return failureThreshold;
    }

    public void setFailureThreshold(int failureThreshold) {
        this.failureThreshold = failureThreshold;
    }

    public int getCoolDownPeriodSeconds() {
        return coolDownPeriodSeconds;
    }

    public void setCoolDownPeriodSeconds(int coolDownPeriodSeconds) {
        this.coolDownPeriodSeconds = coolDownPeriodSeconds;
    }

    public int getHalfOpenMaxAttempts() {
        return halfOpenMaxAttempts;
    }

    public void setHalfOpenMaxAttempts(int halfOpenMaxAttempts) {
        this.halfOpenMaxAttempts = halfOpenMaxAttempts;
    }
}
