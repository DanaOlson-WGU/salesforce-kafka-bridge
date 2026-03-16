package com.example.bridge.model;

/**
 * Represents the connection status of a component, used for health checks.
 */
public class ConnectionStatus {

    private final boolean healthy;
    private final String detail;

    private ConnectionStatus(boolean healthy, String detail) {
        this.healthy = healthy;
        this.detail = detail;
    }

    public static ConnectionStatus up(String detail) {
        return new ConnectionStatus(true, detail);
    }

    public static ConnectionStatus down(String detail) {
        return new ConnectionStatus(false, detail);
    }

    public boolean isHealthy() {
        return healthy;
    }

    public String getDetail() {
        return detail;
    }

    @Override
    public String toString() {
        return "ConnectionStatus{healthy=" + healthy + ", detail='" + detail + "'}";
    }
}
