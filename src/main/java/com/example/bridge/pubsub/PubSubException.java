package com.example.bridge.pubsub;

import io.grpc.Status;

/**
 * Exception thrown when a Pub/Sub API operation fails.
 * Wraps gRPC status codes and provides classification for retry decisions.
 */
public class PubSubException extends RuntimeException {

    private final Status.Code grpcStatusCode;
    private final boolean retryable;

    public PubSubException(String message, Status.Code grpcStatusCode) {
        super(message);
        this.grpcStatusCode = grpcStatusCode;
        this.retryable = isRetryableStatus(grpcStatusCode);
    }

    public PubSubException(String message, Throwable cause, Status.Code grpcStatusCode) {
        super(message, cause);
        this.grpcStatusCode = grpcStatusCode;
        this.retryable = isRetryableStatus(grpcStatusCode);
    }

    public Status.Code getGrpcStatusCode() {
        return grpcStatusCode;
    }

    public boolean isRetryable() {
        return retryable;
    }

    /**
     * Determines if the given gRPC status code indicates a retryable error.
     * 
     * Retryable status codes:
     * - UNAVAILABLE: Network issue, Salesforce downtime
     * - DEADLINE_EXCEEDED: Timeout on gRPC call
     * - RESOURCE_EXHAUSTED: Rate limit exceeded
     * 
     * Non-retryable status codes:
     * - UNAUTHENTICATED: Requires token refresh, not simple retry
     * - INVALID_ARGUMENT: Configuration error, won't succeed on retry
     */
    private static boolean isRetryableStatus(Status.Code code) {
        return code == Status.Code.UNAVAILABLE ||
               code == Status.Code.DEADLINE_EXCEEDED ||
               code == Status.Code.RESOURCE_EXHAUSTED;
    }

    /**
     * Creates a PubSubException from a gRPC StatusRuntimeException.
     */
    public static PubSubException fromGrpcStatus(io.grpc.StatusRuntimeException e) {
        Status status = e.getStatus();
        return new PubSubException(
                "gRPC error: " + status.getDescription(),
                e,
                status.getCode()
        );
    }
}
