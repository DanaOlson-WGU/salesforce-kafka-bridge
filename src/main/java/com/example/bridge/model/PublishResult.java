package com.example.bridge.model;

import java.util.Optional;

/**
 * Result of publishing an event batch to Kafka.
 */
public class PublishResult {

    private final boolean success;
    private final int publishedCount;
    private final Optional<Exception> error;

    private PublishResult(boolean success, int publishedCount, Exception error) {
        this.success = success;
        this.publishedCount = publishedCount;
        this.error = Optional.ofNullable(error);
    }

    public static PublishResult success(int publishedCount) {
        return new PublishResult(true, publishedCount, null);
    }

    public static PublishResult failure(Exception error) {
        return new PublishResult(false, 0, error);
    }

    public boolean isSuccess() {
        return success;
    }

    public int getPublishedCount() {
        return publishedCount;
    }

    public Optional<Exception> getError() {
        return error;
    }

    @Override
    public String toString() {
        return "PublishResult{success=" + success + ", publishedCount=" + publishedCount +
                ", error=" + error.map(Exception::getMessage).orElse("none") + "}";
    }
}
