package com.example.bridge.replay;

/**
 * Exception thrown when a checkpoint operation fails due to database error.
 * This exception triggers fail-fast behavior to prevent event loss.
 */
public class CheckpointException extends Exception {

    public CheckpointException(String message) {
        super(message);
    }

    public CheckpointException(String message, Throwable cause) {
        super(message, cause);
    }
}
