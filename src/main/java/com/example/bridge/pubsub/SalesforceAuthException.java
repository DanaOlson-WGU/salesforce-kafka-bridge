package com.example.bridge.pubsub;

/**
 * Exception thrown when Salesforce OAuth authentication fails.
 */
public class SalesforceAuthException extends RuntimeException {

    public SalesforceAuthException(String message) {
        super(message);
    }

    public SalesforceAuthException(String message, Throwable cause) {
        super(message, cause);
    }
}
