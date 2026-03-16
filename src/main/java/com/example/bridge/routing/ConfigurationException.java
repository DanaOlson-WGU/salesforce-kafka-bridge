package com.example.bridge.routing;

/**
 * Exception thrown when routing configuration is invalid.
 * 
 * This exception is thrown during startup validation when:
 * - A routing configuration references an undefined org
 * - Required routing configuration is missing
 * - Routing configuration contains invalid mappings
 */
public class ConfigurationException extends Exception {
    
    public ConfigurationException(String message) {
        super(message);
    }
    
    public ConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }
}
