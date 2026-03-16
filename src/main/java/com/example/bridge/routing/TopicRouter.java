package com.example.bridge.routing;

import java.util.Optional;

/**
 * Interface for routing Salesforce Platform Event topics to Kafka topics.
 * 
 * The TopicRouter provides topic mapping lookup per org and validates
 * routing configuration at startup.
 */
public interface TopicRouter {
    
    /**
     * Returns the Kafka topic for a given Salesforce topic and org.
     * 
     * @param org The Salesforce org identifier (e.g., "srm", "acad")
     * @param salesforceTopic The Salesforce Platform Event topic name
     * @return Optional containing Kafka topic, or empty if no mapping exists
     */
    Optional<String> getKafkaTopic(String org, String salesforceTopic);
    
    /**
     * Validates all routing configuration at startup.
     * 
     * @throws ConfigurationException if any mapping is invalid
     */
    void validateConfiguration() throws ConfigurationException;
}
