package com.example.bridge.routing;

import com.example.bridge.config.BridgeProperties;
import com.example.bridge.config.SalesforceProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/**
 * Configurable implementation of TopicRouter that loads routing configuration
 * from BridgeProperties and validates against SalesforceProperties.
 * 
 * This implementation:
 * - Loads routing into immutable Map<String, Map<String, String>>
 * - Validates at startup that all orgs in routing config exist in salesforce.orgs
 * - Logs WARNING for unmapped topics
 * - Discards events without checkpointing when no mapping exists
 * 
 * Requirements: 6.1, 6.2, 6.3, 6.4
 */
@Component
public class ConfigurableTopicRouter implements TopicRouter {
    
    private static final Logger logger = LoggerFactory.getLogger(ConfigurableTopicRouter.class);
    
    private final Map<String, Map<String, String>> topicRouting;
    private final SalesforceProperties salesforceProperties;
    
    public ConfigurableTopicRouter(BridgeProperties bridgeProperties, 
                                   SalesforceProperties salesforceProperties) {
        this.salesforceProperties = salesforceProperties;
        
        // Load routing into immutable map
        if (bridgeProperties.getTopicRouting() != null) {
            this.topicRouting = Collections.unmodifiableMap(bridgeProperties.getTopicRouting());
        } else {
            this.topicRouting = Collections.emptyMap();
        }
        
        logger.info("Loaded topic routing configuration for {} orgs", topicRouting.size());
    }
    
    @Override
    public Optional<String> getKafkaTopic(String org, String salesforceTopic) {
        if (org == null || salesforceTopic == null) {
            logger.warn("Null org or topic provided: org={}, topic={}", org, salesforceTopic);
            return Optional.empty();
        }
        
        Map<String, String> orgRouting = topicRouting.get(org);
        if (orgRouting == null) {
            logger.warn("No routing configuration found for org: {}", org);
            return Optional.empty();
        }
        
        String kafkaTopic = orgRouting.get(salesforceTopic);
        if (kafkaTopic == null) {
            logger.warn("No Kafka topic mapping found for org={}, salesforceTopic={}. Event will be discarded without checkpointing.",
                    org, salesforceTopic);
            return Optional.empty();
        }
        
        return Optional.of(kafkaTopic);
    }
    
    @Override
    public void validateConfiguration() throws ConfigurationException {
        if (topicRouting.isEmpty()) {
            throw new ConfigurationException("Topic routing configuration is empty");
        }
        
        if (salesforceProperties.getOrgs() == null || salesforceProperties.getOrgs().isEmpty()) {
            throw new ConfigurationException("No Salesforce orgs configured");
        }
        
        // Validate that all orgs in routing config exist in salesforce.orgs
        for (String org : topicRouting.keySet()) {
            if (!salesforceProperties.getOrgs().containsKey(org)) {
                throw new ConfigurationException(
                        String.format("Routing configuration references undefined org: %s. " +
                                "Org must be defined in salesforce.orgs configuration.", org));
            }
        }
        
        logger.info("Topic routing configuration validated successfully");
    }
}
