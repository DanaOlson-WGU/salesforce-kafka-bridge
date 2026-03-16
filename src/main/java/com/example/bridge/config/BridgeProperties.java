package com.example.bridge.config;

import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import jakarta.validation.constraints.NotEmpty;

@Validated
@ConfigurationProperties(prefix = "bridge")
public class BridgeProperties {

    @NotEmpty(message = "Topic routing configuration is required under 'bridge.topic-routing'")
    private Map<String, Map<String, String>> topicRouting;

    public Map<String, Map<String, String>> getTopicRouting() {
        return topicRouting;
    }

    public void setTopicRouting(Map<String, Map<String, String>> topicRouting) {
        this.topicRouting = topicRouting;
    }
}
