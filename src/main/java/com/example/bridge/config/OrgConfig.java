package com.example.bridge.config;

import java.util.List;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;

public class OrgConfig {

    @NotBlank(message = "pubsub-url is required for each Salesforce org")
    private String pubsubUrl;

    @NotBlank(message = "oauth-url is required for each Salesforce org")
    private String oauthUrl;

    @NotBlank(message = "client-id is required for each Salesforce org")
    private String clientId;

    @NotBlank(message = "client-secret is required for each Salesforce org")
    private String clientSecret;

    @NotBlank(message = "username is required for each Salesforce org")
    private String username;

    @NotBlank(message = "password is required for each Salesforce org")
    private String password;

    @NotEmpty(message = "At least one topic is required for each Salesforce org")
    private List<String> topics;

    private boolean enabled = true;

    public String getPubsubUrl() {
        return pubsubUrl;
    }

    public void setPubsubUrl(String pubsubUrl) {
        this.pubsubUrl = pubsubUrl;
    }

    public String getOauthUrl() {
        return oauthUrl;
    }

    public void setOauthUrl(String oauthUrl) {
        this.oauthUrl = oauthUrl;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getClientSecret() {
        return clientSecret;
    }

    public void setClientSecret(String clientSecret) {
        this.clientSecret = clientSecret;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public List<String> getTopics() {
        return topics;
    }

    public void setTopics(List<String> topics) {
        this.topics = topics;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    @Override
    public String toString() {
        return "OrgConfig{" +
                "pubsubUrl='" + pubsubUrl + '\'' +
                ", oauthUrl='" + oauthUrl + '\'' +
                ", clientId='" + clientId + '\'' +
                ", clientSecret='***'" +
                ", username='" + username + '\'' +
                ", password='***'" +
                ", topics=" + topics +
                ", enabled=" + enabled +
                '}';
    }
}
