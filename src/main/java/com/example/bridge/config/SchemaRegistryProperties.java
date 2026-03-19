package com.example.bridge.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "schema-registry")
public class SchemaRegistryProperties {

    private String url;
    private String authSource;
    private String userInfo;
    private String compatibilityLevel = "BACKWARD";

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getAuthSource() {
        return authSource;
    }

    public void setAuthSource(String authSource) {
        this.authSource = authSource;
    }

    public String getUserInfo() {
        return userInfo;
    }

    public void setUserInfo(String userInfo) {
        this.userInfo = userInfo;
    }

    public String getCompatibilityLevel() {
        return compatibilityLevel;
    }

    public void setCompatibilityLevel(String compatibilityLevel) {
        this.compatibilityLevel = compatibilityLevel;
    }
}
