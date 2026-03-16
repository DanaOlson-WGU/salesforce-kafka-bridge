package com.example.bridge.config;

import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.Valid;

@Validated
@ConfigurationProperties(prefix = "salesforce")
public class SalesforceProperties {

    @NotEmpty(message = "At least one Salesforce org must be configured under 'salesforce.orgs'")
    private Map<String, @Valid OrgConfig> orgs;
    private @Valid RetryConfig retry = new RetryConfig();

    public Map<String, OrgConfig> getOrgs() {
        return orgs;
    }

    public void setOrgs(Map<String, OrgConfig> orgs) {
        this.orgs = orgs;
    }

    public RetryConfig getRetry() {
        return retry;
    }

    public void setRetry(RetryConfig retry) {
        this.retry = retry;
    }

    @Override
    public String toString() {
        return "SalesforceProperties{" +
                "orgs=" + orgs +
                ", retry=" + retry +
                '}';
    }
}
