package com.example.bridge.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.info.BuildProperties;
import org.springframework.context.ApplicationListener;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Logs application startup information including version, configured orgs, and subscribed topics.
 * Implements Requirement 7.6: Log INFO message at startup with application version, configured orgs, and subscribed topics.
 */
@Component
public class ApplicationStartupLogger implements ApplicationListener<ApplicationReadyEvent> {

    private static final Logger log = LoggerFactory.getLogger(ApplicationStartupLogger.class);

    private final SalesforceProperties salesforceProperties;
    @Nullable
    private final BuildProperties buildProperties;

    public ApplicationStartupLogger(SalesforceProperties salesforceProperties,
                                     @Nullable BuildProperties buildProperties) {
        this.salesforceProperties = salesforceProperties;
        this.buildProperties = buildProperties;
    }

    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        String version = buildProperties != null ? buildProperties.getVersion() : "unknown";
        Map<String, OrgConfig> orgs = salesforceProperties.getOrgs();

        List<String> enabledOrgs = orgs.entrySet().stream()
                .filter(e -> e.getValue().isEnabled())
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());

        Map<String, List<String>> topicsByOrg = orgs.entrySet().stream()
                .filter(e -> e.getValue().isEnabled())
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> e.getValue().getTopics()
                ));

        log.info("Salesforce-Kafka Bridge started - version: {}, orgs: {}, topics: {}",
                version, enabledOrgs, topicsByOrg);
    }
}
