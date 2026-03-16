package com.example.bridge.pubsub;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import com.example.bridge.config.OrgConfig;
import com.example.bridge.config.SalesforceProperties;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Handles OAuth 2.0 token acquisition and refresh for Salesforce orgs
 * using the username-password flow.
 */
@Service
public class SalesforceAuthService {

    private static final Logger log = LoggerFactory.getLogger(SalesforceAuthService.class);

    /**
     * Refresh tokens 5 minutes before they would expire.
     * Salesforce session tokens typically last 2 hours, but we refresh early
     * to avoid using an expired token during a request.
     */
    static final Duration TOKEN_REFRESH_BUFFER = Duration.ofMinutes(5);

    /**
     * Default token lifetime assumed when Salesforce does not provide an
     * explicit expiration. Salesforce session tokens typically last 2 hours.
     */
    static final Duration DEFAULT_TOKEN_LIFETIME = Duration.ofHours(2);

    private final SalesforceProperties salesforceProperties;
    private final OAuthHttpClient httpClient;
    private final Map<String, CachedToken> tokenCache = new ConcurrentHashMap<>();

    public SalesforceAuthService(SalesforceProperties salesforceProperties) {
        this(salesforceProperties, new RestTemplateOAuthHttpClient(new RestTemplate()));
    }

    /**
     * Constructor allowing injection of a custom HTTP client (useful for testing).
     */
    public SalesforceAuthService(SalesforceProperties salesforceProperties, OAuthHttpClient httpClient) {
        this.salesforceProperties = salesforceProperties;
        this.httpClient = httpClient;
    }

    /**
     * Returns a valid access token for the given org, fetching or refreshing as needed.
     *
     * @param orgId the org identifier (e.g. "srm", "acad")
     * @return a valid access token string
     * @throws SalesforceAuthException if token acquisition fails
     */
    public String getAccessToken(String orgId) {
        CachedToken cached = tokenCache.get(orgId);
        if (cached != null && !cached.isExpiringSoon()) {
            return cached.accessToken();
        }

        // Synchronize per-org to avoid concurrent token requests for the same org
        synchronized (orgId.intern()) {
            // Double-check after acquiring lock
            cached = tokenCache.get(orgId);
            if (cached != null && !cached.isExpiringSoon()) {
                return cached.accessToken();
            }
            return refreshToken(orgId);
        }
    }

    /**
     * Returns the instance URL for the given org from the cached token response.
     *
     * @param orgId the org identifier
     * @return the Salesforce instance URL
     * @throws SalesforceAuthException if no token has been acquired yet
     */
    public String getInstanceUrl(String orgId) {
        CachedToken cached = tokenCache.get(orgId);
        if (cached == null) {
            // Force a token fetch to populate the cache
            getAccessToken(orgId);
            cached = tokenCache.get(orgId);
        }
        return cached.instanceUrl();
    }

    /**
     * Invalidates the cached token for the given org, forcing a fresh token
     * on the next call to {@link #getAccessToken(String)}.
     */
    public void invalidateToken(String orgId) {
        tokenCache.remove(orgId);
        log.info("Invalidated cached token for org '{}'", orgId);
    }

    private String refreshToken(String orgId) {
        OrgConfig orgConfig = salesforceProperties.getOrgs().get(orgId);
        if (orgConfig == null) {
            throw new SalesforceAuthException("No configuration found for org: " + orgId);
        }

        log.debug("Requesting OAuth token for org '{}'", orgId);

        try {
            OAuthTokenResponse body = httpClient.requestToken(
                    orgConfig.getOauthUrl(),
                    orgConfig.getClientId(),
                    orgConfig.getClientSecret(),
                    orgConfig.getUsername(),
                    orgConfig.getPassword()
            );

            if (body == null || body.accessToken() == null) {
                throw new SalesforceAuthException(
                        "Empty or invalid OAuth response for org: " + orgId);
            }

            Instant expiresAt = computeExpiration(body);
            CachedToken cachedToken = new CachedToken(
                    body.accessToken(),
                    body.instanceUrl(),
                    body.tokenType(),
                    expiresAt
            );
            tokenCache.put(orgId, cachedToken);

            log.info("Successfully acquired OAuth token for org '{}', expires at {}", orgId, expiresAt);
            return body.accessToken();

        } catch (SalesforceAuthException e) {
            throw e;
        } catch (Exception e) {
            throw new SalesforceAuthException(
                    "Failed to acquire OAuth token for org: " + orgId, e);
        }
    }

    private Instant computeExpiration(OAuthTokenResponse response) {
        if (response.issuedAt() != null) {
            try {
                long issuedAtMillis = Long.parseLong(response.issuedAt());
                return Instant.ofEpochMilli(issuedAtMillis).plus(DEFAULT_TOKEN_LIFETIME);
            } catch (NumberFormatException e) {
                log.warn("Could not parse issued_at from OAuth response, using default lifetime");
            }
        }
        return Instant.now().plus(DEFAULT_TOKEN_LIFETIME);
    }

    /**
     * Abstraction for the HTTP call to the OAuth endpoint, enabling easy testing.
     */
    @FunctionalInterface
    public interface OAuthHttpClient {
        OAuthTokenResponse requestToken(String oauthUrl, String clientId,
                                         String clientSecret, String username, String password);
    }

    /**
     * Default implementation using Spring's RestTemplate.
     */
    static class RestTemplateOAuthHttpClient implements OAuthHttpClient {
        private final RestTemplate restTemplate;

        RestTemplateOAuthHttpClient(RestTemplate restTemplate) {
            this.restTemplate = restTemplate;
        }

        @Override
        public OAuthTokenResponse requestToken(String oauthUrl, String clientId,
                                                String clientSecret, String username, String password) {
            MultiValueMap<String, String> formData = new LinkedMultiValueMap<>();
            formData.add("grant_type", "password");
            formData.add("client_id", clientId);
            formData.add("client_secret", clientSecret);
            formData.add("username", username);
            formData.add("password", password);

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);

            HttpEntity<MultiValueMap<String, String>> request = new HttpEntity<>(formData, headers);

            ResponseEntity<OAuthTokenResponse> response = restTemplate.postForEntity(
                    oauthUrl, request, OAuthTokenResponse.class);
            return response.getBody();
        }
    }

    /**
     * Cached OAuth token with expiration tracking.
     */
    record CachedToken(String accessToken, String instanceUrl, String tokenType, Instant expiresAt) {
        boolean isExpiringSoon() {
            return Instant.now().plus(TOKEN_REFRESH_BUFFER).isAfter(expiresAt);
        }
    }

    /**
     * Salesforce OAuth 2.0 token response.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record OAuthTokenResponse(
            @JsonProperty("access_token") String accessToken,
            @JsonProperty("instance_url") String instanceUrl,
            @JsonProperty("token_type") String tokenType,
            @JsonProperty("issued_at") String issuedAt,
            @JsonProperty("id") String id,
            @JsonProperty("signature") String signature
    ) {}
}
