package com.example.bridge.pubsub;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import com.example.bridge.config.OrgConfig;
import com.example.bridge.config.SalesforceProperties;
import com.example.bridge.pubsub.SalesforceAuthService.OAuthHttpClient;
import com.example.bridge.pubsub.SalesforceAuthService.OAuthTokenResponse;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for SalesforceAuthService.
 *
 * Tests cover:
 * - Successful token acquisition via OAuth username-password flow
 * - Token caching (subsequent calls don't re-request)
 * - Per-org token isolation
 * - Error handling for unknown orgs, null responses, HTTP failures
 * - Instance URL retrieval
 * - Token invalidation forces refresh
 * - Thread-safe concurrent access
 *
 * Requirements: 1.1
 */
class SalesforceAuthServiceTest {

    private SalesforceProperties properties;

    @BeforeEach
    void setUp() {
        properties = new SalesforceProperties();
        Map<String, OrgConfig> orgs = new HashMap<>();
        orgs.put("srm", createOrgConfig(
                "https://login.salesforce.com/services/oauth2/token",
                "client-id-srm", "client-secret-srm",
                "user@srm.com", "pass123"
        ));
        orgs.put("acad", createOrgConfig(
                "https://test.salesforce.com/services/oauth2/token",
                "client-id-acad", "client-secret-acad",
                "user@acad.com", "pass456"
        ));
        properties.setOrgs(orgs);
    }

    @Test
    @DisplayName("should acquire token successfully on first call")
    void getAccessToken_successfulTokenAcquisition() {
        OAuthHttpClient httpClient = stubClient(
                new OAuthTokenResponse("access-token-123", "https://myorg.my.salesforce.com",
                        "Bearer", String.valueOf(System.currentTimeMillis()), null, null));

        SalesforceAuthService authService = new SalesforceAuthService(properties, httpClient);

        String token = authService.getAccessToken("srm");

        assertEquals("access-token-123", token);
    }

    @Test
    @DisplayName("should return cached token on subsequent calls without re-requesting")
    void getAccessToken_returnsCachedTokenOnSubsequentCalls() {
        AtomicInteger callCount = new AtomicInteger(0);
        OAuthHttpClient httpClient = (url, cid, cs, u, p) -> {
            callCount.incrementAndGet();
            return new OAuthTokenResponse("access-token-123", "https://myorg.my.salesforce.com",
                    "Bearer", String.valueOf(System.currentTimeMillis()), null, null);
        };

        SalesforceAuthService authService = new SalesforceAuthService(properties, httpClient);

        String token1 = authService.getAccessToken("srm");
        String token2 = authService.getAccessToken("srm");

        assertEquals(token1, token2);
        assertEquals(1, callCount.get(), "Should only call HTTP client once due to caching");
    }

    @Test
    @DisplayName("should acquire different tokens for different orgs")
    void getAccessToken_differentOrgsGetDifferentTokens() {
        OAuthHttpClient httpClient = (url, cid, cs, u, p) -> {
            if (url.contains("login.salesforce.com")) {
                return new OAuthTokenResponse("srm-token", "https://srm.my.salesforce.com",
                        "Bearer", String.valueOf(System.currentTimeMillis()), null, null);
            } else {
                return new OAuthTokenResponse("acad-token", "https://acad.my.salesforce.com",
                        "Bearer", String.valueOf(System.currentTimeMillis()), null, null);
            }
        };

        SalesforceAuthService authService = new SalesforceAuthService(properties, httpClient);

        assertEquals("srm-token", authService.getAccessToken("srm"));
        assertEquals("acad-token", authService.getAccessToken("acad"));
    }

    @Test
    @DisplayName("should throw SalesforceAuthException for unknown org")
    void getAccessToken_throwsForUnknownOrg() {
        OAuthHttpClient httpClient = stubClient(null);
        SalesforceAuthService authService = new SalesforceAuthService(properties, httpClient);

        SalesforceAuthException ex = assertThrows(SalesforceAuthException.class,
                () -> authService.getAccessToken("unknown"));

        assertTrue(ex.getMessage().contains("unknown"));
    }

    @Test
    @DisplayName("should throw SalesforceAuthException when HTTP client throws")
    void getAccessToken_throwsOnHttpClientError() {
        OAuthHttpClient httpClient = (url, cid, cs, u, p) -> {
            throw new RuntimeException("Connection refused");
        };

        SalesforceAuthService authService = new SalesforceAuthService(properties, httpClient);

        SalesforceAuthException ex = assertThrows(SalesforceAuthException.class,
                () -> authService.getAccessToken("srm"));

        assertTrue(ex.getMessage().contains("srm"));
        assertInstanceOf(RuntimeException.class, ex.getCause());
    }

    @Test
    @DisplayName("should throw SalesforceAuthException when response is null")
    void getAccessToken_throwsOnNullResponse() {
        OAuthHttpClient httpClient = stubClient(null);
        SalesforceAuthService authService = new SalesforceAuthService(properties, httpClient);

        SalesforceAuthException ex = assertThrows(SalesforceAuthException.class,
                () -> authService.getAccessToken("srm"));

        assertTrue(ex.getMessage().contains("Empty or invalid"));
    }

    @Test
    @DisplayName("should throw SalesforceAuthException when access_token is null in response")
    void getAccessToken_throwsOnNullAccessTokenInResponse() {
        OAuthHttpClient httpClient = stubClient(
                new OAuthTokenResponse(null, "https://myorg.my.salesforce.com",
                        "Bearer", null, null, null));

        SalesforceAuthService authService = new SalesforceAuthService(properties, httpClient);

        SalesforceAuthException ex = assertThrows(SalesforceAuthException.class,
                () -> authService.getAccessToken("srm"));

        assertTrue(ex.getMessage().contains("Empty or invalid"));
    }

    @Test
    @DisplayName("should return instance URL from cached token response")
    void getInstanceUrl_returnsInstanceUrlFromTokenResponse() {
        OAuthHttpClient httpClient = stubClient(
                new OAuthTokenResponse("access-token-123", "https://myorg.my.salesforce.com",
                        "Bearer", String.valueOf(System.currentTimeMillis()), null, null));

        SalesforceAuthService authService = new SalesforceAuthService(properties, httpClient);

        String instanceUrl = authService.getInstanceUrl("srm");

        assertEquals("https://myorg.my.salesforce.com", instanceUrl);
    }

    @Test
    @DisplayName("should force refresh after token invalidation")
    void invalidateToken_forcesRefreshOnNextCall() {
        AtomicInteger callCount = new AtomicInteger(0);
        OAuthHttpClient httpClient = (url, cid, cs, u, p) -> {
            int count = callCount.incrementAndGet();
            return new OAuthTokenResponse("token-" + count, "https://myorg.my.salesforce.com",
                    "Bearer", String.valueOf(System.currentTimeMillis()), null, null);
        };

        SalesforceAuthService authService = new SalesforceAuthService(properties, httpClient);

        assertEquals("token-1", authService.getAccessToken("srm"));

        authService.invalidateToken("srm");

        assertEquals("token-2", authService.getAccessToken("srm"));
        assertEquals(2, callCount.get());
    }

    @Test
    @DisplayName("should handle null issued_at gracefully using default lifetime")
    void getAccessToken_handlesNullIssuedAtGracefully() {
        OAuthHttpClient httpClient = stubClient(
                new OAuthTokenResponse("access-token-123", "https://myorg.my.salesforce.com",
                        "Bearer", null, null, null));

        SalesforceAuthService authService = new SalesforceAuthService(properties, httpClient);

        String token = authService.getAccessToken("srm");

        assertEquals("access-token-123", token);
    }

    @Test
    @DisplayName("should handle non-numeric issued_at gracefully")
    void getAccessToken_handlesNonNumericIssuedAt() {
        OAuthHttpClient httpClient = stubClient(
                new OAuthTokenResponse("access-token-123", "https://myorg.my.salesforce.com",
                        "Bearer", "not-a-number", null, null));

        SalesforceAuthService authService = new SalesforceAuthService(properties, httpClient);

        String token = authService.getAccessToken("srm");

        assertEquals("access-token-123", token);
    }

    @Test
    @DisplayName("should be thread-safe for concurrent access to same org")
    void getAccessToken_threadSafeConcurrentAccess() throws InterruptedException {
        AtomicInteger callCount = new AtomicInteger(0);
        OAuthHttpClient httpClient = (url, cid, cs, u, p) -> {
            callCount.incrementAndGet();
            // Simulate some latency
            try { Thread.sleep(10); } catch (InterruptedException ignored) {}
            return new OAuthTokenResponse("shared-token", "https://myorg.my.salesforce.com",
                    "Bearer", String.valueOf(System.currentTimeMillis()), null, null);
        };

        SalesforceAuthService authService = new SalesforceAuthService(properties, httpClient);

        int threadCount = 10;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    String token = authService.getAccessToken("srm");
                    assertEquals("shared-token", token);
                } catch (InterruptedException ignored) {
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        doneLatch.await();
        executor.shutdown();

        // Due to synchronization, the HTTP client should be called very few times
        // (ideally 1, but race conditions before the lock may cause a few more)
        assertTrue(callCount.get() <= 3,
                "Expected at most a few HTTP calls due to caching, got " + callCount.get());
    }

    @Test
    @DisplayName("should pass correct credentials to HTTP client")
    void getAccessToken_passesCorrectCredentials() {
        String[] capturedArgs = new String[5];
        OAuthHttpClient httpClient = (url, cid, cs, u, p) -> {
            capturedArgs[0] = url;
            capturedArgs[1] = cid;
            capturedArgs[2] = cs;
            capturedArgs[3] = u;
            capturedArgs[4] = p;
            return new OAuthTokenResponse("token", "https://myorg.my.salesforce.com",
                    "Bearer", String.valueOf(System.currentTimeMillis()), null, null);
        };

        SalesforceAuthService authService = new SalesforceAuthService(properties, httpClient);
        authService.getAccessToken("srm");

        assertEquals("https://login.salesforce.com/services/oauth2/token", capturedArgs[0]);
        assertEquals("client-id-srm", capturedArgs[1]);
        assertEquals("client-secret-srm", capturedArgs[2]);
        assertEquals("user@srm.com", capturedArgs[3]);
        assertEquals("pass123", capturedArgs[4]);
    }

    // =========================================================================
    // Helper Methods
    // =========================================================================

    private OAuthHttpClient stubClient(OAuthTokenResponse response) {
        return (url, cid, cs, u, p) -> response;
    }

    private OrgConfig createOrgConfig(String oauthUrl, String clientId,
                                       String clientSecret, String username, String password) {
        OrgConfig config = new OrgConfig();
        config.setPubsubUrl("api.pubsub.salesforce.com:7443");
        config.setOauthUrl(oauthUrl);
        config.setClientId(clientId);
        config.setClientSecret(clientSecret);
        config.setUsername(username);
        config.setPassword(password);
        config.setTopics(List.of("/event/Test_Event__e"));
        config.setEnabled(true);
        return config;
    }
}
