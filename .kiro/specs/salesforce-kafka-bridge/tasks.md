# Implementation Plan: Salesforce-Kafka Bridge

## Overview

Incremental implementation of the Salesforce-Kafka Bridge microservice, ordered by dependency: project scaffolding → configuration models → database/persistence → core components (TopicRouter, CircuitBreaker, KafkaEventPublisher, PubSubConnector) → event processing pipeline → observability → containerization → multi-environment config. Property-based tests (jqwik) and unit tests (JUnit 5) are included alongside each component.

## Tasks

- [ ] 1. Project scaffolding and core data models
  - [x] 1.1 Initialize Spring Boot 3.x Maven project with all dependencies
    - Create `pom.xml` with Spring Boot 3.x parent, Spring Kafka, Spring Data JPA, PostgreSQL driver, Flyway, Resilience4j, Micrometer/Prometheus, jqwik, JUnit 5, Mockito, Testcontainers, Salesforce gRPC Pub/Sub client, Jackson, gRPC dependencies
    - Create main application class `SalesforceKafkaBridgeApplication.java` with `@SpringBootApplication`
    - Create base package structure: `pubsub`, `kafka`, `replay`, `resilience`, `routing`, `config`, `health`, `model`
    - _Requirements: 1.1, 2.2, 7.1_

  - [x] 1.2 Create core data model classes
    - Create `ReplayId` value object wrapping `byte[]`
    - Create `PlatformEvent` with `eventType`, `payload` (JsonNode), `replayId`
    - Create `EventBatch` with `org`, `salesforceTopic`, `events`, `replayId`, `receivedAt`
    - Create `PublishResult` with `success`, `publishedCount`, `error`
    - Create `ConnectionStatus` with `healthy` flag and detail info
    - Create `CircuitState` enum: `CLOSED`, `OPEN`, `HALF_OPEN`
    - _Requirements: 2.1, 2.3, 2.6_

- [ ] 2. Configuration models and application properties
  - [x] 2.1 Create configuration properties classes
    - Create `SalesforceProperties` with `@ConfigurationProperties(prefix = "salesforce")` containing `Map<String, OrgConfig> orgs` and `RetryConfig retry`
    - Create `OrgConfig` with `pubsubUrl`, `oauthUrl`, `clientId`, `clientSecret`, `username`, `password`, `topics`, `enabled` flag
    - Create `RetryConfig` with `maxAttempts`, `initialIntervalMs`, `maxIntervalMs`, `multiplier`
    - Create `BridgeProperties` with `@ConfigurationProperties(prefix = "bridge")` containing `Map<String, Map<String, String>> topicRouting`
    - Create `CircuitBreakerProperties` with `@ConfigurationProperties(prefix = "resilience.circuit-breaker")` containing `failureThreshold`, `coolDownPeriodSeconds`, `halfOpenMaxAttempts`
    - _Requirements: 6.1, 9.1, 9.2_

  - [x] 2.2 Create base `application.yml` with default configuration
    - Configure Spring datasource, JPA, Flyway settings with environment variable placeholders
    - Configure Kafka producer with idempotent settings (`acks=all`, `enable.idempotence=true`)
    - Configure management endpoints (health, prometheus, info)
    - Configure logging levels and patterns
    - Configure Salesforce org structure with env var placeholders for credentials
    - Configure resilience circuit-breaker defaults
    - _Requirements: 2.2, 7.1, 7.5, 8.2, 9.1, 9.2, 9.4_

  - [x] 2.3 Write property test for required configuration validation (Property 22)
    - **Property 22: Required Configuration Validation**
    - For any required configuration property that is missing at startup, the bridge should fail to start with a descriptive error message
    - **Validates: Requirements 9.3**

  - [x] 2.4 Write property test for sensitive data protection (Property 23)
    - **Property 23: Sensitive Data Protection**
    - For any log output or exposed endpoint, sensitive configuration values should not appear in plain text
    - **Validates: Requirements 9.4**

  - [x] 2.5 Write property test for environment variable override (Property 20)
    - **Property 20: Environment Variable Configuration Override**
    - For any configuration property, setting a corresponding environment variable should override the value from application.yml
    - **Validates: Requirements 8.2, 9.2**

- [x] 3. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 4. Database schema and ReplayStore
  - [x] 4.1 Create Flyway migration and JPA entity
    - Create `V1__create_replay_ids_table.sql` in `src/main/resources/db/migration` with `replay_ids` table: `id BIGSERIAL PRIMARY KEY`, `org VARCHAR(50) NOT NULL`, `salesforce_topic VARCHAR(255) NOT NULL`, `replay_id BYTEA NOT NULL`, `last_updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP`, `UNIQUE(org, salesforce_topic)`, index on `(org, salesforce_topic)`
    - Create `ReplayIdEntity` JPA entity with `@Entity`, `@Table`, `@Id`, `@GeneratedValue`, `@PrePersist`/`@PreUpdate` for `lastUpdated`
    - Create `ReplayIdRepository` extending `JpaRepository` with custom query methods
    - _Requirements: 3.1, 3.2, 10.2_

  - [x] 4.2 Implement ReplayStore interface and JpaReplayStore
    - Create `ReplayStore` interface with `getLastReplayId`, `checkpoint`, `getDatabaseStatus`
    - Create `CheckpointException` for database write failures
    - Implement `JpaReplayStore` using `ReplayIdRepository` with upsert logic (`ON CONFLICT DO UPDATE`)
    - Implement `getDatabaseStatus` using a simple query to verify connectivity
    - Implement fail-fast behavior: throw `CheckpointException` on any database error
    - _Requirements: 3.2, 3.3, 3.4, 3.5_

  - [x] 4.3 Write property test for atomic checkpoint updates (Property 10)
    - **Property 10: Atomic Checkpoint Updates**
    - For any checkpoint operation, the replay ID update should be atomic—either completes entirely or not at all
    - Use Testcontainers PostgreSQL for real database testing
    - **Validates: Requirements 3.2**

  - [x] 4.4 Write property test for replay ID lifecycle (Property 1)
    - **Property 1: Replay ID Lifecycle**
    - For any org and topic, when a subscription is established, the bridge should request events starting from the last checkpointed replay ID, and after failure recovery, resume from that same ID
    - **Validates: Requirements 1.2, 3.3, 4.6**

  - [x] 4.5 Write unit tests for ReplayStore
    - Test checkpoint creates new entry when none exists
    - Test checkpoint updates existing entry
    - Test getLastReplayId returns empty when no entry exists
    - Test fail-fast behavior on database errors
    - Use Testcontainers PostgreSQL
    - _Requirements: 3.1, 3.2, 3.3, 3.4_

- [x] 5. TopicRouter implementation
  - [x] 5.1 Implement TopicRouter interface and ConfigurableTopicRouter
    - Create `TopicRouter` interface with `getKafkaTopic` and `validateConfiguration`
    - Create `ConfigurationException` for invalid routing config
    - Implement `ConfigurableTopicRouter` using `BridgeProperties` and `SalesforceProperties`
    - Load routing into immutable `Map<String, Map<String, String>>`
    - Validate at startup that all orgs in routing config exist in `salesforce.orgs`
    - Log WARNING for unmapped topics, discard events without checkpointing
    - _Requirements: 6.1, 6.2, 6.3, 6.4_

  - [x] 5.2 Write property test for one-to-one topic mapping (Property 16)
    - **Property 16: One-to-One Topic Mapping**
    - For any org and Salesforce topic in routing config, there should be exactly one corresponding Kafka topic
    - **Validates: Requirements 6.2**

  - [x] 5.3 Write property test for unmapped topic handling (Property 17)
    - **Property 17: Unmapped Topic Handling**
    - For any event received for a topic with no configured mapping, the bridge should log a warning and discard without checkpointing
    - **Validates: Requirements 6.3**

  - [x] 5.4 Write property test for event routing (Property 5)
    - **Property 5: Event Routing**
    - For any event batch, each event should be published to the Kafka topic determined by the routing configuration
    - **Validates: Requirements 2.1**

  - [x] 5.5 Write unit tests for TopicRouter
    - Test valid routing returns correct Kafka topic
    - Test unmapped topic returns empty Optional
    - Test startup validation fails for undefined org
    - Test configuration loaded from properties
    - _Requirements: 6.1, 6.2, 6.3, 6.4_

- [x] 6. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 7. CircuitBreaker implementation
  - [x] 7.1 Implement CircuitBreaker using Resilience4j
    - Create `CircuitBreaker` interface with `execute`, `recordSuccess`, `recordFailure`, `getState`
    - Create `CircuitOpenException`
    - Implement `Resilience4jCircuitBreaker` with per-org circuit breaker instances
    - Configure from `CircuitBreakerProperties`: failure threshold, cool-down period, half-open max attempts
    - Emit circuit state change metrics via Micrometer
    - Log ERROR on circuit open with org and topic details
    - _Requirements: 1.6, 4.3, 4.4, 4.5_

  - [x] 7.2 Write property test for circuit breaker opens after max retries (Property 4)
    - **Property 4: Circuit Breaker Opens After Max Retries**
    - For any org, when reconnection attempts exceed max retry count, circuit breaker should transition to open and health endpoint should report unhealthy
    - **Validates: Requirements 1.6**

  - [x] 7.3 Write property test for circuit breaker recovery (Property 12)
    - **Property 12: Circuit Breaker Recovery**
    - For any org with open circuit breaker, after cool-down period, the bridge should attempt to re-establish the subscription
    - **Validates: Requirements 4.4**

  - [x] 7.4 Write property test for circuit breaker observability (Property 13)
    - **Property 13: Circuit Breaker Observability**
    - For any org where circuit breaker transitions to open, bridge should log ERROR with org and topic, and health endpoint should report unhealthy
    - **Validates: Requirements 4.3, 4.5**

  - [x] 7.5 Write property test for org failure isolation (Property 15)
    - **Property 15: Org Failure Isolation**
    - For any org that experiences a failure, all other orgs should continue processing without interruption
    - **Validates: Requirements 5.3**

  - [x] 7.6 Write unit tests for CircuitBreaker
    - Test circuit transitions: closed → open → half-open → closed
    - Test per-org isolation (failure in one org doesn't affect another)
    - Test cool-down period timing
    - Test metrics emission on state change
    - _Requirements: 1.6, 4.3, 4.4, 4.5, 5.3_

- [-] 8. KafkaEventPublisher implementation
  - [x] 8.1 Implement KafkaEventPublisher
    - Create `KafkaEventPublisher` interface with `publishBatch` and `getKafkaStatus`
    - Implement `DefaultKafkaEventPublisher` using Spring Kafka `KafkaTemplate`
    - Configure idempotent producer with `enable.idempotence=true`, `acks=all`
    - Serialize payloads as JSON byte arrays using Jackson `ObjectMapper`
    - Set composite key: `{org}:{topic}:{eventId}`
    - Add Kafka headers: `replay-id`, `org`, `event-type`, `received-at`, `bridge-version`
    - Implement synchronous send with timeout for checkpoint coordination
    - Coordinate with `ReplayStore` for checkpoint after successful batch publish
    - Return failure result without checkpointing on Kafka errors
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6_

  - [x] 8.2 Write property test for message headers completeness (Property 6)
    - **Property 6: Message Headers Completeness**
    - For any event published to Kafka, headers should include replay ID, event type, org identifier, and reception timestamp
    - **Validates: Requirements 2.3, 5.4**

  - [x] 8.3 Write property test for checkpoint after successful publish (Property 7)
    - **Property 7: Checkpoint After Successful Publish**
    - For any batch where all events are successfully published, the bridge should persist the latest replay ID
    - **Validates: Requirements 2.4**

  - [x] 8.4 Write property test for no checkpoint on publish failure (Property 8)
    - **Property 8: No Checkpoint On Publish Failure**
    - For any batch where Kafka publishing fails, the bridge should not checkpoint the replay ID
    - **Validates: Requirements 2.5**

  - [x] 8.5 Write property test for JSON serialization round trip (Property 9)
    - **Property 9: JSON Serialization Round Trip**
    - For any event payload, serializing to JSON bytes and deserializing back should produce an equivalent structure
    - **Validates: Requirements 2.6**

  - [x] 8.6 Write unit tests for KafkaEventPublisher
    - Test successful batch publish returns success result
    - Test Kafka failure returns failure result without checkpoint
    - Test headers are set correctly on published messages
    - Test composite key format
    - Use embedded Kafka or Mockito mocks
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6_

- [x] 9. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 10. PubSubConnector implementation
  - [x] 10.1 Implement OAuth 2.0 authentication for Salesforce
    - Create `SalesforceAuthService` to handle OAuth token acquisition and refresh
    - Implement token request using `oauthUrl`, `clientId`, `clientSecret`, `username`, `password`
    - Cache tokens and refresh before expiration
    - _Requirements: 1.1_

  - [x] 10.2 Implement PubSubConnector interface and gRPC subscription management
    - Create `PubSubConnector` interface with `subscribe`, `stopSubscription`, `reconnect`, `getStatus`
    - Create `Subscription` handle for lifecycle management
    - Implement `GrpcPubSubConnector` using Salesforce gRPC Pub/Sub client library
    - Maintain separate `ManagedChannel` per org for connection isolation
    - Establish gRPC connections for each configured org at startup
    - Subscribe to configured topics with replay ID from `ReplayStore` (or earliest if none)
    - Handle gRPC status codes: `UNAVAILABLE`, `UNAUTHENTICATED`, `DEADLINE_EXCEEDED`, `INVALID_ARGUMENT`, `RESOURCE_EXHAUSTED`
    - Implement exponential backoff retry with `RetryConfig` parameters
    - Process incoming event batches including empty batches
    - Coordinate with `CircuitBreaker` for failure handling
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6_

  - [x] 10.3 Write property test for active subscription maintenance (Property 2)
    - **Property 2: Active Subscription Maintenance**
    - For any configured Salesforce topic, while the bridge is running and circuit breaker is closed, an active subscription stream should exist
    - **Validates: Requirements 1.4**

  - [x] 10.4 Write property test for exponential backoff retry (Property 3)
    - **Property 3: Exponential Backoff Retry**
    - For any gRPC connection loss, the bridge should attempt reconnection with exponentially increasing intervals up to max interval of 60 seconds
    - **Validates: Requirements 1.5**

  - [x] 10.5 Write property test for fail-fast on checkpoint failure (Property 11)
    - **Property 11: Fail-Fast On Checkpoint Failure**
    - For any checkpoint failure due to database error, the bridge should stop the subscription within 5 seconds, not acknowledge the batch, and report failure via health endpoint
    - **Validates: Requirements 3.4, 3.5, 4.2**

  - [x] 10.6 Write unit tests for PubSubConnector
    - Test subscription established at startup for each org
    - Test subscription uses last replay ID from ReplayStore
    - Test subscription uses earliest when no replay ID exists
    - Test exponential backoff on connection loss
    - Test circuit breaker opens after max retries
    - Test empty batch processing and checkpointing
    - Mock Salesforce gRPC API
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 4.1_

- [x] 11. Event processing pipeline integration
  - [x] 11.1 Wire components together in main application
    - Create `EventProcessingService` that coordinates `PubSubConnector`, `TopicRouter`, `KafkaEventPublisher`, `CircuitBreaker`
    - Implement batch processing flow: receive batch → check circuit → route → publish → checkpoint
    - Handle empty batches by checkpointing replay ID without publishing
    - Implement fail-fast behavior: on checkpoint failure, stop subscription and open circuit
    - Start all org subscriptions at application startup
    - _Requirements: 1.4, 2.1, 2.4, 3.4, 4.1, 4.2_

  - [x] 11.2 Write property test for multi-org concurrency (Property 14)
    - **Property 14: Multi-Org Concurrency**
    - For any configuration with N orgs, the bridge should maintain N concurrent active subscriptions with independent connections, circuit breakers, and replay store entries
    - **Validates: Requirements 5.1, 5.2**

  - [x] 11.3 Write property test for org subscription toggle (Property 24)
    - **Property 24: Org Subscription Toggle**
    - For any org with subscription disabled via configuration flag, the bridge should not establish a subscription
    - **Validates: Requirements 10.4**

  - [x] 11.4 Write integration test for end-to-end flow
    - Mock Salesforce Pub/Sub API → Bridge → Real Kafka (Testcontainers) → Real PostgreSQL (Testcontainers)
    - Verify replay ID persistence and retrieval across restarts
    - Verify circuit breaker state transitions with simulated failures
    - Verify multi-org isolation with concurrent subscriptions
    - Verify fail-fast behavior on database failures
    - _Requirements: 1.1, 1.2, 2.1, 2.4, 3.2, 3.3, 3.4, 4.2, 5.1, 5.3_

- [x] 12. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 13. Health indicators and observability
  - [x] 13.1 Implement Spring Boot Actuator health indicators
    - Create `PubSubHealthIndicator` implementing `HealthIndicator` to report status of each org gRPC connection
    - Create `KafkaHealthIndicator` to report Kafka producer connection status
    - Create `CircuitBreakerHealthIndicator` to report circuit breaker state per org
    - Create `DatabaseHealthIndicator` to report PostgreSQL connection status (may use built-in)
    - Return HTTP 503 when any component is unhealthy
    - _Requirements: 7.1, 7.2, 7.3_

  - [x] 13.2 Implement Micrometer metrics
    - Add counter `bridge.events.received{org, topic}` for events received from Salesforce
    - Add counter `bridge.events.published{org, topic}` for events published to Kafka
    - Add counter `bridge.checkpoint.success{org, topic}` for successful checkpoints
    - Add counter `bridge.checkpoint.failure{org, topic}` for failed checkpoints
    - Add gauge `bridge.circuit.state{org}` for circuit breaker state (0=closed, 1=open, 2=half-open)
    - Add counter `bridge.reconnect.attempts{org}` for gRPC reconnection attempts
    - Add timer `bridge.batch.processing.time{org, topic}` for batch processing duration
    - Configure Prometheus endpoint at `/actuator/prometheus`
    - _Requirements: 7.4, 7.5_

  - [x] 13.3 Implement startup logging
    - Log INFO message at startup with application version, configured orgs, and subscribed topics
    - _Requirements: 7.6_

  - [x] 13.4 Write property test for health endpoint component status (Property 18)
    - **Property 18: Health Endpoint Component Status**
    - For any health check request, the endpoint should report status of all org connections, Kafka producer, and PostgreSQL, and return HTTP 503 when any component is unhealthy
    - **Validates: Requirements 7.2, 7.3**

  - [x] 13.5 Write property test for metrics emission (Property 19)
    - **Property 19: Metrics Emission**
    - For any event received from Salesforce, increment `bridge.events.received`, and for any event published to Kafka, increment `bridge.events.published`
    - **Validates: Requirements 7.4**

  - [x] 13.6 Write unit tests for health indicators
    - Test health endpoint exists at `/actuator/health`
    - Test health endpoint reports all component statuses
    - Test health endpoint returns 503 when any component unhealthy
    - Test Prometheus endpoint exists at `/actuator/prometheus`
    - Test startup message includes version, orgs, topics
    - _Requirements: 7.1, 7.2, 7.3, 7.5, 7.6_

- [x] 14. Docker containerization
  - [x] 14.1 Create multi-stage Dockerfile
    - Create Dockerfile with build stage using `maven:3.9-eclipse-temurin-17`
    - Copy `pom.xml`, run `mvn dependency:go-offline`
    - Copy `src`, run `mvn clean package -DskipTests`
    - Create runtime stage using `eclipse-temurin:17-jre-alpine`
    - Create non-root user `bridge` with group `bridge`
    - Copy JAR from build stage
    - Change ownership to `bridge:bridge`
    - Switch to non-root user
    - Expose port 8080
    - Add HEALTHCHECK invoking `/actuator/health` with 30s interval, 5s timeout, 30s start period, 3 retries
    - Set ENTRYPOINT to run JAR
    - _Requirements: 8.1, 8.3, 8.4_

  - [x] 14.2 Create docker-compose.yml for local development
    - Define services: `postgres`, `zookeeper`, `kafka`, `bridge`
    - Configure PostgreSQL with `bridge_dev` database
    - Configure Kafka with Zookeeper dependency
    - Configure bridge with environment variables for DB, Kafka, Salesforce credentials
    - Add health checks for all services
    - Add volume for PostgreSQL data persistence
    - _Requirements: 8.1, 8.2, 8.4_

  - [x] 14.3 Write property test for startup timing (Property 21)
    - **Property 21: Startup Timing**
    - For any container start under normal conditions, the health endpoint should respond within 30 seconds
    - **Validates: Requirements 8.5**

  - [x] 14.4 Write unit tests for Docker configuration
    - Test Dockerfile uses multi-stage build
    - Test Dockerfile runs as non-root user
    - Test Dockerfile includes HEALTHCHECK
    - Test docker-compose defines all required services
    - _Requirements: 8.1, 8.3, 8.4_

- [x] 15. Multi-environment configuration
  - [x] 15.1 Create environment-specific application profiles
    - Create `application-dev.yml` with localhost Kafka/PostgreSQL, SRM only, dev topic routing
    - Create `application-staging.yml` with env var placeholders, both orgs enabled, staging topic routing
    - Create `application-prod.yml` with env var placeholders, both orgs enabled, production topic routing, SASL_SSL Kafka security, increased connection pools, higher circuit breaker thresholds
    - _Requirements: 9.1, 9.2, 10.3_

  - [x] 15.2 Write unit tests for multi-environment configuration
    - Test dev profile loads correctly
    - Test staging profile loads correctly
    - Test prod profile loads correctly
    - Test separate Kafka topics per environment
    - Test separate consumer group and table names
    - _Requirements: 9.1, 10.2, 10.3_

- [x] 16. Final checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 17. Integration and final validation
  - [x] 17.1 Run full integration test suite
    - Execute all property-based tests (24 properties)
    - Execute all unit tests
    - Execute all integration tests
    - Verify test coverage meets minimum thresholds
    - _Requirements: All_

  - [x] 17.2 Validate migration requirements
    - Verify bridge uses separate PostgreSQL table `replay_ids` (not `pubsub_replay_ids`)
    - Verify bridge supports separate Kafka topics during parallel deployment
    - Verify org subscription can be toggled via `enabled` flag
    - Verify configuration supports incremental migration
    - _Requirements: 10.1, 10.2, 10.3, 10.4_

  - [x] 17.3 Build and test Docker image
    - Build Docker image using Dockerfile
    - Run container with docker-compose
    - Verify health endpoint responds within 30 seconds
    - Verify all health indicators report status
    - Verify Prometheus metrics endpoint is accessible
    - Verify logs do not contain sensitive data
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 9.4_

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties (24 total)
- Unit tests validate specific examples and edge cases
- Integration tests validate end-to-end flows with real infrastructure (Testcontainers)
- Implementation uses Java with Spring Boot 3.x, Spring Kafka, Spring Data JPA, Resilience4j, jqwik, JUnit 5
