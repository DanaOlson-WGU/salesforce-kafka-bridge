# Requirements Document

## Introduction

The Salesforce-Kafka Bridge is a standalone Spring Boot microservice that subscribes to Salesforce Platform Events via the gRPC Pub/Sub API and publishes them to Apache Kafka topics. This service extracts the event bridging responsibility from the existing enrollment application into a dedicated, independently deployable microservice. The bridge is a pure data pipeline — it contains no business logic processing. It supports multiple Salesforce orgs (SRM and ACAD), configurable topic routing, replay ID checkpointing in PostgreSQL, and comprehensive event loss prevention mechanisms.

## Glossary

- **Bridge**: The standalone Spring Boot microservice that connects Salesforce Pub/Sub API to Kafka
- **Platform_Event**: A Salesforce event published on the Pub/Sub API event bus
- **Pub_Sub_API**: The Salesforce gRPC-based API for subscribing to and receiving Platform Events
- **Replay_ID**: An opaque identifier provided by Salesforce that marks a position in the event stream, used to resume subscriptions
- **Kafka_Producer**: The component within the Bridge that publishes events to Kafka topics
- **Replay_Store**: The PostgreSQL-backed persistence layer that stores Replay IDs for each subscribed topic
- **Circuit_Breaker**: A resilience pattern that stops retry attempts after repeated failures and transitions to an open state
- **Topic_Routing**: The configurable mapping from a Salesforce Platform Event topic to a Kafka topic
- **Org**: A Salesforce organization instance; the Bridge supports SRM and ACAD orgs
- **Empty_Batch**: A Pub/Sub API response that contains zero events but includes a valid Replay ID
- **Health_Endpoint**: An HTTP endpoint exposed by the Bridge that reports the operational status of internal components
- **Idempotent_Producer**: A Kafka producer configured with `enable.idempotence=true` to prevent duplicate message delivery
- **Checkpoint**: The act of persisting a Replay ID to the Replay Store after successful Kafka publication

## Requirements

### Requirement 1: Salesforce Pub/Sub API Subscription

**User Story:** As a platform engineer, I want the Bridge to subscribe to Salesforce Platform Events via gRPC Pub/Sub API, so that events are received reliably from Salesforce.

#### Acceptance Criteria

1. WHEN the Bridge starts, THE Bridge SHALL establish a gRPC connection to the Salesforce Pub/Sub API for each configured Org.
2. WHEN a subscription is established, THE Bridge SHALL request events starting from the last persisted Replay ID for the given topic from the Replay Store.
3. WHEN no persisted Replay ID exists for a topic, THE Bridge SHALL subscribe using the earliest available Replay ID.
4. WHILE the Bridge is running, THE Bridge SHALL maintain an active subscription stream for each configured Salesforce topic.
5. IF the gRPC connection to the Pub/Sub API is lost, THEN THE Bridge SHALL attempt to reconnect using exponential backoff with a maximum retry interval of 60 seconds.
6. IF reconnection attempts exceed the configured maximum retry count, THEN THE Bridge SHALL open the Circuit Breaker for the affected Org and report the failure via the Health Endpoint.

### Requirement 2: Kafka Event Publishing

**User Story:** As a platform engineer, I want the Bridge to publish received Salesforce events to Kafka with exactly-once semantics, so that downstream consumers receive each event exactly once.

#### Acceptance Criteria

1. WHEN a Platform Event batch is received from the Pub Sub API, THE Kafka Producer SHALL publish each event to the Kafka topic determined by the Topic Routing configuration.
2. THE Kafka Producer SHALL be configured as an Idempotent Producer with `acks=all` and `enable.idempotence=true`.
3. WHEN a Platform Event is published to Kafka, THE Kafka Producer SHALL include the Salesforce event Replay ID, event type, Org identifier, and reception timestamp as Kafka message headers.
4. WHEN all events in a batch are successfully published to Kafka, THE Bridge SHALL persist the latest Replay ID from the batch to the Replay Store as a Checkpoint.
5. IF Kafka publishing fails for any event in a batch, THEN THE Bridge SHALL skip the Checkpoint for that batch and retry the entire batch on the next subscription cycle.
6. THE Kafka Producer SHALL serialize event payloads as JSON-encoded byte arrays.

### Requirement 3: Replay ID Checkpoint Persistence

**User Story:** As a platform engineer, I want Replay IDs to be checkpointed in PostgreSQL, so that the Bridge can resume from the correct position after a restart.

#### Acceptance Criteria

1. THE Replay Store SHALL persist Replay IDs in a PostgreSQL table with columns for Org identifier, Salesforce topic name, Replay ID value, and last-updated timestamp.
2. WHEN a Checkpoint is performed, THE Replay Store SHALL update the Replay ID for the given Org and topic combination atomically.
3. WHEN the Bridge starts, THE Replay Store SHALL retrieve the most recent Replay ID for each configured Org and topic combination.
4. IF a database write fails during a Checkpoint, THEN THE Bridge SHALL terminate the subscription for the affected topic and report the failure via the Health Endpoint.
5. IF a database write fails during a Checkpoint, THEN THE Bridge SHALL NOT acknowledge the event batch to prevent event loss.

### Requirement 4: Event Loss Prevention

**User Story:** As a platform engineer, I want the Bridge to implement event loss prevention mechanisms, so that no Salesforce events are lost during bridging.

#### Acceptance Criteria

1. WHEN an Empty Batch is received from the Pub Sub API, THE Bridge SHALL persist the Replay ID from the Empty Batch to the Replay Store.
2. WHEN a database persistence failure occurs during Checkpoint, THE Bridge SHALL fail fast by stopping the subscription for the affected topic within 5 seconds.
3. WHEN the Circuit Breaker transitions to the open state, THE Bridge SHALL log the event with severity ERROR including the Org identifier and topic name.
4. WHEN the Circuit Breaker is in the open state, THE Bridge SHALL attempt automatic recovery by re-establishing the subscription after the configured cool-down period.
5. WHILE the Circuit Breaker is in the open state, THE Bridge SHALL report the affected subscription as unhealthy via the Health Endpoint.
6. WHEN the Bridge resumes a subscription after a failure, THE Bridge SHALL use the last successfully checkpointed Replay ID from the Replay Store.

### Requirement 5: Multi-Org Support

**User Story:** As a platform engineer, I want the Bridge to support multiple Salesforce orgs simultaneously, so that events from SRM and ACAD orgs are bridged independently.

#### Acceptance Criteria

1. THE Bridge SHALL support concurrent subscriptions to two or more Salesforce Orgs as defined in the application configuration.
2. THE Bridge SHALL maintain independent gRPC connections, Circuit Breakers, and Replay Store entries for each configured Org.
3. IF a failure occurs for one Org subscription, THEN THE Bridge SHALL continue operating subscriptions for all other configured Orgs without interruption.
4. WHEN events are published to Kafka, THE Kafka Producer SHALL include the originating Org identifier as a Kafka message header.

### Requirement 6: Configurable Topic Routing

**User Story:** As a platform engineer, I want to configure mappings from Salesforce topics to Kafka topics, so that events are routed to the correct Kafka destinations.

#### Acceptance Criteria

1. THE Bridge SHALL read Topic Routing configuration from the Spring Boot application configuration file.
2. THE Bridge SHALL support one-to-one mapping from a Salesforce Platform Event topic to a Kafka topic for each Org.
3. WHEN a Platform Event is received for a topic with no configured Kafka mapping, THE Bridge SHALL log a warning and discard the event.
4. THE Bridge SHALL validate all Topic Routing entries at startup and fail to start if any mapping references an undefined Org.

### Requirement 7: Health Checks and Monitoring

**User Story:** As a platform engineer, I want the Bridge to expose health checks and emit metrics, so that operational issues are detected and diagnosed quickly.

#### Acceptance Criteria

1. THE Bridge SHALL expose a Spring Boot Actuator health endpoint at `/actuator/health`.
2. THE Health Endpoint SHALL report the status of each Org gRPC connection, each Kafka Producer connection, and the PostgreSQL database connection.
3. WHEN any monitored component is unhealthy, THE Health Endpoint SHALL return HTTP status 503 with details identifying the unhealthy component.
4. THE Bridge SHALL emit Micrometer metrics for: events received per topic, events published per topic, checkpoint successes, checkpoint failures, circuit breaker state changes, and gRPC reconnection attempts.
5. THE Bridge SHALL expose a Prometheus-compatible metrics endpoint at `/actuator/prometheus` for Dynatrace scraping.
6. WHEN the Bridge starts successfully, THE Bridge SHALL log a startup message including the application version, configured Orgs, and subscribed topics.

### Requirement 8: Docker Containerization

**User Story:** As a DevOps engineer, I want the Bridge packaged as a Docker container, so that it can be deployed consistently across environments.

#### Acceptance Criteria

1. THE Bridge SHALL provide a multi-stage Dockerfile that produces a minimal JRE-based container image.
2. THE Bridge SHALL support configuration via environment variables for all sensitive values including Salesforce credentials, Kafka bootstrap servers, and PostgreSQL connection strings.
3. THE Bridge SHALL use a non-root user inside the Docker container.
4. THE Bridge SHALL include a Docker health check that invokes the Health Endpoint.
5. WHEN the container starts, THE Bridge SHALL be ready to accept health check requests within 30 seconds under normal conditions.

### Requirement 9: Configuration Management

**User Story:** As a platform engineer, I want all Bridge configuration externalized and environment-specific, so that the same artifact can be deployed across dev, staging, and production.

#### Acceptance Criteria

1. THE Bridge SHALL load configuration from `application.yml` with support for Spring profile-based overrides.
2. THE Bridge SHALL support overriding any configuration property via environment variables following Spring Boot relaxed binding rules.
3. THE Bridge SHALL validate all required configuration properties at startup and fail to start with a descriptive error message if any required property is missing.
4. THE Bridge SHALL NOT log or expose any sensitive configuration values including credentials and connection strings.

### Requirement 10: Migration from Embedded to Standalone

**User Story:** As a platform engineer, I want a clear migration path from the existing embedded Pub/Sub subscription in the enrollment application to the standalone Bridge, so that the transition is safe and reversible.

#### Acceptance Criteria

1. THE Bridge SHALL be deployable alongside the existing enrollment application during a transition period without causing duplicate event processing.
2. THE Bridge SHALL use a separate Kafka consumer group and separate Replay Store table from the existing enrollment application.
3. WHEN the Bridge is deployed in parallel with the existing enrollment application, THE Bridge SHALL publish to a separate set of Kafka topics configurable per environment.
4. THE Bridge SHALL include a configuration flag to enable or disable each Org subscription independently, allowing incremental migration.
