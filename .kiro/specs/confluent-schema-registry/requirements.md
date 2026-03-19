# Requirements Document

## Introduction

The Salesforce-Kafka Bridge currently serializes Salesforce Platform Event payloads as raw JSON byte arrays using Jackson's ObjectMapper and the standard Kafka ByteArraySerializer. This feature integrates Confluent Schema Registry to serialize messages in Apache Avro format using a generic envelope schema (`SalesforcePlatformEvent`), enabling schema evolution, compatibility enforcement, and interoperability with Confluent Cloud Kafka. The consumer side will use the corresponding Confluent Avro deserializer to reconstruct the original event data. Because Salesforce Platform Events have dynamic, per-event-type schemas, a single envelope Avro schema wrapping the original JSON payload as a string is used rather than per-event-type Avro schemas.

## Glossary

- **Bridge**: The standalone Spring Boot microservice that connects Salesforce Pub/Sub API to Kafka
- **Schema_Registry**: The Confluent Schema Registry service that stores and validates Avro schemas
- **Avro_Envelope**: The generic `SalesforcePlatformEvent` Avro schema used to wrap all Salesforce event payloads regardless of event type
- **GenericRecord**: An Apache Avro GenericRecord instance populated according to the Avro Envelope schema
- **KafkaAvroSerializer**: The `io.confluent.kafka.serializers.KafkaAvroSerializer` class that serializes GenericRecord values and registers schemas with the Schema Registry
- **KafkaAvroDeserializer**: The `io.confluent.kafka.serializers.KafkaAvroDeserializer` class that deserializes Avro-encoded messages using schemas retrieved from the Schema Registry
- **Kafka_Producer**: The component within the Bridge that publishes events to Kafka topics
- **Platform_Event**: A Salesforce event published on the Pub/Sub API event bus
- **Schema_Compatibility**: The Confluent Schema Registry compatibility mode (e.g., BACKWARD, FORWARD, FULL) governing allowed schema changes
- **Envelope_Payload**: The original Salesforce event JSON payload stored as a string field within the Avro Envelope

## Requirements

### Requirement 1: Confluent Avro Dependencies

**User Story:** As a platform engineer, I want the Bridge to include Confluent Kafka Avro serialization libraries, so that the Bridge can produce and consume Avro-encoded messages compatible with Confluent Cloud.

#### Acceptance Criteria

1. THE Bridge SHALL include the `io.confluent:kafka-avro-serializer` library as a compile-time dependency.
2. THE Bridge SHALL include the `io.confluent:kafka-schema-registry-client` library as a compile-time dependency.
3. THE Bridge SHALL include the `org.apache.avro:avro` library as a compile-time dependency.
4. THE Bridge SHALL resolve Confluent dependencies from the Confluent Maven repository (`https://packages.confluent.io/maven/`).

### Requirement 2: Generic Avro Envelope Schema

**User Story:** As a platform engineer, I want a single generic Avro envelope schema for all Salesforce events, so that the Bridge handles dynamic Salesforce event schemas without requiring per-event-type Avro schema definitions.

#### Acceptance Criteria

1. THE Bridge SHALL define an Avro schema named `SalesforcePlatformEvent` with namespace `com.example.bridge.avro`.
2. THE Avro_Envelope SHALL contain the following fields: `org` (string), `eventType` (string), `replayId` (bytes), `timestamp` (string in ISO-8601 format), and `payload` (string containing the JSON-serialized event data).
3. THE Bridge SHALL store the Avro Envelope schema definition as a `.avsc` file in the project resources directory.
4. THE Bridge SHALL load the Avro Envelope schema from the `.avsc` file at application startup.
5. IF the Avro Envelope schema file is missing or malformed at startup, THEN THE Bridge SHALL fail to start with a descriptive error message identifying the schema loading failure.

### Requirement 3: Avro Serialization on the Producer Side

**User Story:** As a platform engineer, I want the Kafka producer to serialize event payloads as Avro GenericRecords using the KafkaAvroSerializer, so that all messages published to Kafka are Avro-encoded and their schemas are registered in the Schema Registry.

#### Acceptance Criteria

1. WHEN a Platform Event is published to Kafka, THE Kafka_Producer SHALL construct a GenericRecord conforming to the Avro Envelope schema with the org identifier, event type, replay ID, reception timestamp, and JSON-serialized payload.
2. THE Kafka_Producer SHALL use the KafkaAvroSerializer as the Kafka value serializer instead of the ByteArraySerializer.
3. WHEN the KafkaAvroSerializer serializes a GenericRecord, THE KafkaAvroSerializer SHALL register the Avro Envelope schema with the Schema_Registry if the schema is not already registered.
4. THE Kafka_Producer SHALL retain the StringSerializer as the Kafka key serializer.
5. IF serialization of a GenericRecord fails, THEN THE Kafka_Producer SHALL skip the checkpoint for the affected batch and log an error including the org identifier, event type, and failure reason.

### Requirement 4: Avro Deserialization on the Consumer Side

**User Story:** As a platform engineer, I want downstream consumers to deserialize Avro-encoded messages using the KafkaAvroDeserializer, so that consumers can reconstruct the original Salesforce event data from Kafka.

#### Acceptance Criteria

1. THE Bridge SHALL provide a consumer configuration that uses the KafkaAvroDeserializer as the Kafka value deserializer.
2. WHEN the KafkaAvroDeserializer deserializes a message, THE KafkaAvroDeserializer SHALL retrieve the corresponding Avro schema from the Schema_Registry using the schema ID embedded in the message.
3. WHEN a deserialized GenericRecord is received, THE consumer SHALL be able to extract the `org`, `eventType`, `replayId`, `timestamp`, and `payload` fields from the GenericRecord.
4. WHEN the `payload` field is extracted from a deserialized GenericRecord, THE consumer SHALL be able to parse the payload string back into the original JSON structure.

### Requirement 5: Avro Serialization Round-Trip

**User Story:** As a platform engineer, I want serialization and deserialization of Salesforce events through Avro to preserve all event data, so that no information is lost during the encoding process.

#### Acceptance Criteria

1. FOR ALL valid Platform Event payloads, serializing a Platform Event into a GenericRecord and then deserializing the GenericRecord back SHALL produce a payload equivalent to the original Platform Event payload.
2. FOR ALL valid GenericRecords conforming to the Avro Envelope schema, serializing to Avro bytes using the KafkaAvroSerializer and deserializing using the KafkaAvroDeserializer SHALL produce a GenericRecord with field values equivalent to the original.
3. FOR ALL valid JSON payload strings, storing the JSON string in the Avro Envelope `payload` field and extracting the string after deserialization SHALL produce a string equal to the original.

### Requirement 6: Schema Registry Connection Configuration

**User Story:** As a platform engineer, I want the Schema Registry URL and authentication configurable per environment, so that the Bridge connects to the correct Schema Registry in dev, staging, and production.

#### Acceptance Criteria

1. THE Bridge SHALL read the Schema Registry URL from the application configuration property `spring.kafka.properties.schema.registry.url`.
2. THE Bridge SHALL support overriding the Schema Registry URL via the environment variable `SCHEMA_REGISTRY_URL`.
3. WHERE Confluent Cloud authentication is required, THE Bridge SHALL support configuring `basic.auth.credentials.source` and `basic.auth.user.info` properties for Schema Registry authentication.
4. THE Bridge SHALL support overriding Schema Registry authentication credentials via environment variables `SCHEMA_REGISTRY_AUTH_SOURCE` and `SCHEMA_REGISTRY_USER_INFO`.
5. IF the Schema Registry is unreachable at startup, THEN THE Bridge SHALL fail to start with a descriptive error message identifying the Schema Registry connection failure.
6. THE Bridge SHALL provide environment-specific Schema Registry URLs in the `application-dev.yml`, `application-staging.yml`, and `application-prod.yml` profile configuration files.

### Requirement 7: Local Development Schema Registry

**User Story:** As a developer, I want a Schema Registry service available in the local Docker Compose environment, so that I can develop and test Avro serialization locally without connecting to Confluent Cloud.

#### Acceptance Criteria

1. THE docker-compose.yml SHALL include a Confluent Schema Registry service container using the `confluentinc/cp-schema-registry` image.
2. THE Schema Registry container SHALL depend on the Kafka service and start after Kafka is healthy.
3. THE Schema Registry container SHALL expose port 8081 for local access.
4. THE Schema Registry container SHALL be configured to connect to the local Kafka broker defined in the docker-compose.yml.
5. THE `application-dev.yml` SHALL configure the Schema Registry URL to `http://localhost:8081`.

### Requirement 8: Kafka Message Header Preservation

**User Story:** As a platform engineer, I want Kafka message headers to remain unchanged after the Avro migration, so that existing header-based routing and monitoring continues to work.

#### Acceptance Criteria

1. WHEN a Platform Event is published to Kafka using Avro serialization, THE Kafka_Producer SHALL include the same message headers as the current implementation: replay-id, org, event-type, received-at, and bridge-version.
2. THE Kafka_Producer SHALL retain the same composite key format (`org:salesforceTopic:eventId`) as the current implementation.

### Requirement 9: Schema Compatibility Configuration

**User Story:** As a platform engineer, I want the Schema Registry compatibility mode configurable, so that schema evolution is governed by an appropriate compatibility policy.

#### Acceptance Criteria

1. THE Bridge SHALL configure the default schema compatibility mode for the Avro Envelope schema via the application configuration.
2. THE Bridge SHALL default to BACKWARD compatibility mode when no explicit compatibility mode is configured.
3. WHERE a different compatibility mode is required, THE Bridge SHALL support overriding the compatibility mode via the environment variable `SCHEMA_REGISTRY_COMPATIBILITY`.
