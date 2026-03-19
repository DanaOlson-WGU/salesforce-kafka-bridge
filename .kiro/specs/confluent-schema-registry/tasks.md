# Implementation Plan: Confluent Schema Registry Integration

## Overview

Integrate Confluent Schema Registry into the Salesforce-Kafka Bridge, replacing raw JSON byte array serialization with Avro-encoded messages using a generic envelope schema. Implementation proceeds bottom-up: dependencies → schema → new components → modify existing components → configuration → Docker → tests.

## Tasks

- [x] 1. Add Confluent Maven dependencies and repository
  - [x] 1.1 Add Confluent Maven repository and Avro dependencies to pom.xml
    - Add `confluent.version` property (7.6.0) to `<properties>`
    - Add Confluent Maven repository (`https://packages.confluent.io/maven/`) to `<repositories>`
    - Add `org.apache.avro:avro:1.11.3` dependency
    - Add `io.confluent:kafka-avro-serializer:${confluent.version}` dependency
    - Add `io.confluent:kafka-schema-registry-client:${confluent.version}` dependency
    - _Requirements: 1.1, 1.2, 1.3, 1.4_

- [x] 2. Create Avro envelope schema and schema loader
  - [x] 2.1 Create the SalesforcePlatformEvent.avsc schema file
    - Create `src/main/resources/avro/SalesforcePlatformEvent.avsc`
    - Define record with name `SalesforcePlatformEvent`, namespace `com.example.bridge.avro`
    - Include fields: `org` (string), `eventType` (string), `replayId` (bytes), `timestamp` (string), `payload` (string)
    - _Requirements: 2.1, 2.2, 2.3_

  - [x] 2.2 Implement SchemaRegistryLoader configuration class
    - Create `src/main/java/com/example/bridge/config/SchemaRegistryLoader.java`
    - Implement `@Bean` method that loads and parses the `.avsc` file from classpath
    - Throw `IllegalStateException` with descriptive message if file is missing or malformed
    - _Requirements: 2.4, 2.5_

  - [x] 2.3 Write unit tests for schema structure and loading
    - Create `src/test/java/com/example/bridge/avro/SchemaStructureTest.java` — verify schema name, namespace, 5 fields with correct types
    - Create `src/test/java/com/example/bridge/avro/SchemaRegistryLoaderTest.java` — verify successful load and fail-fast on missing/malformed schema
    - _Requirements: 2.1, 2.2, 2.4, 2.5_

- [x] 3. Implement SchemaRegistryProperties and application configuration
  - [x] 3.1 Create SchemaRegistryProperties configuration class
    - Create `src/main/java/com/example/bridge/config/SchemaRegistryProperties.java`
    - Annotate with `@ConfigurationProperties(prefix = "schema-registry")`
    - Include fields: `url`, `authSource`, `userInfo`, `compatibilityLevel`
    - Enable configuration properties scanning in the application
    - _Requirements: 6.1, 6.3, 6.4, 9.1, 9.2, 9.3_

  - [x] 3.2 Add schema-registry configuration to application YAML files
    - Add `schema-registry` block to `application.yml` with env var placeholders and defaults
    - Add `schema-registry.url: http://localhost:8081` to `application-dev.yml`
    - Add `schema-registry` block with Confluent Cloud auth to `application-staging.yml`
    - Add `schema-registry` block with Confluent Cloud auth to `application-prod.yml`
    - _Requirements: 6.1, 6.2, 6.5, 6.6, 7.5, 9.2_

- [x] 4. Implement AvroRecordConverter
  - [x] 4.1 Create AvroRecordConverter component
    - Create `src/main/java/com/example/bridge/avro/AvroRecordConverter.java`
    - Inject the Avro `Schema` bean and `ObjectMapper`
    - Implement `toGenericRecord(EventBatch batch, PlatformEvent event)` method
    - Map fields: org, eventType, replayId (as ByteBuffer), timestamp (ISO-8601 string), payload (JSON string)
    - _Requirements: 3.1, 5.1, 5.3_

  - [x] 4.2 Write property test for PlatformEvent Avro round trip
    - **Property 1: PlatformEvent Avro Round Trip**
    - Create `src/test/java/com/example/bridge/avro/AvroRoundTripPropertyTest.java`
    - Generate random org, eventType, replayId bytes, timestamp, and JSON payloads
    - Convert to GenericRecord via AvroRecordConverter, extract fields back, assert equivalence
    - **Validates: Requirements 5.1, 3.1, 4.3, 4.4, 5.3**

  - [x] 4.3 Write property test for Avro binary serialization round trip
    - **Property 2: Avro Binary Serialization Round Trip**
    - Add test to `src/test/java/com/example/bridge/avro/AvroRoundTripPropertyTest.java`
    - Generate random GenericRecords, serialize with KafkaAvroSerializer, deserialize with KafkaAvroDeserializer
    - Assert all field values match original
    - **Validates: Requirements 5.2**

  - [x] 4.4 Write unit tests for AvroRecordConverter
    - Create `src/test/java/com/example/bridge/avro/AvroRecordConverterTest.java`
    - Test specific conversion cases and edge conditions
    - _Requirements: 3.1, 5.1_

- [x] 5. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 6. Modify BridgeConfiguration for KafkaAvroSerializer
  - [x] 6.1 Add ProducerFactory and KafkaTemplate beans to BridgeConfiguration
    - Add `ProducerFactory<String, GenericRecord>` bean with KafkaAvroSerializer as value serializer
    - Configure Schema Registry URL and optional basic auth from SchemaRegistryProperties
    - Add `KafkaTemplate<String, GenericRecord>` bean
    - Retain StringSerializer for keys, configure acks=all and idempotence
    - _Requirements: 3.2, 3.3, 3.4, 6.1, 6.3_

  - [x] 6.2 Add consumer reference configuration bean
    - Add `ConsumerFactory<String, GenericRecord>` bean with KafkaAvroDeserializer
    - Configure Schema Registry URL and optional basic auth
    - Set `specific.avro.reader` to false for GenericRecord usage
    - _Requirements: 4.1, 4.2_

  - [x] 6.3 Write unit tests for Kafka Avro producer configuration
    - Create `src/test/java/com/example/bridge/kafka/KafkaAvroProducerConfigTest.java`
    - Verify KafkaAvroSerializer is configured as value serializer
    - Verify StringSerializer retained as key serializer
    - Verify Schema Registry URL and auth properties are passed through
    - _Requirements: 3.2, 3.4, 6.1, 6.3_

- [x] 7. Modify DefaultKafkaEventPublisher for GenericRecord
  - [x] 7.1 Update DefaultKafkaEventPublisher to use GenericRecord
    - Change `KafkaTemplate<String, byte[]>` to `KafkaTemplate<String, GenericRecord>`
    - Inject `AvroRecordConverter` as a constructor dependency
    - Replace `objectMapper.writeValueAsBytes(event.getPayload())` with `avroRecordConverter.toGenericRecord(batch, event)`
    - Update `ProducerRecord` type from `<String, byte[]>` to `<String, GenericRecord>`
    - Preserve all header logic and composite key logic unchanged
    - _Requirements: 3.1, 3.2, 3.5, 8.1, 8.2_

  - [x] 7.2 Write property test for serialization failure skips checkpoint
    - **Property 3: Serialization Failure Skips Checkpoint**
    - Create `src/test/java/com/example/bridge/kafka/AvroSerializationFailurePropertyTest.java`
    - Generate random batches, inject serialization failures via mock
    - Assert ReplayStore.checkpoint() is never called and result is failure
    - **Validates: Requirements 3.5**

  - [x] 7.3 Write property test for message metadata preservation
    - **Property 4: Message Metadata Preservation**
    - Create `src/test/java/com/example/bridge/kafka/MessageMetadataPreservationPropertyTest.java`
    - Generate random events and batches, publish via publisher with capturing KafkaTemplate mock
    - Assert headers (replay-id, org, event-type, received-at, bridge-version) and composite key format match
    - **Validates: Requirements 8.1, 8.2**

- [x] 8. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 9. Add Schema Registry to Docker Compose
  - [x] 9.1 Add schema-registry service to docker-compose.yml
    - Add `schema-registry` service using `confluentinc/cp-schema-registry:7.6.0` image
    - Configure `depends_on: kafka` with `condition: service_healthy`
    - Expose port 8081
    - Set `SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka:29092`
    - Add healthcheck using `curl -f http://localhost:8081/subjects`
    - _Requirements: 7.1, 7.2, 7.3, 7.4_

  - [x] 9.2 Update bridge service in docker-compose.yml
    - Add `schema-registry` to bridge service `depends_on` with `condition: service_healthy`
    - Add `SCHEMA_REGISTRY_URL: http://schema-registry:8081` environment variable
    - _Requirements: 6.2, 7.2_

  - [x] 9.3 Write unit test for Docker Compose Schema Registry configuration
    - Create `src/test/java/com/example/bridge/docker/SchemaRegistryDockerTest.java`
    - Verify schema-registry service exists with correct image, port, depends_on, and healthcheck
    - Verify bridge service depends on schema-registry
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5_

- [x] 10. Schema Registry configuration property tests
  - [x] 10.1 Write property test for Schema Registry URL environment override
    - **Property 5: Schema Registry URL Environment Override**
    - Create `src/test/java/com/example/bridge/config/SchemaRegistryConfigPropertyTest.java`
    - Generate random URL strings, set as environment variable, assert resolved URL matches
    - **Validates: Requirements 6.2**

  - [x] 10.2 Write unit tests for schema compatibility configuration
    - Add tests to `SchemaRegistryConfigPropertyTest.java` or separate test class
    - Verify default BACKWARD compatibility mode
    - Verify env var override for compatibility mode
    - _Requirements: 9.2, 9.3_

- [x] 11. Final checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Property tests use jqwik 1.8.4 (already in project dependencies)
- Checkpoints ensure incremental validation after major milestones
- The bridge service in docker-compose depends on schema-registry being healthy before starting
