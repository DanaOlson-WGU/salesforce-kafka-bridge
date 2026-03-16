package com.example.bridge.health;

import com.example.bridge.metrics.BridgeMetrics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import net.jqwik.api.*;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.lifecycle.BeforeTry;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Property 19: Metrics Emission
 *
 * For any event received from Salesforce, the bridge should increment the
 * corresponding bridge.events.received metric, and for any event published
 * to Kafka, should increment the corresponding bridge.events.published metric.
 *
 * Validates: Requirements 7.4
 */
class MetricsEmissionPropertyTest {

    private SimpleMeterRegistry meterRegistry;
    private BridgeMetrics bridgeMetrics;

    @BeforeTry
    void setup() {
        meterRegistry = new SimpleMeterRegistry();
        bridgeMetrics = new BridgeMetrics(meterRegistry);
    }

    // -----------------------------------------------------------------------
    // Property: events.received increments by exact count
    // -----------------------------------------------------------------------

    @Property(tries = 100)
    void eventsReceivedShouldIncrementByExactCount(
            @ForAll("orgNames") String org,
            @ForAll("topicNames") String topic,
            @ForAll @IntRange(min = 1, max = 100) int count) {

        bridgeMetrics.recordEventsReceived(org, topic, count);

        Counter counter = meterRegistry.find("bridge.events.received")
                .tag("org", org)
                .tag("topic", topic)
                .counter();

        assertNotNull(counter, "bridge.events.received counter should exist");
        assertEquals(count, counter.count(), 0.001,
                "bridge.events.received should increment by " + count);
    }

    // -----------------------------------------------------------------------
    // Property: events.published increments by exact count
    // -----------------------------------------------------------------------

    @Property(tries = 100)
    void eventsPublishedShouldIncrementByExactCount(
            @ForAll("orgNames") String org,
            @ForAll("topicNames") String topic,
            @ForAll @IntRange(min = 1, max = 100) int count) {

        bridgeMetrics.recordEventsPublished(org, topic, count);

        Counter counter = meterRegistry.find("bridge.events.published")
                .tag("org", org)
                .tag("topic", topic)
                .counter();

        assertNotNull(counter, "bridge.events.published counter should exist");
        assertEquals(count, counter.count(), 0.001,
                "bridge.events.published should increment by " + count);
    }

    // -----------------------------------------------------------------------
    // Property: Multiple receives accumulate correctly
    // -----------------------------------------------------------------------

    @Property(tries = 50)
    void multipleReceivesShouldAccumulate(
            @ForAll("orgNames") String org,
            @ForAll("topicNames") String topic,
            @ForAll @IntRange(min = 1, max = 50) int batch1,
            @ForAll @IntRange(min = 1, max = 50) int batch2) {

        bridgeMetrics.recordEventsReceived(org, topic, batch1);
        bridgeMetrics.recordEventsReceived(org, topic, batch2);

        Counter counter = meterRegistry.find("bridge.events.received")
                .tag("org", org)
                .tag("topic", topic)
                .counter();

        assertNotNull(counter);
        assertEquals(batch1 + batch2, counter.count(), 0.001,
                "bridge.events.received should accumulate across multiple calls");
    }

    // -----------------------------------------------------------------------
    // Property: Metrics are isolated per org and topic
    // -----------------------------------------------------------------------

    @Property(tries = 50)
    void metricsShouldBeIsolatedPerOrgAndTopic(
            @ForAll @IntRange(min = 1, max = 50) int count1,
            @ForAll @IntRange(min = 1, max = 50) int count2) {

        String org1 = "srm";
        String org2 = "acad";
        String topic = "/event/Test_Event__e";

        bridgeMetrics.recordEventsReceived(org1, topic, count1);
        bridgeMetrics.recordEventsReceived(org2, topic, count2);

        Counter counter1 = meterRegistry.find("bridge.events.received")
                .tag("org", org1).tag("topic", topic).counter();
        Counter counter2 = meterRegistry.find("bridge.events.received")
                .tag("org", org2).tag("topic", topic).counter();

        assertNotNull(counter1);
        assertNotNull(counter2);
        assertEquals(count1, counter1.count(), 0.001,
                "Metrics for org1 should be independent of org2");
        assertEquals(count2, counter2.count(), 0.001,
                "Metrics for org2 should be independent of org1");
    }

    // -----------------------------------------------------------------------
    // Property: Checkpoint success increments counter
    // -----------------------------------------------------------------------

    @Property(tries = 50)
    void checkpointSuccessShouldIncrementCounter(
            @ForAll("orgNames") String org,
            @ForAll("topicNames") String topic,
            @ForAll @IntRange(min = 1, max = 10) int times) {

        for (int i = 0; i < times; i++) {
            bridgeMetrics.recordCheckpointSuccess(org, topic);
        }

        Counter counter = meterRegistry.find("bridge.checkpoint.success")
                .tag("org", org)
                .tag("topic", topic)
                .counter();

        assertNotNull(counter, "bridge.checkpoint.success counter should exist");
        assertEquals(times, counter.count(), 0.001,
                "bridge.checkpoint.success should increment " + times + " times");
    }

    // -----------------------------------------------------------------------
    // Property: Checkpoint failure increments counter
    // -----------------------------------------------------------------------

    @Property(tries = 50)
    void checkpointFailureShouldIncrementCounter(
            @ForAll("orgNames") String org,
            @ForAll("topicNames") String topic,
            @ForAll @IntRange(min = 1, max = 10) int times) {

        for (int i = 0; i < times; i++) {
            bridgeMetrics.recordCheckpointFailure(org, topic);
        }

        Counter counter = meterRegistry.find("bridge.checkpoint.failure")
                .tag("org", org)
                .tag("topic", topic)
                .counter();

        assertNotNull(counter, "bridge.checkpoint.failure counter should exist");
        assertEquals(times, counter.count(), 0.001,
                "bridge.checkpoint.failure should increment " + times + " times");
    }

    // -----------------------------------------------------------------------
    // Property: Reconnect attempts increments counter
    // -----------------------------------------------------------------------

    @Property(tries = 50)
    void reconnectAttemptsShouldIncrementCounter(
            @ForAll("orgNames") String org,
            @ForAll @IntRange(min = 1, max = 10) int times) {

        for (int i = 0; i < times; i++) {
            bridgeMetrics.recordReconnectAttempt(org);
        }

        Counter counter = meterRegistry.find("bridge.reconnect.attempts")
                .tag("org", org)
                .counter();

        assertNotNull(counter, "bridge.reconnect.attempts counter should exist");
        assertEquals(times, counter.count(), 0.001,
                "bridge.reconnect.attempts should increment " + times + " times");
    }

    // -----------------------------------------------------------------------
    // Providers
    // -----------------------------------------------------------------------

    @Provide
    Arbitrary<String> orgNames() {
        return Arbitraries.of("srm", "acad", "org-alpha", "org-beta");
    }

    @Provide
    Arbitrary<String> topicNames() {
        return Arbitraries.of(
                "/event/Enrollment_Event__e",
                "/event/Student_Event__e",
                "/event/Course_Event__e",
                "/event/Test_Event__e"
        );
    }
}
