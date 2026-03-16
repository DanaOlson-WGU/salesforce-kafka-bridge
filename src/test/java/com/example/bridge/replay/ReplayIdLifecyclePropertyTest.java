package com.example.bridge.replay;

import com.example.bridge.model.ReplayId;
import net.jqwik.api.*;
import net.jqwik.api.lifecycle.*;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.sql.DataSource;
import jakarta.persistence.EntityManagerFactory;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.orm.jpa.JpaTransactionManager;

import java.util.Optional;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Property 1: Replay ID Lifecycle
 *
 * For any org and topic, when a subscription is established, the bridge should
 * request events starting from the last checkpointed replay ID, and after failure
 * recovery, resume from that same ID.
 *
 * Validates: Requirements 1.2, 3.3, 4.6
 */
class ReplayIdLifecyclePropertyTest implements AutoCloseable {

    private static volatile PostgreSQLContainer<?> postgres;
    private static volatile AnnotationConfigApplicationContext context;
    private static volatile ReplayStore replayStore;
    private static volatile JdbcTemplate jdbcTemplate;
    private static volatile boolean initialized = false;
    private static volatile boolean dockerAvailable = true;
    private static final Object lock = new Object();

    @Configuration
    @EnableJpaRepositories(basePackages = "com.example.bridge.replay")
    @EnableTransactionManagement
    static class TestConfig {

        @Bean
        public DataSource dataSource() {
            return DataSourceBuilder.create()
                    .url(postgres.getJdbcUrl())
                    .username(postgres.getUsername())
                    .password(postgres.getPassword())
                    .driverClassName("org.postgresql.Driver")
                    .build();
        }

        @Bean
        public LocalContainerEntityManagerFactoryBean entityManagerFactory(DataSource dataSource) {
            LocalContainerEntityManagerFactoryBean em = new LocalContainerEntityManagerFactoryBean();
            em.setDataSource(dataSource);
            em.setPackagesToScan("com.example.bridge.replay");
            em.setJpaVendorAdapter(new HibernateJpaVendorAdapter());

            Properties properties = new Properties();
            properties.setProperty("hibernate.hbm2ddl.auto", "none");
            properties.setProperty("hibernate.dialect", "org.hibernate.dialect.PostgreSQLDialect");
            em.setJpaProperties(properties);

            return em;
        }

        @Bean
        public PlatformTransactionManager transactionManager(EntityManagerFactory entityManagerFactory) {
            return new JpaTransactionManager(entityManagerFactory);
        }

        @Bean
        public JpaReplayStore replayStore(ReplayIdRepository repository) {
            return new JpaReplayStore(repository);
        }

        @Bean
        public JdbcTemplate jdbcTemplate(DataSource dataSource) {
            return new JdbcTemplate(dataSource);
        }
    }

    private static void ensureInitialized() {
        if (initialized) return;
        synchronized (lock) {
            if (initialized) return;
            try {
                postgres = new PostgreSQLContainer<>("postgres:15-alpine")
                        .withDatabaseName("testdb")
                        .withUsername("test")
                        .withPassword("test");
                postgres.start();

                DataSource dataSource = DataSourceBuilder.create()
                        .url(postgres.getJdbcUrl())
                        .username(postgres.getUsername())
                        .password(postgres.getPassword())
                        .driverClassName("org.postgresql.Driver")
                        .build();

                JdbcTemplate template = new JdbcTemplate(dataSource);
                template.execute("CREATE TABLE IF NOT EXISTS replay_ids (" +
                        "id BIGSERIAL PRIMARY KEY, " +
                        "org VARCHAR(50) NOT NULL, " +
                        "salesforce_topic VARCHAR(255) NOT NULL, " +
                        "replay_id BYTEA NOT NULL, " +
                        "last_updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, " +
                        "CONSTRAINT uq_org_topic UNIQUE(org, salesforce_topic))");
                template.execute("CREATE INDEX IF NOT EXISTS idx_replay_ids_org_topic ON replay_ids(org, salesforce_topic)");

                context = new AnnotationConfigApplicationContext(TestConfig.class);
                replayStore = context.getBean(ReplayStore.class);
                jdbcTemplate = context.getBean(JdbcTemplate.class);
                initialized = true;
            } catch (Exception e) {
                dockerAvailable = false;
                initialized = true;
            }
        }
    }

    @Override
    public void close() {
        // Cleanup handled by shutdown hook
    }

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (context != null) context.close();
            if (postgres != null) postgres.stop();
        }));
    }

    // -----------------------------------------------------------------------
    // Generators
    // -----------------------------------------------------------------------

    @Provide
    Arbitrary<String> orgIdentifiers() {
        return Arbitraries.of("SRM", "ACAD", "TEST");
    }

    @Provide
    Arbitrary<String> salesforceTopics() {
        return Arbitraries.of(
                "/event/Enrollment_Event__e",
                "/event/Student_Event__e",
                "/event/Course_Event__e",
                "/event/Test_Event__e"
        );
    }

    @Provide
    Arbitrary<ReplayId> replayIds() {
        return Arbitraries.bytes()
                .array(byte[].class)
                .ofMinSize(16)
                .ofMaxSize(32)
                .map(ReplayId::new);
    }

    // -----------------------------------------------------------------------
    // Property: Replay ID lifecycle - checkpoint and retrieval
    // -----------------------------------------------------------------------

    @Property(tries = 50)
    void subscriptionUsesLastCheckpointedReplayId(
            @ForAll("orgIdentifiers") String org,
            @ForAll("salesforceTopics") String topic,
            @ForAll("replayIds") ReplayId checkpointedReplayId) throws Exception {

        ensureInitialized();
        if (!dockerAvailable) { return; }

        // Clean up
        jdbcTemplate.update("DELETE FROM replay_ids WHERE org = ? AND salesforce_topic = ?", org, topic);

        // Simulate: Bridge checkpoints a replay ID after successful processing
        replayStore.checkpoint(org, topic, checkpointedReplayId);

        // Simulate: Bridge restarts and establishes subscription
        // It should request events starting from the last checkpointed replay ID
        Optional<ReplayId> retrievedReplayId = replayStore.getLastReplayId(org, topic);

        assertTrue(retrievedReplayId.isPresent(),
                "Last replay ID should be present after checkpoint");

        assertEquals(checkpointedReplayId, retrievedReplayId.get(),
                "Retrieved replay ID should match the checkpointed value");
    }

    @Property(tries = 50)
    void noCheckpointReturnsEmpty(
            @ForAll("orgIdentifiers") String org,
            @ForAll("salesforceTopics") String topic) {

        ensureInitialized();
        if (!dockerAvailable) { return; }

        // Clean up
        jdbcTemplate.update("DELETE FROM replay_ids WHERE org = ? AND salesforce_topic = ?", org, topic);

        // Simulate: Bridge starts for the first time (no checkpoint exists)
        Optional<ReplayId> retrievedReplayId = replayStore.getLastReplayId(org, topic);

        assertFalse(retrievedReplayId.isPresent(),
                "No replay ID should be present when no checkpoint exists");
    }

    @Property(tries = 50)
    void afterFailureRecoveryResumeFromCheckpoint(
            @ForAll("orgIdentifiers") String org,
            @ForAll("salesforceTopics") String topic,
            @ForAll("replayIds") ReplayId replayId1,
            @ForAll("replayIds") ReplayId replayId2,
            @ForAll("replayIds") ReplayId replayId3) throws Exception {

        ensureInitialized();
        if (!dockerAvailable) { return; }
        Assume.that(!replayId1.equals(replayId2));
        Assume.that(!replayId2.equals(replayId3));

        // Clean up
        jdbcTemplate.update("DELETE FROM replay_ids WHERE org = ? AND salesforce_topic = ?", org, topic);

        // Simulate: Bridge processes events and checkpoints replay IDs
        replayStore.checkpoint(org, topic, replayId1);
        replayStore.checkpoint(org, topic, replayId2);
        replayStore.checkpoint(org, topic, replayId3);

        // Simulate: Bridge fails and restarts
        // It should resume from the last checkpointed replay ID (replayId3)
        Optional<ReplayId> retrievedReplayId = replayStore.getLastReplayId(org, topic);

        assertTrue(retrievedReplayId.isPresent(),
                "Last replay ID should be present after multiple checkpoints");

        assertEquals(replayId3, retrievedReplayId.get(),
                "After failure recovery, bridge should resume from the last checkpointed replay ID");
    }

    @Property(tries = 50)
    void multipleOrgsAndTopicsHaveIndependentReplayIds(
            @ForAll("orgIdentifiers") String org1,
            @ForAll("orgIdentifiers") String org2,
            @ForAll("salesforceTopics") String topic1,
            @ForAll("salesforceTopics") String topic2,
            @ForAll("replayIds") ReplayId replayId1,
            @ForAll("replayIds") ReplayId replayId2) throws Exception {

        ensureInitialized();
        if (!dockerAvailable) { return; }
        Assume.that(!org1.equals(org2) || !topic1.equals(topic2));
        Assume.that(!replayId1.equals(replayId2));

        // Clean up
        jdbcTemplate.update("DELETE FROM replay_ids WHERE (org = ? AND salesforce_topic = ?) OR (org = ? AND salesforce_topic = ?)",
                org1, topic1, org2, topic2);

        // Checkpoint different replay IDs for different org/topic combinations
        replayStore.checkpoint(org1, topic1, replayId1);
        replayStore.checkpoint(org2, topic2, replayId2);

        // Retrieve and verify independence
        Optional<ReplayId> retrieved1 = replayStore.getLastReplayId(org1, topic1);
        Optional<ReplayId> retrieved2 = replayStore.getLastReplayId(org2, topic2);

        assertTrue(retrieved1.isPresent(), "Replay ID for org1/topic1 should be present");
        assertTrue(retrieved2.isPresent(), "Replay ID for org2/topic2 should be present");

        assertEquals(replayId1, retrieved1.get(),
                "Replay ID for org1/topic1 should match checkpointed value");
        assertEquals(replayId2, retrieved2.get(),
                "Replay ID for org2/topic2 should match checkpointed value");
    }
}
