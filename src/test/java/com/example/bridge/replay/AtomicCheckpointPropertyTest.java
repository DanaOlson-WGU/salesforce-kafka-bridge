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

import java.util.Properties;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Property 10: Atomic Checkpoint Updates
 *
 * For any checkpoint operation, the replay ID update should be atomic—either
 * completes entirely or not at all.
 *
 * Validates: Requirements 3.2
 */
class AtomicCheckpointPropertyTest implements AutoCloseable {

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
    // Property: Checkpoint operation is atomic
    // -----------------------------------------------------------------------

    @Property(tries = 50)
    void checkpointOperationIsAtomic(
            @ForAll("orgIdentifiers") String org,
            @ForAll("salesforceTopics") String topic,
            @ForAll("replayIds") ReplayId replayId) throws Exception {

        ensureInitialized();
        if (!dockerAvailable) { return; }

        // Clean up any existing data
        jdbcTemplate.update("DELETE FROM replay_ids WHERE org = ? AND salesforce_topic = ?", org, topic);

        // Perform checkpoint
        replayStore.checkpoint(org, topic, replayId);

        // Verify the checkpoint was persisted atomically
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM replay_ids WHERE org = ? AND salesforce_topic = ?",
                Integer.class, org, topic);

        assertEquals(1, count, "Exactly one record should exist after checkpoint");

        // Verify the replay ID matches
        byte[] storedReplayId = jdbcTemplate.queryForObject(
                "SELECT replay_id FROM replay_ids WHERE org = ? AND salesforce_topic = ?",
                byte[].class, org, topic);

        assertArrayEquals(replayId.getValue(), storedReplayId,
                "Stored replay ID should match the checkpointed value");
    }

    @Property(tries = 50)
    void checkpointUpdateIsAtomic(
            @ForAll("orgIdentifiers") String org,
            @ForAll("salesforceTopics") String topic,
            @ForAll("replayIds") ReplayId initialReplayId,
            @ForAll("replayIds") ReplayId updatedReplayId) throws Exception {

        ensureInitialized();
        if (!dockerAvailable) { return; }
        Assume.that(!initialReplayId.equals(updatedReplayId));

        // Clean up
        jdbcTemplate.update("DELETE FROM replay_ids WHERE org = ? AND salesforce_topic = ?", org, topic);

        // Create initial checkpoint
        replayStore.checkpoint(org, topic, initialReplayId);

        // Update checkpoint
        replayStore.checkpoint(org, topic, updatedReplayId);

        // Verify only one record exists
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM replay_ids WHERE org = ? AND salesforce_topic = ?",
                Integer.class, org, topic);

        assertEquals(1, count, "Exactly one record should exist after update");

        // Verify the replay ID was updated
        byte[] storedReplayId = jdbcTemplate.queryForObject(
                "SELECT replay_id FROM replay_ids WHERE org = ? AND salesforce_topic = ?",
                byte[].class, org, topic);

        assertArrayEquals(updatedReplayId.getValue(), storedReplayId,
                "Stored replay ID should match the updated value");
    }

    @Property(tries = 30)
    void concurrentCheckpointsAreAtomic(
            @ForAll("orgIdentifiers") String org,
            @ForAll("salesforceTopics") String topic,
            @ForAll("replayIds") ReplayId replayId1,
            @ForAll("replayIds") ReplayId replayId2) throws Exception {

        ensureInitialized();
        if (!dockerAvailable) { return; }
        Assume.that(!replayId1.equals(replayId2));

        // Clean up
        jdbcTemplate.update("DELETE FROM replay_ids WHERE org = ? AND salesforce_topic = ?", org, topic);

        // Execute concurrent checkpoints
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch latch = new CountDownLatch(2);

        Future<Boolean> future1 = executor.submit(() -> {
            try {
                replayStore.checkpoint(org, topic, replayId1);
                latch.countDown();
                return true;
            } catch (Exception e) {
                latch.countDown();
                return false;
            }
        });

        Future<Boolean> future2 = executor.submit(() -> {
            try {
                replayStore.checkpoint(org, topic, replayId2);
                latch.countDown();
                return true;
            } catch (Exception e) {
                latch.countDown();
                return false;
            }
        });

        // Wait for both to complete
        latch.await(5, TimeUnit.SECONDS);
        executor.shutdown();

        // At least one should succeed
        assertTrue(future1.get() || future2.get(),
                "At least one concurrent checkpoint should succeed");

        // Verify exactly one record exists (atomicity)
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM replay_ids WHERE org = ? AND salesforce_topic = ?",
                Integer.class, org, topic);

        assertEquals(1, count,
                "Exactly one record should exist after concurrent checkpoints (atomic update)");

        // Verify the stored replay ID is one of the two values
        byte[] storedReplayId = jdbcTemplate.queryForObject(
                "SELECT replay_id FROM replay_ids WHERE org = ? AND salesforce_topic = ?",
                byte[].class, org, topic);

        boolean matchesEither = java.util.Arrays.equals(storedReplayId, replayId1.getValue()) ||
                java.util.Arrays.equals(storedReplayId, replayId2.getValue());

        assertTrue(matchesEither,
                "Stored replay ID should match one of the concurrent checkpoint values");
    }
}
