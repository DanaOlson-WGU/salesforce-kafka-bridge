package com.example.bridge.replay;

import com.example.bridge.model.ConnectionStatus;
import com.example.bridge.model.ReplayId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

/**
 * JPA-based implementation of ReplayStore using PostgreSQL.
 * Implements fail-fast behavior on database errors to prevent event loss.
 */
@Service
public class JpaReplayStore implements ReplayStore {

    private static final Logger logger = LoggerFactory.getLogger(JpaReplayStore.class);

    private final ReplayIdRepository repository;

    public JpaReplayStore(ReplayIdRepository repository) {
        this.repository = repository;
    }

    @Override
    @Transactional(readOnly = true)
    public Optional<ReplayId> getLastReplayId(String org, String topic) {
        try {
            return repository.findByOrgAndSalesforceTopic(org, topic)
                    .map(entity -> new ReplayId(entity.getReplayId()));
        } catch (DataAccessException e) {
            logger.error("Failed to retrieve replay ID for org={}, topic={}", org, topic, e);
            return Optional.empty();
        }
    }

    @Override
    @Transactional
    public void checkpoint(String org, String topic, ReplayId replayId) throws CheckpointException {
        try {
            Optional<ReplayIdEntity> existingEntity = repository.findByOrgAndSalesforceTopic(org, topic);

            if (existingEntity.isPresent()) {
                // Update existing entry
                ReplayIdEntity entity = existingEntity.get();
                entity.setReplayId(replayId.getValue());
                repository.save(entity);
                logger.debug("Updated replay ID checkpoint for org={}, topic={}", org, topic);
            } else {
                // Create new entry
                ReplayIdEntity newEntity = new ReplayIdEntity(org, topic, replayId.getValue());
                repository.save(newEntity);
                logger.debug("Created new replay ID checkpoint for org={}, topic={}", org, topic);
            }
        } catch (DataAccessException e) {
            String errorMsg = String.format("Checkpoint failed for org=%s, topic=%s", org, topic);
            logger.error(errorMsg, e);
            throw new CheckpointException(errorMsg, e);
        } catch (Exception e) {
            String errorMsg = String.format("Unexpected error during checkpoint for org=%s, topic=%s", org, topic);
            logger.error(errorMsg, e);
            throw new CheckpointException(errorMsg, e);
        }
    }

    @Override
    public ConnectionStatus getDatabaseStatus() {
        try {
            // Execute a simple query to verify database connectivity
            repository.countAll();
            return ConnectionStatus.up("Database connection healthy");
        } catch (DataAccessException e) {
            logger.error("Database health check failed", e);
            return ConnectionStatus.down("Database connection failed: " + e.getMessage());
        } catch (Exception e) {
            logger.error("Unexpected error during database health check", e);
            return ConnectionStatus.down("Database health check error: " + e.getMessage());
        }
    }
}
