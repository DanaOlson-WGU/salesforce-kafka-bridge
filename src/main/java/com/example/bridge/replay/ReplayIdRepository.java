package com.example.bridge.replay;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * Repository for accessing replay ID entities.
 */
@Repository
public interface ReplayIdRepository extends JpaRepository<ReplayIdEntity, Long> {

    /**
     * Finds a replay ID entity by org and Salesforce topic.
     *
     * @param org the Salesforce org identifier
     * @param salesforceTopic the Salesforce topic name
     * @return Optional containing the entity if found
     */
    Optional<ReplayIdEntity> findByOrgAndSalesforceTopic(String org, String salesforceTopic);

    /**
     * Checks if a replay ID exists for the given org and topic.
     *
     * @param org the Salesforce org identifier
     * @param salesforceTopic the Salesforce topic name
     * @return true if exists, false otherwise
     */
    boolean existsByOrgAndSalesforceTopic(String org, String salesforceTopic);

    /**
     * Verifies database connectivity by executing a simple query.
     *
     * @return the count of records (used for health check)
     */
    @Query("SELECT COUNT(r) FROM ReplayIdEntity r")
    long countAll();
}
