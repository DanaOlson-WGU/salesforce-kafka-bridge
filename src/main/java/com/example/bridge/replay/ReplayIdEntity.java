package com.example.bridge.replay;

import jakarta.persistence.*;
import java.time.Instant;

/**
 * JPA entity for persisting Salesforce replay IDs.
 */
@Entity
@Table(name = "replay_ids", uniqueConstraints = {
    @UniqueConstraint(name = "uq_org_topic", columnNames = {"org", "salesforce_topic"})
})
public class ReplayIdEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, length = 50)
    private String org;

    @Column(name = "salesforce_topic", nullable = false, length = 255)
    private String salesforceTopic;

    @Column(name = "replay_id", nullable = false)
    private byte[] replayId;

    @Column(name = "last_updated", nullable = false)
    private Instant lastUpdated;

    @PrePersist
    @PreUpdate
    protected void onUpdate() {
        lastUpdated = Instant.now();
    }

    // Default constructor for JPA
    protected ReplayIdEntity() {
    }

    public ReplayIdEntity(String org, String salesforceTopic, byte[] replayId) {
        this.org = org;
        this.salesforceTopic = salesforceTopic;
        this.replayId = replayId;
    }

    public Long getId() {
        return id;
    }

    public String getOrg() {
        return org;
    }

    public void setOrg(String org) {
        this.org = org;
    }

    public String getSalesforceTopic() {
        return salesforceTopic;
    }

    public void setSalesforceTopic(String salesforceTopic) {
        this.salesforceTopic = salesforceTopic;
    }

    public byte[] getReplayId() {
        return replayId;
    }

    public void setReplayId(byte[] replayId) {
        this.replayId = replayId;
    }

    public Instant getLastUpdated() {
        return lastUpdated;
    }

    public void setLastUpdated(Instant lastUpdated) {
        this.lastUpdated = lastUpdated;
    }
}
