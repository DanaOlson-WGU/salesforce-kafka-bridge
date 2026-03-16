-- Create replay_ids table for storing Salesforce replay ID checkpoints
CREATE TABLE replay_ids (
    id BIGSERIAL PRIMARY KEY,
    org VARCHAR(50) NOT NULL,
    salesforce_topic VARCHAR(255) NOT NULL,
    replay_id BYTEA NOT NULL,
    last_updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_org_topic UNIQUE(org, salesforce_topic)
);

-- Create index for efficient lookup by org and topic
CREATE INDEX idx_replay_ids_org_topic ON replay_ids(org, salesforce_topic);
