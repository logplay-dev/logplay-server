CREATE TABLE jobs (
    id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(256) NOT NULL,
    type VARCHAR(512) NOT NULL,
    status VARCHAR(64) NOT NULL,
    retries INT NOT NULL DEFAULT 0,
    max_retries INT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    version BIGINT NOT NULL DEFAULT 0,
    last_acquired_at BIGINT NULL,
    acquired_by_worker_id VARCHAR(64) NULL,
    idempotency_key VARCHAR(64) NOT NULL,
    input_data BINARY VARYING NULL,
    output_data BINARY VARYING NULL
);

CREATE TABLE checkpoints (
    id VARCHAR(64) PRIMARY KEY,
    job_id VARCHAR(64) NOT NULL,
    previous_checkpoint_id VARCHAR(64) NULL,
    previous_checkpoint_key VARCHAR(64) GENERATED ALWAYS AS (COALESCE(previous_checkpoint_id, 'ROOT')),
    name VARCHAR(256),
    created_at BIGINT NOT NULL,
    order_key BIGINT NOT NULL DEFAULT 0,
    data BINARY VARYING NOT NULL,
    CONSTRAINT fk_checkpoint_job FOREIGN KEY (job_id) REFERENCES jobs(id),
    CONSTRAINT fk_checkpoint_previous FOREIGN KEY (previous_checkpoint_id) REFERENCES checkpoints(id),
    CONSTRAINT uq_checkpoint_chain UNIQUE (job_id, previous_checkpoint_key)
);

CREATE TABLE workers (
    id VARCHAR(64) PRIMARY KEY,
    heartbeat_timeout BIGINT NOT NULL,
    session_timeout BIGINT NOT NULL,
    last_heartbeat_at BIGINT NOT NULL,
    registered_at BIGINT NOT NULL,
    condemned BOOLEAN NOT NULL DEFAULT FALSE
);

-- Jobs foreign keys
ALTER TABLE jobs ADD CONSTRAINT fk_job_acquired_worker FOREIGN KEY (acquired_by_worker_id) REFERENCES workers(id);

-- Jobs indexes
CREATE UNIQUE INDEX uq_job_idempotency_key ON jobs(idempotency_key);
CREATE INDEX idx_jobs_status_updated_at ON jobs(status, updated_at);
CREATE INDEX idx_jobs_acquired_worker ON jobs(acquired_by_worker_id, status);

-- Checkpoints indexes
CREATE INDEX idx_checkpoints_job_id ON checkpoints(job_id);
CREATE UNIQUE INDEX uq_checkpoint_order ON checkpoints(job_id, order_key);

-- Job events table
CREATE TABLE job_events (
    id VARCHAR(64) PRIMARY KEY,
    job_id VARCHAR(64) NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    actor_type VARCHAR(64) NOT NULL,
    actor_id VARCHAR(64) NULL,
    created_at BIGINT NOT NULL,
    event_message VARCHAR(1024) NULL,
    event_detail VARCHAR(4096) NULL,
    CONSTRAINT fk_job_event_job FOREIGN KEY (job_id) REFERENCES jobs(id)
);

CREATE INDEX idx_job_events_job_id ON job_events(job_id);
