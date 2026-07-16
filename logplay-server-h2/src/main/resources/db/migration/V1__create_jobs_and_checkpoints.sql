CREATE TABLE jobs (
    -- id is derived deterministically from (group_id, idempotency_key) via JobIdGenerator.
    -- The idempotency key itself is not stored: the SDK sends it on create, the server hashes
    -- it into `id`, and any future lookup by key re-derives the id client-side. Uniqueness on
    -- (group_id, idempotency_key) is therefore enforced transitively by the PK on `id`.
    id VARCHAR(64) PRIMARY KEY,
    group_id VARCHAR(64) NOT NULL,
    name VARCHAR(256) NOT NULL,
    type VARCHAR(512) NOT NULL,
    max_retries INT NULL,
    input_data BINARY VARYING NULL,
    output_data BINARY VARYING NULL,
    created_at BIGINT NOT NULL,
    terminal_status VARCHAR(64) NULL,
    terminal_at BIGINT NULL
);

CREATE TABLE workers (
    id VARCHAR(64) PRIMARY KEY,
    heartbeat_timeout BIGINT NOT NULL,
    session_timeout BIGINT NOT NULL,
    last_heartbeat_at BIGINT NOT NULL,
    registered_at BIGINT NOT NULL
);

-- PENDING references; rows here are time-gated by available_at and ordered by enqueued_at.
-- available_at = 0 means "available immediately" — sentinel so the predicate stays a plain range scan.
CREATE TABLE job_queue (
    job_id VARCHAR(64) PRIMARY KEY,
    group_id VARCHAR(64) NOT NULL,
    type VARCHAR(512) NOT NULL,
    enqueued_at BIGINT NOT NULL,
    retries INT NOT NULL DEFAULT 0,
    available_at BIGINT NOT NULL DEFAULT 0,
    CONSTRAINT fk_job_queue_job FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
);

-- ACQUIRED references; one row per in-flight job.
CREATE TABLE job_acquired (
    job_id VARCHAR(64) PRIMARY KEY,
    group_id VARCHAR(64) NOT NULL,
    type VARCHAR(512) NOT NULL,
    acquired_by_worker_id VARCHAR(64) NOT NULL,
    acquired_at BIGINT NOT NULL,
    retries INT NOT NULL DEFAULT 0,
    CONSTRAINT fk_job_acquired_job FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE,
    CONSTRAINT fk_job_acquired_worker FOREIGN KEY (acquired_by_worker_id) REFERENCES workers(id)
);

-- Acquisition index: equality on (group_id, type), range on available_at, FIFO by enqueued_at.
CREATE INDEX idx_job_queue_acquire_ready ON job_queue(group_id, type, available_at, enqueued_at);
CREATE INDEX idx_job_acquired_worker ON job_acquired(acquired_by_worker_id);

CREATE TABLE checkpoints (
    id VARCHAR(64) PRIMARY KEY,
    job_id VARCHAR(64) NOT NULL,
    previous_checkpoint_id VARCHAR(64) NULL,
    name VARCHAR(256),
    created_at BIGINT NOT NULL,
    order_key BIGINT NOT NULL DEFAULT 0,
    data BINARY VARYING NULL,
    CONSTRAINT fk_checkpoint_job FOREIGN KEY (job_id) REFERENCES jobs(id),
    CONSTRAINT fk_checkpoint_previous FOREIGN KEY (previous_checkpoint_id) REFERENCES checkpoints(id)
);

-- Checkpoints indexes
-- Chain uniqueness is enforced by the primary key: checkpoints.id is derived
-- deterministically from (job_id, previous_checkpoint_id) via CheckpointIdGenerator,
-- so a duplicate chain position collides on the PK and no separate unique index is needed.
-- Order uniqueness needs no index either: order_key is computed server-side inside a
-- FOR UPDATE lock on the job row, making collisions unreachable.
CREATE INDEX idx_checkpoints_job_order ON checkpoints(job_id, order_key);

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
