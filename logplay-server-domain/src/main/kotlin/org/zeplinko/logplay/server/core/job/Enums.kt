package org.zeplinko.logplay.server.core.job

/**
 * Lifecycle states of a [Job].
 *
 * Allowed transitions:
 * - `PENDING → ACQUIRED` (acquire), `PENDING → ABORTED` (abort)
 * - `ACQUIRED → FINISHED` (complete), `ACQUIRED → PENDING` (release / retryable error), `ACQUIRED →
 *   FAILED` (error after max retries), `ACQUIRED → ABORTED` (abort)
 * - `FINISHED`, `FAILED`, `ABORTED` are terminal — no further transitions.
 */
enum class JobStatus {
    /** Created but not yet picked up by a worker. */
    PENDING,
    /** A worker has claimed the job and is responsible for executing it. */
    ACQUIRED,
    /** Worker explicitly completed the job. Terminal. */
    FINISHED,
    /** Exhausted all retries. Terminal. */
    FAILED,
    /** Manually cancelled before completion. Terminal. */
    ABORTED,
}

/** Discriminator for an entry in a job's audit log. See [JobEvent]. */
enum class JobEventType {
    /** Job was created. */
    CREATED,
    /** Job was acquired by a worker. */
    ACQUIRED,
    /** Worker released the job back to `PENDING` without completing it. */
    RELEASED,
    /** Worker reported successful completion. */
    COMPLETED,
    /** Worker reported an execution error; retry will follow if budget remains. */
    ERROR_REPORTED,
    /**
     * Job exhausted its retry budget. Always emitted in the same transaction as the final
     * `ERROR_REPORTED` event.
     */
    FAILED,
    /** Job was aborted (system-initiated). */
    ABORTED,
}

/** Identifies who triggered an event. */
enum class ActorType {
    /** A worker call drove the transition. The actor id is the worker id. */
    WORKER,
    /** The server itself drove the transition (max-retries failure, abort, cleanup). */
    SYSTEM,
}
