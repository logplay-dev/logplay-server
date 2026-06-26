package org.zeplinko.logplay.server.core.job

import java.time.Instant

/**
 * Persistence port for jobs, checkpoints, and job events — a set of fine-grained, mostly
 * single-statement primitives the domain composes inside a `UnitOfWork` transaction. Backend
 * modules implement it with database-specific SQL; the domain layer never touches a driver
 * directly.
 *
 * **Three-table state machine.** Implementations split job state across three tables:
 * - `jobs` — authoritative metadata; touched only at creation and at terminal transitions.
 * - `job_queue` — references for `PENDING` jobs, time-gated by `available_at`.
 * - `job_acquired` — references for `ACQUIRED` jobs, bound to the owning worker.
 *
 * **Transaction model.** Methods are ambient: each runs on the connection of the enclosing
 * `UnitOfWork.transaction { }` when one is open, and auto-commits standalone otherwise. A per-job
 * state transition is orchestrated by its use case: [findAndLockJobById] takes `SELECT ... FOR
 * UPDATE` to serialise concurrent transitions for the job, the use case classifies status/ownership
 * from the returned [Job] (throwing the appropriate domain exception), then composes the mechanical
 * writes ([removeFromQueue]/[removeFromAcquired], [enqueue]/[markTerminal], [insertCheckpoint],
 * [insertEvents], ...) — all under that lock, in one transaction. A thrown domain exception rolls
 * the transaction back.
 *
 * **Set-based operations.** [acquirePendingJobs] and [releaseJobsByWorkerIds] move rows in bulk
 * (`SELECT ... FOR UPDATE SKIP LOCKED` / batched `DELETE`+`INSERT`); they carry no business logic,
 * and the use case builds any resulting events via [insertEvents] in the same transaction.
 *
 * **Constraint-backed guards.** [insertJob] and [insertCheckpoint] surface unique-constraint
 * collisions as [DuplicateJobIdException] / [InvalidCheckpointOrderException]; the use case maps or
 * pre-validates as needed.
 */
interface JobGateway {
    /**
     * Atomically claims up to `limit` `PENDING` jobs matching `(groupId, type)` whose `available_at
     * <= now`, transitioning them to `ACQUIRED` and binding them to `workerId`, and returns the
     * acquired jobs. Implementations use `SELECT ... FOR UPDATE SKIP LOCKED` (or equivalent)
     * ordered by `enqueued_at ASC`. Enlists in the caller's transaction; the use case builds and
     * persists the `ACQUIRED` events via [insertEvents] within the same transaction.
     */
    suspend fun acquirePendingJobs(
        groupId: String,
        type: String,
        workerId: String,
        limit: Int,
    ): List<Job>

    /**
     * Looks up a job by primary key. Callers that have a `(groupId, idempotencyKey)` pair should
     * derive the id via [JobIdGenerator.fromIdempotencyKey] and call this method — the idempotency
     * key is not stored, so there is no server-side lookup by key.
     */
    suspend fun findJobById(id: String): Job?

    /**
     * Locks the job row (`SELECT ... FOR UPDATE`) and returns the assembled [Job], or `null` if no
     * job with [id] exists. Must be called inside a `UnitOfWork` transaction; the lock is held for
     * the rest of that transaction, serialising concurrent state transitions for the same job. The
     * use case classifies status/ownership from the returned [Job] (or throws on `null`).
     */
    suspend fun findAndLockJobById(id: String): Job?

    /** Looks up a checkpoint by primary key. */
    suspend fun findCheckpointById(id: String): Checkpoint?

    /**
     * Returns the tail (highest `orderKey`) checkpoint of the job's chain, or `null` if it has
     * none. Intended to run inside a transaction after [findAndLockJobById], so it is serialised by
     * the job lock and takes no lock of its own.
     */
    suspend fun latestCheckpoint(jobId: String): Checkpoint?

    /**
     * Returns up to `limit` checkpoints for the job, ordered by `orderKey ASC`. When
     * `afterOrderKey` is non-null, only checkpoints strictly after that key are returned.
     */
    suspend fun findCheckpointsByJobId(
        jobId: String,
        afterOrderKey: Long?,
        limit: Int,
    ): List<Checkpoint>

    /**
     * Lambda-free, ambient variant of [releaseJobsByWorkerIds] returning the released job ids. The
     * use case builds and persists the `RELEASED` events via [insertEvents] in the same
     * transaction.
     */
    suspend fun releaseJobsByWorkerIds(workerIds: List<String>, updatedAt: Instant): List<String>

    /**
     * Inserts the `jobs` row and its `job_queue` entry (PENDING, `retries = 0`, `available_at =
     * 0`).
     *
     * @throws DuplicateJobIdException on a primary-key collision; the use case maps this to its
     *   richer idempotency-key conflict.
     */
    suspend fun insertJob(newJob: NewJob)

    /** Appends audit events in a single batch. No-op for an empty list. */
    suspend fun insertEvents(events: List<JobEvent>)

    /** Deletes the job's `job_queue` row, if present. */
    suspend fun removeFromQueue(jobId: String)

    /**
     * Deletes the job's `job_acquired` row, if present. Ownership is the caller's concern — it is
     * verified under [findAndLockJobById] before this is called, so no worker filter is applied
     * here.
     */
    suspend fun removeFromAcquired(jobId: String)

    /** Inserts a `job_queue` row with the given retry count and `available_at` floor. */
    suspend fun enqueue(
        jobId: String,
        groupId: String,
        type: String,
        enqueuedAt: Instant,
        retries: Int,
        availableAt: Long,
    )

    /**
     * Writes the terminal transition on `jobs` (`terminal_status`, `terminal_at`, and `output_data`
     * — pass `null` for non-`FINISHED` transitions). Does not touch the secondary tables; the
     * caller removes the `job_queue`/`job_acquired` row first.
     */
    suspend fun markTerminal(
        jobId: String,
        status: JobStatus,
        terminalAt: Instant,
        outputData: ByteArray?,
    )

    /**
     * Inserts a checkpoint into the job's chain.
     *
     * @throws InvalidCheckpointOrderException on a chain-constraint collision — a concurrency
     *   backstop; the use case validates chain order against [latestCheckpoint] first.
     */
    suspend fun insertCheckpoint(checkpoint: Checkpoint)

    /**
     * Resets the acquired job's `retries` to 0 — saving a checkpoint counts as forward progress.
     */
    suspend fun resetAcquiredRetries(jobId: String)

    /** Returns all events for a job, ordered by `created_at ASC`. */
    suspend fun findEventsByJobId(jobId: String): List<JobEvent>
}
