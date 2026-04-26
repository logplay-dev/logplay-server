package org.zeplinko.logplay.server.core.job

import java.time.Instant

/**
 * Persistence port for jobs, checkpoints, and job events. Backend modules implement this interface
 * with database-specific transactional and concurrency primitives. The domain layer never touches a
 * driver directly.
 *
 * **Transactional contract.** Methods that mutate multiple rows (saving a checkpoint, reporting an
 * error, completing/releasing/aborting a job) are required to execute as a single transaction with
 * appropriate row-level locking on the affected job. Implementations should map known database
 * constraint violations to domain exceptions (e.g. duplicate idempotency key →
 * [DuplicateIdempotencyKeyException], checkpoint chain conflict →
 * [InvalidCheckpointOrderException]).
 *
 * **Compute pattern.** [saveCheckpoint] and [reportExecutionError] accept a `compute` lambda that
 * runs inside the transaction with the locked job (and, where applicable, the latest checkpoint)
 * already loaded. This keeps domain logic — id derivation, retry math, event creation — free of any
 * driver knowledge while still letting the gateway own locking semantics.
 *
 * **Optimistic locking.** Mutating operations bump `Job.version`; if the underlying conditional
 * update affects zero rows, implementations must throw [JobConcurrentModificationException].
 *
 * **Null returns.** Where a method returns `Job?` or `Checkpoint?`, `null` means "the precondition
 * the caller relied on was not met" (e.g. the job does not exist, is not `ACQUIRED`, or is owned by
 * a different worker). The use-case layer turns that nullity into the appropriate domain exception
 * after re-reading the job to determine the precise reason.
 */
interface JobGateway {
    /**
     * Inserts a new job. The id is expected to already be derived deterministically from `(groupId,
     * idempotencyKey)`.
     *
     * @throws DuplicateIdempotencyKeyException on primary-key collision.
     */
    suspend fun insertJob(job: Job): Job

    /**
     * Atomically claims up to `limit` `PENDING` jobs matching `(groupId, type)`, transitioning them
     * to `ACQUIRED` and binding them to `workerId`. Implementations must use `SELECT ... FOR UPDATE
     * SKIP LOCKED` (or equivalent) ordered by `updated_at ASC`.
     *
     * @param eventFactory optional factory invoked once per acquired job; the resulting events are
     *   inserted in the same transaction as the status change.
     */
    suspend fun acquirePendingJobs(
        groupId: String,
        type: String,
        workerId: String,
        limit: Int,
        eventFactory: ((Job) -> JobEvent)? = null,
    ): List<Job>

    /** Looks up a job by primary key. */
    suspend fun findJobById(id: String): Job?

    /**
     * Looks up a job by `(groupId, idempotencyKey)`. Useful for diagnostics; callers should
     * generally rely on the deterministic id instead.
     */
    suspend fun findJobByIdempotencyKey(groupId: String, idempotencyKey: String): Job?

    /**
     * Appends a checkpoint to the job's chain inside a single transaction. The implementation locks
     * the job row (`SELECT ... FOR UPDATE`), loads the latest checkpoint, calls `compute` to
     * construct the new checkpoint from the locked state, inserts it, and resets `retries` to 0
     * with a `version` bump on the job.
     *
     * @return the persisted checkpoint, or `null` if the job is missing/not `ACQUIRED`/not owned by
     *   `workerId`.
     * @throws InvalidCheckpointOrderException when the checkpoint chain unique constraint is
     *   violated (e.g. concurrent writers targeting the same chain position).
     * @throws JobConcurrentModificationException on version mismatch.
     */
    suspend fun saveCheckpoint(
        jobId: String,
        workerId: String,
        updatedAt: Instant,
        compute: (Job, Checkpoint?) -> Checkpoint,
    ): Checkpoint?

    /** Looks up a checkpoint by primary key. */
    suspend fun findCheckpointById(id: String): Checkpoint?

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
     * Records an execution error inside a single transaction. The `compute` lambda is invoked with
     * the locked job and returns the new status, retry count, and the events to emit (typically
     * `ERROR_REPORTED`, plus `FAILED` if retries are exhausted).
     *
     * @return the updated job, or `null` if the job is missing/not `ACQUIRED`/not owned by
     *   `workerId`.
     * @throws JobConcurrentModificationException on version mismatch.
     */
    suspend fun reportExecutionError(
        jobId: String,
        workerId: String,
        updatedAt: Instant,
        compute: (Job) -> ExecutionErrorResult,
    ): Job?

    /**
     * Transitions an `ACQUIRED` job to `FINISHED`, optionally storing `outputData`, and emits the
     * `COMPLETED` event in the same transaction.
     *
     * @return the updated job, or `null` on ownership/state mismatch.
     */
    suspend fun completeJob(
        jobId: String,
        workerId: String,
        outputData: ByteArray?,
        updatedAt: Instant,
        event: JobEvent,
    ): Job?

    /**
     * Transitions a `PENDING` or `ACQUIRED` job to `ABORTED`, clearing its worker binding, and
     * emits the `ABORTED` event in the same transaction.
     *
     * @return the updated job, or `null` if the job is missing or already terminal.
     */
    suspend fun abortJob(jobId: String, updatedAt: Instant, event: JobEvent): Job?

    /**
     * Returns an `ACQUIRED` job to `PENDING`, clearing its worker binding, and emits the `RELEASED`
     * event in the same transaction.
     *
     * @return the updated job, or `null` on ownership/state mismatch.
     */
    suspend fun releaseJob(
        jobId: String,
        workerId: String,
        updatedAt: Instant,
        event: JobEvent,
    ): Job?

    /**
     * Bulk-releases every `ACQUIRED` job currently owned by `workerId` back to `PENDING`. Used by
     * the dead-worker cleanup path.
     *
     * @return the number of jobs released.
     */
    suspend fun releaseJobsByWorkerId(workerId: String, updatedAt: Instant): Int

    /**
     * Bulk-releases every `ACQUIRED` job owned by any of the given workers. Used to release jobs
     * for a batch of condemned workers in one round-trip.
     *
     * @return the total number of jobs released.
     */
    suspend fun releaseJobsByWorkerIds(workerIds: List<String>, updatedAt: Instant): Int

    /**
     * Inserts a new job and its first event (typically `CREATED`) inside a single transaction so
     * the audit log is never missing the bookend.
     */
    suspend fun insertJobWithEvent(job: Job, event: JobEvent): Job

    /** Returns all events for a job, ordered by `created_at ASC`. */
    suspend fun findEventsByJobId(jobId: String): List<JobEvent>
}
