package org.zeplinko.logplay.server.core.job

import java.time.Instant

/**
 * Persistence port for jobs, checkpoints, and job events. Backend modules implement this interface
 * with database-specific transactional and concurrency primitives. The domain layer never touches a
 * driver directly.
 *
 * **Three-table state machine.** Implementations split job state across three tables:
 * - `jobs` — authoritative metadata; touched only at creation and at terminal transitions.
 * - `job_queue` — references for `PENDING` jobs, time-gated by `available_at`.
 * - `job_acquired` — references for `ACQUIRED` jobs, bound to the owning worker.
 *
 * Every state transition (acquire, release, complete, abort, retryable error, exhausted error) is a
 * single transaction that opens with `SELECT 1 FROM jobs WHERE id = ? FOR UPDATE` to serialise
 * concurrent transitions for the same job, then performs rowcount-guarded `DELETE`+`INSERT` pairs
 * across the secondary tables, and finally writes the audit event.
 *
 * **Compute pattern.** [saveCheckpoint] and [reportExecutionError] accept a `compute` lambda that
 * runs inside the transaction with the locked job (and, where applicable, the latest checkpoint)
 * already loaded. This keeps domain logic — id derivation, retry math, event creation — free of any
 * driver knowledge while still letting the gateway own locking semantics.
 *
 * **Typed transition results.** State-transition methods ([abortJob], [completeJob], [releaseJob],
 * [reportExecutionError], [saveCheckpoint]) return a sealed result type rather than a nullable
 * value. Each variant carries the precise reason classified *under the same row lock* that gated
 * the transition (job missing, already terminal, wrong status, wrong worker). The use-case layer
 * pattern-matches the result into the appropriate domain exception without a second lookup, so the
 * reported failure reason is never racy with respect to subsequent state changes.
 */
interface JobGateway {
    /**
     * Atomically claims up to `limit` `PENDING` jobs matching `(groupId, type)` whose `available_at
     * <= now`, transitioning them to `ACQUIRED` and binding them to `workerId`. Implementations
     * must use `SELECT ... FOR UPDATE SKIP LOCKED` (or equivalent) ordered by `enqueued_at ASC`.
     *
     * @param eventFactory optional factory invoked once per acquired job; the resulting events are
     *   inserted in the same transaction as the move from `job_queue` to `job_acquired`.
     */
    suspend fun acquirePendingJobs(
        groupId: String,
        type: String,
        workerId: String,
        limit: Int,
        eventFactory: ((Job) -> JobEvent)? = null,
    ): List<Job>

    /**
     * Looks up a job by primary key. Callers that have a `(groupId, idempotencyKey)` pair should
     * derive the id via [JobIdGenerator.fromIdempotencyKey] and call this method — the idempotency
     * key is not stored, so there is no server-side lookup by key.
     */
    suspend fun findJobById(id: String): Job?

    /**
     * Appends a checkpoint to the job's chain inside a single transaction. The implementation locks
     * the job row (`SELECT ... FOR UPDATE`), verifies ownership via `job_acquired`, loads the
     * latest checkpoint, calls `compute` to construct the new checkpoint from the latest tail,
     * inserts it, and resets `retries` to 0 on the corresponding `job_acquired` row.
     *
     * @return a [SaveCheckpointResult] classifying success or the precondition failure.
     * @throws InvalidCheckpointOrderException when the checkpoint chain unique constraint is
     *   violated (e.g. concurrent writers targeting the same chain position).
     */
    suspend fun saveCheckpoint(
        jobId: String,
        workerId: String,
        updatedAt: Instant,
        compute: (Checkpoint?) -> Checkpoint,
    ): SaveCheckpointResult

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
     * On `PENDING` (retryable), the row is moved from `job_acquired` back into `job_queue` with
     * `available_at = 0` and the new retry count. On `FAILED`, the row is removed from
     * `job_acquired` and `jobs.terminal_status` is set.
     *
     * @return a [ReportExecutionErrorResult] classifying success or the precondition failure.
     */
    suspend fun reportExecutionError(
        jobId: String,
        workerId: String,
        updatedAt: Instant,
        compute: (Job) -> ExecutionErrorResult,
    ): ReportExecutionErrorResult

    /**
     * Transitions an `ACQUIRED` job to `FINISHED`, optionally storing `outputData`, and emits the
     * `COMPLETED` event in the same transaction. Removes the row from `job_acquired` and writes
     * `jobs.terminal_status = 'FINISHED'`.
     *
     * @return a [CompleteJobResult] classifying success or the precondition failure.
     */
    suspend fun completeJob(
        jobId: String,
        workerId: String,
        outputData: ByteArray?,
        updatedAt: Instant,
        event: JobEvent,
    ): CompleteJobResult

    /**
     * Transitions a `PENDING` or `ACQUIRED` job to `ABORTED`, removing it from whichever secondary
     * table it currently lives in, and emits the `ABORTED` event in the same transaction.
     *
     * @return an [AbortJobResult] classifying success or the precondition failure.
     */
    suspend fun abortJob(jobId: String, updatedAt: Instant, event: JobEvent): AbortJobResult

    /**
     * Returns an `ACQUIRED` job to `PENDING`, optionally with a re-acquisition deadline. The row is
     * moved from `job_acquired` to `job_queue` with `available_at = availableAt` (or `0` when
     * `availableAt` is `null` or `0`). Acquisition will skip the row until `now >= available_at`.
     *
     * @param availableAt epoch-millis floor for re-acquisition. `null` is treated as `0` (no
     *   deadline). Used by SDK sleep to defer re-acquisition until the wake-at moment.
     * @return a [ReleaseJobResult] classifying success or the precondition failure.
     */
    suspend fun releaseJob(
        jobId: String,
        workerId: String,
        updatedAt: Instant,
        availableAt: Long?,
        event: JobEvent,
    ): ReleaseJobResult

    /**
     * Bulk-releases every `ACQUIRED` job currently owned by `workerId` back to `PENDING`. The
     * deadline is **not** preserved — every released row is written with `available_at = 0`. Used
     * by the dead-worker cleanup path; the SDK's replay flow re-derives any sleep deadline from the
     * checkpoint chain on the next acquire. `eventFactory` is invoked once per released job and the
     * resulting events are persisted in the same transaction as the move from `job_acquired` back
     * into `job_queue`.
     *
     * @return the number of jobs released.
     */
    suspend fun releaseJobsByWorkerId(
        workerId: String,
        updatedAt: Instant,
        eventFactory: (jobId: String) -> JobEvent,
    ): Int

    /**
     * Bulk-releases every `ACQUIRED` job owned by any of the given workers. Same `available_at = 0`
     * semantics as [releaseJobsByWorkerId]. Used to release jobs for a batch of condemned workers
     * in one round-trip. `eventFactory` is invoked once per released job and the resulting events
     * are persisted in the same transaction.
     *
     * @return the total number of jobs released.
     */
    suspend fun releaseJobsByWorkerIds(
        workerIds: List<String>,
        updatedAt: Instant,
        eventFactory: (jobId: String) -> JobEvent,
    ): Int

    /**
     * Inserts a new job and its first event (typically `CREATED`) inside a single transaction so
     * the audit log is never missing the bookend. Also inserts the matching `job_queue` row with
     * `available_at = 0`.
     *
     * @return an [InsertJobWithEventResult] classifying success or a duplicate-id collision.
     */
    suspend fun insertJobWithEvent(newJob: NewJob, event: JobEvent): InsertJobWithEventResult

    /** Returns all events for a job, ordered by `created_at ASC`. */
    suspend fun findEventsByJobId(jobId: String): List<JobEvent>
}

/** Outcome of [JobGateway.abortJob]. */
sealed interface AbortJobResult {
    data class Success(val job: Job) : AbortJobResult

    object NotFound : AbortJobResult

    data class AlreadyTerminal(val status: JobStatus) : AbortJobResult
}

/** Outcome of [JobGateway.completeJob]. */
sealed interface CompleteJobResult {
    data class Success(val job: Job) : CompleteJobResult

    object NotFound : CompleteJobResult

    data class WrongStatus(val status: JobStatus) : CompleteJobResult

    data class WrongWorker(val acquiredBy: String) : CompleteJobResult
}

/** Outcome of [JobGateway.releaseJob]. */
sealed interface ReleaseJobResult {
    data class Success(val job: Job) : ReleaseJobResult

    object NotFound : ReleaseJobResult

    data class WrongStatus(val status: JobStatus) : ReleaseJobResult

    data class WrongWorker(val acquiredBy: String) : ReleaseJobResult
}

/** Outcome of [JobGateway.reportExecutionError]. */
sealed interface ReportExecutionErrorResult {
    data class Success(val job: Job) : ReportExecutionErrorResult

    object NotFound : ReportExecutionErrorResult

    data class WrongStatus(val status: JobStatus) : ReportExecutionErrorResult

    data class WrongWorker(val acquiredBy: String) : ReportExecutionErrorResult
}

/** Outcome of [JobGateway.insertJobWithEvent]. */
sealed interface InsertJobWithEventResult {
    data class Success(val job: Job) : InsertJobWithEventResult

    object AlreadyExists : InsertJobWithEventResult
}

/** Outcome of [JobGateway.saveCheckpoint]. */
sealed interface SaveCheckpointResult {
    data class Success(val checkpoint: Checkpoint) : SaveCheckpointResult

    object NotFound : SaveCheckpointResult

    data class WrongStatus(val status: JobStatus) : SaveCheckpointResult

    data class WrongWorker(val acquiredBy: String) : SaveCheckpointResult
}
