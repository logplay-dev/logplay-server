package org.zeplinko.logplay.server.core.job

/**
 * Creates a new job in the `PENDING` state, identified by a deterministic id derived from
 * `(groupId, idempotencyKey)`.
 */
interface CreateJobUseCase {
    /**
     * @throws BlankGroupIdException, BlankJobTypeException, BlankIdempotencyKeyException for
     *   missing required fields.
     * @throws InvalidGroupIdException, InvalidJobTypeException, InvalidJobNameException,
     *   InvalidIdempotencyKeyException, InvalidMaxRetriesException for malformed values.
     * @throws DuplicateIdempotencyKeyException if a job with the same `(groupId, idempotencyKey)`
     *   already exists. Duplicates are surfaced as conflicts; the existing job is not returned.
     */
    suspend fun execute(createJobCommand: CreateJobCommand): Job
}

/**
 * Atomically transitions up to `limit` `PENDING` jobs matching `(groupId, type)` whose
 * `available_at <= now` to `ACQUIRED`, binding them to `workerId`. Uses `SELECT ... FOR UPDATE SKIP
 * LOCKED` so concurrent acquirers never block each other.
 */
interface AcquirePendingJobsUseCase {
    /**
     * @return the acquired jobs in `enqueued_at ASC` order. Empty list (not an error) if the worker
     *   is live but no matching pending jobs exist.
     * @throws WorkerNotFoundException if the worker is unknown or has timed out (must re-register).
     * @throws BlankGroupIdException, BlankJobTypeException, BlankWorkerIdException,
     *   InvalidLimitException for input validation failures.
     */
    suspend fun execute(command: AcquirePendingJobsCommand): List<Job>
}

/**
 * Appends a checkpoint to a job's chain. The new checkpoint must reference the current tail (or
 * `null` for the first). Resets the job's `retries` to 0 — saving a checkpoint counts as forward
 * progress.
 */
interface SaveJobCheckpointUseCase {
    /**
     * @throws InvalidCheckpointOrderException if `previousCheckpointId` does not match the job's
     *   current tail checkpoint id.
     * @throws JobNotFoundException, JobNotAcquiredException, JobNotOwnedByWorkerException for
     *   ownership/state violations.
     */
    suspend fun execute(command: SaveJobCheckpointCommand): Checkpoint
}

/**
 * Transitions an `ACQUIRED` job owned by `workerId` to `FINISHED`, optionally storing `outputData`.
 * Terminal — the job is no longer mutable.
 */
interface CompleteJobUseCase {
    /** @throws JobNotFoundException, JobNotAcquiredException, JobNotOwnedByWorkerException. */
    suspend fun execute(command: CompleteJobCommand): Job
}

/**
 * Returns an `ACQUIRED` job to `PENDING` so another worker can pick it up. Used for graceful
 * shutdown, voluntary handoff, and SDK-driven sleep (when an `availableAt` deadline is supplied).
 * Does not increment `retries`.
 */
interface ReleaseJobUseCase {
    /** @throws JobNotFoundException, JobNotAcquiredException, JobNotOwnedByWorkerException. */
    suspend fun execute(command: ReleaseJobCommand): Job
}

/**
 * Reads checkpoints for a job using cursor-based pagination keyed on `orderKey`. Page size is
 * bounded; the response carries `hasMore` to indicate whether further pages exist.
 */
interface GetCheckpointsUseCase {
    /**
     * @throws JobNotFoundException if the job does not exist.
     * @throws CheckpointNotFoundException if `after` references an unknown checkpoint.
     * @throws InvalidLimitException if `limit` is outside the allowed range.
     */
    suspend fun execute(command: GetCheckpointsCommand): CheckpointPage
}

/**
 * Records an execution error reported by a worker. Increments `retries` and either returns the job
 * to `PENDING` (when `retries < maxRetries`) or transitions it to terminal `FAILED` (otherwise).
 * The corresponding `ERROR_REPORTED` and (if applicable) `FAILED` events are emitted atomically
 * with the state transition.
 */
interface ReportExecutionErrorUseCase {
    /** @throws JobNotFoundException, JobNotAcquiredException, JobNotOwnedByWorkerException. */
    suspend fun execute(command: ReportExecutionErrorCommand): Job
}

/**
 * Cancels a job that is still in `PENDING` or `ACQUIRED`. Terminal jobs cannot be aborted. The
 * cancellation is system-initiated and is recorded as an `ABORTED` event.
 */
interface AbortJobUseCase {
    /**
     * @throws JobNotFoundException if the job does not exist.
     * @throws JobNotAbortableException if the job is already in a terminal state.
     */
    suspend fun execute(command: AbortJobCommand): Job
}

/** Returns the full audit timeline of state transitions for a job, ordered by `created_at`. */
interface GetJobEventsUseCase {
    /** @throws JobNotFoundException if the job does not exist. */
    suspend fun execute(jobId: String): List<JobEvent>
}
