package org.zeplinko.logplay.server.core.job

/**
 * Base for all job-domain errors. Concrete subclasses are mapped to specific HTTP status codes by
 * the app layer's central error handler; the domain itself is HTTP-agnostic.
 */
abstract class JobException(message: String, cause: Throwable? = null) :
    RuntimeException(message, cause)

/** Required `groupId` was missing or blank. → 400 */
class BlankGroupIdException : JobException("Group id cannot be blank")

/** `groupId` violates length/charset constraints. → 400 */
class InvalidGroupIdException(message: String) : JobException(message)

/** Required `type` was missing or blank. → 400 */
class BlankJobTypeException : JobException("Job type cannot be blank")

/** `type` violates length/charset constraints. → 400 */
class InvalidJobTypeException(message: String) : JobException(message)

/** `name` violates length constraints. → 400 */
class InvalidJobNameException(message: String) : JobException(message)

/** Required `jobId` was missing or blank. → 400 */
class BlankJobIdException : JobException("Job id cannot be blank")

/** Checkpoint `name` is blank or exceeds the length limit. → 400 */
class InvalidCheckpointNameException(message: String) : JobException(message)

/**
 * A job with the same `(groupId, idempotencyKey)` already exists. The server does not return the
 * existing job — duplicates are surfaced as conflicts so callers see any drift in their request
 * payload. → 409
 */
class DuplicateIdempotencyKeyException(groupId: String, idempotencyKey: String) :
    JobException("Job with idempotency key '$idempotencyKey' already exists in group '$groupId'")

/**
 * A job row with the same primary key (the deterministic id derived from `(groupId,
 * idempotencyKey)`) already exists. Raised by the `insertJob` gateway primitive on a primary-key
 * collision — the gateway knows only the id, not the originating idempotency key.
 * [CreateJobUseCase] catches this inside its transaction and rethrows the richer
 * [DuplicateIdempotencyKeyException] (which carries the group and key it has on hand), so this type
 * is an internal gateway→use-case signal and is not normally surfaced to the HTTP layer.
 */
class DuplicateJobIdException(jobId: String, cause: Throwable? = null) :
    JobException("Job with id '$jobId' already exists", cause)

/** Required `idempotencyKey` was missing or blank. → 400 */
class BlankIdempotencyKeyException : JobException("Idempotency key cannot be blank")

/** `idempotencyKey` violates length/charset constraints. → 400 */
class InvalidIdempotencyKeyException(message: String) : JobException(message)

/** No job exists with the given id. → 404 */
class JobNotFoundException(jobId: String) : JobException("Job not found: $jobId")

/** Operation requires `ACQUIRED` status; the job is in some other state. → 409 */
class JobNotAcquiredException(jobId: String, val status: JobStatus) :
    JobException("Job $jobId is in status $status and is not acquired")

/** No checkpoint exists with the given id (typically used for invalid `after` cursors). → 404 */
class CheckpointNotFoundException(checkpointId: String) :
    JobException("Checkpoint not found: $checkpointId")

/**
 * Either `previousCheckpointId` does not match the current tail of the chain, or two writers raced
 * to claim the same chain position. → 409
 */
class InvalidCheckpointOrderException(jobId: String, cause: Throwable? = null) :
    JobException(
        "Checkpoint order mismatch for job $jobId: provided previous checkpoint ID does not match the last checkpoint",
        cause,
    )

/** `maxRetries` was non-positive. → 400 */
class InvalidMaxRetriesException : JobException("maxRetries must be a positive number")

/** Provided `data` field is not valid Base64. → 400 */
class InvalidCheckpointDataException :
    JobException("Checkpoint data is not valid Base64-encoded content")

/** Provided `inputData` is not valid Base64. → 400 */
class InvalidJobInputDataException :
    JobException("Job input data is not valid Base64-encoded content")

/** Provided `outputData` is not valid Base64. → 400 */
class InvalidJobOutputDataException :
    JobException("Job output data is not valid Base64-encoded content")

/** Pagination `limit` outside the allowed range `[1, max]`. → 400 */
class InvalidLimitException(max: Int) : JobException("limit must be between 1 and $max")

/** Job is already terminal (`FINISHED`, `FAILED`, `ABORTED`) and cannot be aborted. → 409 */
class JobNotAbortableException(jobId: String, val status: JobStatus) :
    JobException("Job $jobId is in status $status and cannot be aborted")

/** Acquired job is owned by a different worker than the one making the request. → 409 */
class JobNotOwnedByWorkerException(jobId: String, workerId: String) :
    JobException("Job $jobId is not owned by worker $workerId")
