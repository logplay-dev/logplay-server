package org.zeplinko.logplay.server.core.job

abstract class JobException(message: String, cause: Throwable? = null) :
    RuntimeException(message, cause)

class BlankGroupIdException : JobException("Group id cannot be blank")

class InvalidGroupIdException(message: String) : JobException(message)

class BlankJobTypeException : JobException("Job type cannot be blank")

class InvalidJobTypeException(message: String) : JobException(message)

class InvalidJobNameException(message: String) : JobException(message)

class BlankJobIdException : JobException("Job id cannot be blank")

class InvalidCheckpointNameException(message: String) : JobException(message)

class DuplicateIdempotencyKeyException(groupId: String, idempotencyKey: String) :
    JobException("Job with idempotency key '$idempotencyKey' already exists in group '$groupId'")

class BlankIdempotencyKeyException : JobException("Idempotency key cannot be blank")

class InvalidIdempotencyKeyException(message: String) : JobException(message)

class JobNotFoundException(jobId: String) : JobException("Job not found: $jobId")

class JobNotAcquiredException(jobId: String, status: JobStatus) :
    JobException("Job $jobId is in status $status and is not acquired")

class JobConcurrentModificationException(jobId: String) :
    JobException("Job $jobId was modified concurrently")

class CheckpointNotFoundException(checkpointId: String) :
    JobException("Checkpoint not found: $checkpointId")

class InvalidCheckpointOrderException(jobId: String, cause: Throwable? = null) :
    JobException(
        "Checkpoint order mismatch for job $jobId: provided previous checkpoint ID does not match the last checkpoint",
        cause,
    )

class InvalidMaxRetriesException : JobException("maxRetries must be a positive number")

class InvalidCheckpointDataException :
    JobException("Checkpoint data is not valid Base64-encoded content")

class InvalidJobInputDataException :
    JobException("Job input data is not valid Base64-encoded content")

class InvalidJobOutputDataException :
    JobException("Job output data is not valid Base64-encoded content")

class InvalidLimitException(max: Int) : JobException("limit must be between 1 and $max")

class JobNotAbortableException(jobId: String, status: JobStatus) :
    JobException("Job $jobId is in status $status and cannot be aborted")

class JobNotOwnedByWorkerException(jobId: String, workerId: String) :
    JobException("Job $jobId is not owned by worker $workerId")
