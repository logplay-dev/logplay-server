package dev.logplay.server.core.job

abstract class JobException(message: String, cause: Throwable? = null) :
    RuntimeException(message, cause)

class BlankJobTypeException : JobException("Job type cannot be blank")

class BlankJobIdException : JobException("Job id cannot be blank")

class BlankCheckpointClassTypeException : JobException("Checkpoint class type cannot be blank")

class BlankCheckpointDescriptionException : JobException("Checkpoint description cannot be blank")

class JobAlreadyExistsException(cause: Throwable? = null) :
    JobException("Job already exists", cause)

class JobNotFoundException(jobId: String) : JobException("Job not found: $jobId")

class JobNotPendingException(jobId: String, status: JobStatus) :
    JobException("Job $jobId is in status $status and cannot be modified")
