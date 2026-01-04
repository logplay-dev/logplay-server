package dev.logplay.server.core.job

open class JobException(override val message: String, override val cause: Throwable?) :
    RuntimeException(message, cause)

private const val JOB_TYPE_CANNOT_BE_BLANK = "Job type cannot be blank"

class BlankJobTypeException() : JobException(JOB_TYPE_CANNOT_BE_BLANK, null)

private const val JOB_ALREADY_EXISTS = "Job already exists"

class JobAlreadyExistsException(override val cause: Throwable?) :
    JobException(JOB_ALREADY_EXISTS, cause)
