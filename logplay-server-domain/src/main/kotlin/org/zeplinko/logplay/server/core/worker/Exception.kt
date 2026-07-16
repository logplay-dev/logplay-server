package org.zeplinko.logplay.server.core.worker

/**
 * Base for all worker-domain errors. Mapped to HTTP status codes centrally by the app layer; the
 * domain itself remains HTTP-agnostic.
 */
abstract class WorkerException(message: String, cause: Throwable? = null) :
    RuntimeException(message, cause)

/** Required `workerId` was missing or blank. → 400 */
class BlankWorkerIdException : WorkerException("Worker id cannot be blank")

/** `workerId` violates length/charset constraints. → 400 */
class InvalidWorkerIdException(message: String) : WorkerException(message)

/** No worker exists with the given id. → 404 */
class WorkerNotFoundException(workerId: String) : WorkerException("Worker not found: $workerId")

/**
 * A worker with the same id is already registered. → 409
 *
 * The single-arg form is thrown by the gateway on a primary-key collision, which only knows the id.
 * The use case enriches it via the [alive] secondary constructor once it has read the existing
 * row's liveness, so the 409 message tells the caller whether the conflicting worker is still
 * active or has timed out and is awaiting cleanup.
 */
class WorkerAlreadyRegisteredException : WorkerException {
    constructor(workerId: String) : super("Worker already registered: $workerId")

    constructor(
        workerId: String,
        alive: Boolean,
    ) : super(
        if (alive) "Worker already registered and still active: $workerId"
        else
            "Worker already registered but timed out and pending cleanup: $workerId; " +
                "register with a new worker id"
    )
}

/** `heartbeatTimeout` or `sessionTimeout` violates required ordering or non-positivity. → 400 */
class InvalidWorkerTimeoutException(message: String) : WorkerException(message)
