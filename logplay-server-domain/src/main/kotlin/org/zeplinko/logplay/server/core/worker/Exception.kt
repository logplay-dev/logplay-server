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

/** A worker with the same id is already registered. → 409 */
class WorkerAlreadyRegisteredException(workerId: String) :
    WorkerException("Worker already registered: $workerId")

/** `heartbeatTimeout` or `sessionTimeout` violates required ordering or non-positivity. → 400 */
class InvalidWorkerTimeoutException(message: String) : WorkerException(message)

/**
 * The worker has been condemned (declared dead) by the cleanup pass. Heartbeats and acquisitions
 * are rejected; the row is awaiting deletion in phase 2 of cleanup. → 403
 */
class WorkerCondemnedException(workerId: String) :
    WorkerException("Worker $workerId is condemned and pending removal")
