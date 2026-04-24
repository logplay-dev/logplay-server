package org.zeplinko.logplay.server.core.worker

abstract class WorkerException(message: String, cause: Throwable? = null) :
    RuntimeException(message, cause)

class BlankWorkerIdException : WorkerException("Worker id cannot be blank")

class InvalidWorkerIdException(message: String) : WorkerException(message)

class WorkerNotFoundException(workerId: String) : WorkerException("Worker not found: $workerId")

class WorkerAlreadyRegisteredException(workerId: String) :
    WorkerException("Worker already registered: $workerId")

class InvalidWorkerTimeoutException(message: String) : WorkerException(message)

class WorkerCondemnedException(workerId: String) :
    WorkerException("Worker $workerId is condemned and pending removal")
