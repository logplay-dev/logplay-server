package org.zeplinko.logplay.server.core.worker.impl

import java.time.Instant
import org.zeplinko.logplay.server.core.worker.*

class RegisterWorkerUseCaseImpl(private val workerGateway: WorkerGateway) : RegisterWorkerUseCase {

    companion object {
        const val MAX_WORKER_ID_LENGTH = 64
    }

    override suspend fun execute(command: RegisterWorkerCommand): Worker {
        validate(command)
        val existing = workerGateway.findWorkerById(command.workerId)
        if (existing != null) throw WorkerAlreadyRegisteredException(command.workerId)

        val now = Instant.now()
        val worker =
            Worker(
                id = command.workerId,
                heartbeatTimeout = command.heartbeatTimeout,
                sessionTimeout = command.sessionTimeout,
                lastHeartbeatAt = now,
                registeredAt = now,
            )
        return workerGateway.insertWorker(worker)
    }

    private fun validate(command: RegisterWorkerCommand) {
        if (command.workerId.isBlank()) throw BlankWorkerIdException()
        if (command.workerId.length > MAX_WORKER_ID_LENGTH)
            throw InvalidWorkerIdException(
                "workerId must not exceed $MAX_WORKER_ID_LENGTH characters"
            )
        if (command.heartbeatTimeout <= 0)
            throw InvalidWorkerTimeoutException("heartbeatTimeout must be positive")
        if (command.sessionTimeout <= 0)
            throw InvalidWorkerTimeoutException("sessionTimeout must be positive")
        if (command.sessionTimeout <= command.heartbeatTimeout)
            throw InvalidWorkerTimeoutException(
                "sessionTimeout must be greater than heartbeatTimeout"
            )
    }
}
