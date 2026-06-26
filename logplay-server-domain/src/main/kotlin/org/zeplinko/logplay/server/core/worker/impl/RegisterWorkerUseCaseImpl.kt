package org.zeplinko.logplay.server.core.worker.impl

import java.time.Instant
import org.zeplinko.logplay.server.core.UnitOfWork
import org.zeplinko.logplay.server.core.worker.*

class RegisterWorkerUseCaseImpl(
    private val workerGateway: WorkerGateway,
    private val unitOfWork: UnitOfWork,
) : RegisterWorkerUseCase {

    companion object {
        const val MAX_WORKER_ID_LENGTH = 64
    }

    override suspend fun execute(command: RegisterWorkerCommand): Worker {
        validate(command)
        val now = Instant.now()
        val worker =
            Worker(
                id = command.workerId,
                heartbeatTimeout = command.heartbeatTimeout,
                sessionTimeout = command.sessionTimeout,
                lastHeartbeatAt = now,
                registeredAt = now,
            )
        // The unique constraint on workers(id) is the authoritative duplicate guard:
        // insertWorker translates a PK collision into WorkerAlreadyRegisteredException, so no
        // (racy) pre-check is needed.
        return try {
            workerGateway.insertWorker(worker)
        } catch (e: WorkerAlreadyRegisteredException) {
            // Enrich the conflict with the colliding worker's liveness so the 409 tells the caller
            // whether it clashed with an active worker or one that has timed out and is awaiting
            // cleanup. If the row was reaped between the failed insert and this read, the conflict
            // is gone — surface the original (generic) exception rather than guessing.
            val existing =
                unitOfWork.transaction { workerGateway.findAndLockWorkerById(command.workerId) }
                    ?: throw e
            throw WorkerAlreadyRegisteredException(
                command.workerId,
                alive = !existing.isDeadAt(now),
            )
        }
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
