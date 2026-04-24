package org.zeplinko.logplay.server.core.worker.impl

import java.time.Instant
import org.zeplinko.logplay.server.core.worker.*

class HeartbeatWorkerUseCaseImpl(private val workerGateway: WorkerGateway) :
    HeartbeatWorkerUseCase {

    override suspend fun execute(command: HeartbeatWorkerCommand): Worker {
        if (command.workerId.isBlank()) throw BlankWorkerIdException()
        val now = Instant.now()
        val updated = workerGateway.updateWorkerHeartbeat(command.workerId, now)
        if (updated != null) return updated
        val worker = workerGateway.findWorkerById(command.workerId)
        if (worker != null && worker.condemned) throw WorkerCondemnedException(command.workerId)
        throw WorkerNotFoundException(command.workerId)
    }
}
