package org.zeplinko.logplay.server.core.worker.impl

import java.time.Instant
import org.zeplinko.logplay.server.core.worker.*

class HeartbeatWorkerUseCaseImpl(private val workerGateway: WorkerGateway) :
    HeartbeatWorkerUseCase {

    override suspend fun execute(command: HeartbeatWorkerCommand): Worker {
        if (command.workerId.isBlank()) throw BlankWorkerIdException()
        val now = Instant.now()
        // Liveness-gated: a missing or timed-out worker yields null and must re-register.
        return workerGateway.updateWorkerHeartbeat(command.workerId, now)
            ?: throw WorkerNotFoundException(command.workerId)
    }
}
