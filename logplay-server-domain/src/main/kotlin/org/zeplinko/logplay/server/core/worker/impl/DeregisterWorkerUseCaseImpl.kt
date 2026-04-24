package org.zeplinko.logplay.server.core.worker.impl

import java.time.Instant
import org.zeplinko.logplay.server.core.job.JobGateway
import org.zeplinko.logplay.server.core.worker.*

class DeregisterWorkerUseCaseImpl(
    private val workerGateway: WorkerGateway,
    private val jobGateway: JobGateway,
) : DeregisterWorkerUseCase {

    override suspend fun execute(command: DeregisterWorkerCommand) {
        if (command.workerId.isBlank()) throw BlankWorkerIdException()
        workerGateway.findWorkerById(command.workerId)
            ?: throw WorkerNotFoundException(command.workerId)
        workerGateway.condemnWorker(command.workerId)
        jobGateway.releaseJobsByWorkerId(command.workerId, Instant.now())
        workerGateway.deleteWorker(command.workerId)
    }
}
