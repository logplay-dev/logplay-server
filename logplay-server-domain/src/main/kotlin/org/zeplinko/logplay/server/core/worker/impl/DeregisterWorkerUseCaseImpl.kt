package org.zeplinko.logplay.server.core.worker.impl

import java.time.Instant
import java.util.UUID
import org.zeplinko.logplay.server.core.job.ActorType
import org.zeplinko.logplay.server.core.job.JobEvent
import org.zeplinko.logplay.server.core.job.JobEventType
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
        val now = Instant.now()
        jobGateway.releaseJobsByWorkerId(command.workerId, now) { jobId ->
            JobEvent(
                id = UUID.randomUUID().toString(),
                jobId = jobId,
                eventType = JobEventType.RELEASED,
                actorType = ActorType.SYSTEM,
                actorId = null,
                createdAt = now,
                eventMessage = "released by worker deregistration",
                eventDetail = null,
            )
        }
        workerGateway.deleteWorker(command.workerId)
    }
}
