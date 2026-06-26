package org.zeplinko.logplay.server.core.worker.impl

import java.time.Instant
import org.zeplinko.logplay.server.core.UnitOfWork
import org.zeplinko.logplay.server.core.job.JobEvent
import org.zeplinko.logplay.server.core.job.JobGateway
import org.zeplinko.logplay.server.core.worker.*

class DeregisterWorkerUseCaseImpl(
    private val workerGateway: WorkerGateway,
    private val jobGateway: JobGateway,
    private val unitOfWork: UnitOfWork,
) : DeregisterWorkerUseCase {

    override suspend fun execute(command: DeregisterWorkerCommand) {
        if (command.workerId.isBlank()) throw BlankWorkerIdException()
        val now = Instant.now()
        unitOfWork.transaction {
            // Lock the worker row first so concurrent lifecycle ops on this worker (another
            // deregister, or cleanup eviction) serialise here rather than deadlocking on the
            // worker-row vs jobs-row lock order.
            workerGateway.findAndLockWorkerById(command.workerId)
                ?: throw WorkerNotFoundException(command.workerId)
            val releasedIds = jobGateway.releaseJobsByWorkerIds(listOf(command.workerId), now)
            jobGateway.insertEvents(
                releasedIds.map { jobId ->
                    JobEvent.released(jobId, now, "released by worker deregistration")
                }
            )
            workerGateway.deleteWorker(command.workerId)
        }
    }
}
