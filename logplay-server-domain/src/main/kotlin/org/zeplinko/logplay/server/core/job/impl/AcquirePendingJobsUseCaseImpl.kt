package org.zeplinko.logplay.server.core.job.impl

import java.time.Instant
import java.util.*
import org.zeplinko.logplay.server.core.job.*
import org.zeplinko.logplay.server.core.worker.BlankWorkerIdException
import org.zeplinko.logplay.server.core.worker.WorkerCondemnedException
import org.zeplinko.logplay.server.core.worker.WorkerGateway
import org.zeplinko.logplay.server.core.worker.WorkerNotFoundException

class AcquirePendingJobsUseCaseImpl(
    private val jobGateway: JobGateway,
    private val workerGateway: WorkerGateway,
) : AcquirePendingJobsUseCase {

    companion object {
        const val MAX_LIMIT = 100
    }

    override suspend fun execute(command: AcquirePendingJobsCommand): List<Job> {
        if (command.groupId.isBlank()) throw BlankGroupIdException()
        if (command.type.isBlank()) throw BlankJobTypeException()
        if (command.workerId.isBlank()) throw BlankWorkerIdException()
        if (command.limit !in 1..MAX_LIMIT) throw InvalidLimitException(MAX_LIMIT)
        val now = Instant.now()
        val jobs =
            jobGateway.acquirePendingJobs(
                command.groupId,
                command.type,
                command.workerId,
                command.limit,
            ) { job ->
                JobEvent(
                    id = UUID.randomUUID().toString(),
                    jobId = job.id,
                    eventType = JobEventType.ACQUIRED,
                    actorType = ActorType.WORKER,
                    actorId = command.workerId,
                    createdAt = now,
                    eventMessage = null,
                    eventDetail = null,
                )
            }
        if (jobs.isNotEmpty()) return jobs
        val worker =
            workerGateway.findWorkerById(command.workerId)
                ?: throw WorkerNotFoundException(command.workerId)
        if (worker.condemned) throw WorkerCondemnedException(command.workerId)
        return jobs
    }
}
