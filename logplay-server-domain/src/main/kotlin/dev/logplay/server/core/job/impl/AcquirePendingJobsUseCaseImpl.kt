package dev.logplay.server.core.job.impl

import dev.logplay.server.core.job.*
import dev.logplay.server.core.worker.BlankWorkerIdException
import dev.logplay.server.core.worker.WorkerCondemnedException
import dev.logplay.server.core.worker.WorkerGateway
import dev.logplay.server.core.worker.WorkerNotFoundException
import java.time.Instant
import java.util.*

class AcquirePendingJobsUseCaseImpl(
    private val jobGateway: JobGateway,
    private val workerGateway: WorkerGateway,
) : AcquirePendingJobsUseCase {

    companion object {
        const val MAX_LIMIT = 100
    }

    override suspend fun execute(command: AcquirePendingJobsCommand): List<Job> {
        if (command.workerId.isBlank()) throw BlankWorkerIdException()
        if (command.limit !in 1..MAX_LIMIT) throw InvalidLimitException(MAX_LIMIT)
        val now = Instant.now()
        val jobs =
            jobGateway.acquirePendingJobs(command.workerId, command.limit) { job ->
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
