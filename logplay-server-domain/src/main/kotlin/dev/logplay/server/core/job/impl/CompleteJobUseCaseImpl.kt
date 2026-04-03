package dev.logplay.server.core.job.impl

import dev.logplay.server.core.job.*
import dev.logplay.server.core.worker.BlankWorkerIdException
import java.time.Instant
import java.util.*

class CompleteJobUseCaseImpl(private val jobGateway: JobGateway) : CompleteJobUseCase {
    override suspend fun execute(command: CompleteJobCommand): Job {
        if (command.jobId.isBlank()) throw BlankJobIdException()
        if (command.workerId.isBlank()) throw BlankWorkerIdException()
        val now = Instant.now()
        val event =
            JobEvent(
                id = UUID.randomUUID().toString(),
                jobId = command.jobId,
                eventType = JobEventType.COMPLETED,
                actorType = ActorType.WORKER,
                actorId = command.workerId,
                createdAt = now,
                eventMessage = null,
                eventDetail = null,
            )
        val completed =
            jobGateway.completeJob(command.jobId, command.workerId, command.outputData, now, event)
        if (completed != null) return completed
        val job = jobGateway.findJobById(command.jobId) ?: throw JobNotFoundException(command.jobId)
        if (job.status != JobStatus.ACQUIRED)
            throw JobNotAcquiredException(command.jobId, job.status)
        if (job.acquiredByWorkerId != command.workerId)
            throw JobNotOwnedByWorkerException(command.jobId, command.workerId)
        throw JobConcurrentModificationException(command.jobId)
    }
}
