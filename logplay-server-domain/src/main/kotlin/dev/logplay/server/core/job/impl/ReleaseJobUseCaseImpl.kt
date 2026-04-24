package dev.logplay.server.core.job.impl

import dev.logplay.server.core.job.*
import dev.logplay.server.core.worker.BlankWorkerIdException
import java.time.Instant
import java.util.*

class ReleaseJobUseCaseImpl(private val jobGateway: JobGateway) : ReleaseJobUseCase {
    override suspend fun execute(command: ReleaseJobCommand): Job {
        if (command.jobId.isBlank()) throw BlankJobIdException()
        if (command.workerId.isBlank()) throw BlankWorkerIdException()
        val now = Instant.now()
        val event =
            JobEvent(
                id = UUID.randomUUID().toString(),
                jobId = command.jobId,
                eventType = JobEventType.RELEASED,
                actorType = ActorType.WORKER,
                actorId = command.workerId,
                createdAt = now,
                eventMessage = null,
                eventDetail = null,
            )
        val released = jobGateway.releaseJob(command.jobId, command.workerId, now, event)
        if (released != null) return released
        val job = jobGateway.findJobById(command.jobId) ?: throw JobNotFoundException(command.jobId)
        if (job.status != JobStatus.ACQUIRED)
            throw JobNotAcquiredException(command.jobId, job.status)
        if (job.acquiredByWorkerId != command.workerId)
            throw JobNotOwnedByWorkerException(command.jobId, command.workerId)
        throw JobConcurrentModificationException(command.jobId)
    }
}
