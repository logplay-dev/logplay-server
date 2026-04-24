package org.zeplinko.logplay.server.core.job.impl

import java.time.Instant
import java.util.*
import org.zeplinko.logplay.server.core.job.*

class AbortJobUseCaseImpl(private val jobGateway: JobGateway) : AbortJobUseCase {
    override suspend fun execute(command: AbortJobCommand): Job {
        if (command.jobId.isBlank()) throw BlankJobIdException()
        val now = Instant.now()
        val event =
            JobEvent(
                id = UUID.randomUUID().toString(),
                jobId = command.jobId,
                eventType = JobEventType.ABORTED,
                actorType = ActorType.SYSTEM,
                actorId = null,
                createdAt = now,
                eventMessage = null,
                eventDetail = null,
            )
        val aborted = jobGateway.abortJob(command.jobId, now, event)
        if (aborted != null) return aborted
        val job = jobGateway.findJobById(command.jobId) ?: throw JobNotFoundException(command.jobId)
        if (job.status != JobStatus.PENDING && job.status != JobStatus.ACQUIRED)
            throw JobNotAbortableException(command.jobId, job.status)
        throw JobConcurrentModificationException(command.jobId)
    }
}
