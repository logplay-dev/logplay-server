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
        return when (val result = jobGateway.abortJob(command.jobId, now, event)) {
            is AbortJobResult.Success -> result.job
            is AbortJobResult.NotFound -> throw JobNotFoundException(command.jobId)
            is AbortJobResult.AlreadyTerminal ->
                throw JobNotAbortableException(command.jobId, result.status)
        }
    }
}
