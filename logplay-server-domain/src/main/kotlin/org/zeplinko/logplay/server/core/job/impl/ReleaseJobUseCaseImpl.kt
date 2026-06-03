package org.zeplinko.logplay.server.core.job.impl

import java.time.Instant
import java.util.*
import org.zeplinko.logplay.server.core.job.*
import org.zeplinko.logplay.server.core.worker.BlankWorkerIdException

class ReleaseJobUseCaseImpl(private val jobGateway: JobGateway) : ReleaseJobUseCase {
    override suspend fun execute(command: ReleaseJobCommand): Job {
        if (command.jobId.isBlank()) throw BlankJobIdException()
        if (command.workerId.isBlank()) throw BlankWorkerIdException()
        val now = Instant.now()
        val availableAt = command.availableAt ?: 0L
        val eventDetail = if (availableAt != 0L) """{"availableAt":$availableAt}""" else null
        val event =
            JobEvent(
                id = UUID.randomUUID().toString(),
                jobId = command.jobId,
                eventType = JobEventType.RELEASED,
                actorType = ActorType.WORKER,
                actorId = command.workerId,
                createdAt = now,
                eventMessage = null,
                eventDetail = eventDetail,
            )
        return when (
            val result =
                jobGateway.releaseJob(command.jobId, command.workerId, now, availableAt, event)
        ) {
            is ReleaseJobResult.Success -> result.job
            is ReleaseJobResult.NotFound -> throw JobNotFoundException(command.jobId)
            is ReleaseJobResult.WrongStatus ->
                throw JobNotAcquiredException(command.jobId, result.status)
            is ReleaseJobResult.WrongWorker ->
                throw JobNotOwnedByWorkerException(command.jobId, command.workerId)
        }
    }
}
