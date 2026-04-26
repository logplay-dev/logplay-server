package org.zeplinko.logplay.server.core.job.impl

import java.time.Instant
import java.util.*
import org.zeplinko.logplay.server.core.job.*
import org.zeplinko.logplay.server.core.worker.BlankWorkerIdException

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
        return when (
            val result =
                jobGateway.completeJob(
                    command.jobId,
                    command.workerId,
                    command.outputData,
                    now,
                    event,
                )
        ) {
            is CompleteJobResult.Success -> result.job
            is CompleteJobResult.NotFound -> throw JobNotFoundException(command.jobId)
            is CompleteJobResult.WrongStatus ->
                throw JobNotAcquiredException(command.jobId, result.status)
            is CompleteJobResult.WrongWorker ->
                throw JobNotOwnedByWorkerException(command.jobId, command.workerId)
        }
    }
}
