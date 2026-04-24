package org.zeplinko.logplay.server.core.job.impl

import java.time.Instant
import java.util.*
import org.zeplinko.logplay.server.core.job.*
import org.zeplinko.logplay.server.core.worker.BlankWorkerIdException

class ReportExecutionErrorUseCaseImpl(private val jobGateway: JobGateway) :
    ReportExecutionErrorUseCase {

    override suspend fun execute(command: ReportExecutionErrorCommand): Job {
        if (command.jobId.isBlank()) throw BlankJobIdException()
        if (command.workerId.isBlank()) throw BlankWorkerIdException()
        val now = Instant.now()
        val updated =
            jobGateway.reportExecutionError(command.jobId, command.workerId, now) { job ->
                val newRetries = job.retries + 1
                val nextStatus =
                    if (job.maxRetries != null && newRetries >= job.maxRetries) JobStatus.FAILED
                    else JobStatus.PENDING
                val errorEvent =
                    JobEvent(
                        id = UUID.randomUUID().toString(),
                        jobId = command.jobId,
                        eventType = JobEventType.ERROR_REPORTED,
                        actorType = ActorType.WORKER,
                        actorId = command.workerId,
                        createdAt = now,
                        eventMessage = command.error,
                        eventDetail = null,
                    )
                val events =
                    if (nextStatus == JobStatus.FAILED) {
                        val failedEvent =
                            JobEvent(
                                id = UUID.randomUUID().toString(),
                                jobId = command.jobId,
                                eventType = JobEventType.FAILED,
                                actorType = ActorType.SYSTEM,
                                actorId = null,
                                createdAt = now,
                                eventMessage = null,
                                eventDetail = null,
                            )
                        listOf(errorEvent, failedEvent)
                    } else {
                        listOf(errorEvent)
                    }
                ExecutionErrorResult(status = nextStatus, retries = newRetries, events = events)
            }
        if (updated != null) return updated
        val job = jobGateway.findJobById(command.jobId) ?: throw JobNotFoundException(command.jobId)
        if (job.status != JobStatus.ACQUIRED)
            throw JobNotAcquiredException(command.jobId, job.status)
        if (job.acquiredByWorkerId != command.workerId)
            throw JobNotOwnedByWorkerException(command.jobId, command.workerId)
        throw JobConcurrentModificationException(command.jobId)
    }
}
