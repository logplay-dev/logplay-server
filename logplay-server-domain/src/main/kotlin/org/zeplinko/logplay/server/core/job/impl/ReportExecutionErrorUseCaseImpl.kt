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
        val result =
            jobGateway.reportExecutionError(command.jobId, command.workerId, now) { job ->
                // job is guaranteed ACQUIRED here (gateway verified ownership) — retries is
                // populated for ACQUIRED rows from job_acquired.retries.
                val newRetries = (job.retries ?: 0) + 1
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
        return when (result) {
            is ReportExecutionErrorResult.Success -> result.job
            is ReportExecutionErrorResult.NotFound -> throw JobNotFoundException(command.jobId)
            is ReportExecutionErrorResult.WrongStatus ->
                throw JobNotAcquiredException(command.jobId, result.status)
            is ReportExecutionErrorResult.WrongWorker ->
                throw JobNotOwnedByWorkerException(command.jobId, command.workerId)
        }
    }
}
