package org.zeplinko.logplay.server.core.job.impl

import java.time.Instant
import org.zeplinko.logplay.server.core.UnitOfWork
import org.zeplinko.logplay.server.core.job.*
import org.zeplinko.logplay.server.core.worker.BlankWorkerIdException

class ReportExecutionErrorUseCaseImpl(
    private val jobGateway: JobGateway,
    private val unitOfWork: UnitOfWork,
) : ReportExecutionErrorUseCase {

    override suspend fun execute(command: ReportExecutionErrorCommand): Job {
        if (command.jobId.isBlank()) throw BlankJobIdException()
        if (command.workerId.isBlank()) throw BlankWorkerIdException()
        val now = Instant.now()
        return unitOfWork.transaction {
            val job =
                jobGateway.findAndLockJobById(command.jobId)
                    ?: throw JobNotFoundException(command.jobId)
            when (job.status) {
                JobStatus.ACQUIRED -> Unit
                JobStatus.PENDING,
                JobStatus.FINISHED,
                JobStatus.FAILED,
                JobStatus.ABORTED -> throw JobNotAcquiredException(command.jobId, job.status)
            }
            if (job.acquiredByWorkerId != command.workerId)
                throw JobNotOwnedByWorkerException(command.jobId, command.workerId)
            // retries is populated for ACQUIRED rows from job_acquired.retries.
            val newRetries = (job.retries ?: 0) + 1
            val failed = job.maxRetries != null && newRetries >= job.maxRetries
            val errorEvent =
                JobEvent.worker(
                    command.jobId,
                    JobEventType.ERROR_REPORTED,
                    command.workerId,
                    now,
                    message = command.error,
                )
            jobGateway.removeFromAcquired(command.jobId)
            return@transaction if (failed) {
                jobGateway.markTerminal(command.jobId, JobStatus.FAILED, now, null)
                val failedEvent = JobEvent.system(command.jobId, JobEventType.FAILED, now)
                jobGateway.insertEvents(listOf(errorEvent, failedEvent))
                job.copy(
                    status = JobStatus.FAILED,
                    retries = null,
                    acquiredByWorkerId = null,
                    lastAcquiredAt = null,
                    availableAt = null,
                    terminalAt = now,
                    updatedAt = now,
                )
            } else {
                jobGateway.enqueue(command.jobId, job.groupId, job.type, now, newRetries, 0L)
                jobGateway.insertEvents(listOf(errorEvent))
                job.copy(
                    status = JobStatus.PENDING,
                    retries = newRetries,
                    acquiredByWorkerId = null,
                    lastAcquiredAt = null,
                    availableAt = 0L,
                    updatedAt = now,
                )
            }
        }
    }
}
