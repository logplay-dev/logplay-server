package org.zeplinko.logplay.server.core.job.impl

import java.time.Instant
import org.zeplinko.logplay.server.core.UnitOfWork
import org.zeplinko.logplay.server.core.job.*
import org.zeplinko.logplay.server.core.worker.BlankWorkerIdException

class ReleaseJobUseCaseImpl(
    private val jobGateway: JobGateway,
    private val unitOfWork: UnitOfWork,
) : ReleaseJobUseCase {
    override suspend fun execute(command: ReleaseJobCommand): Job {
        if (command.jobId.isBlank()) throw BlankJobIdException()
        if (command.workerId.isBlank()) throw BlankWorkerIdException()
        val now = Instant.now()
        val availableAt = command.availableAt ?: 0L
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
            val retries = job.retries ?: 0
            jobGateway.removeFromAcquired(command.jobId)
            jobGateway.enqueue(command.jobId, job.groupId, job.type, now, retries, availableAt)
            val eventDetail = if (availableAt != 0L) """{"availableAt":$availableAt}""" else null
            val event =
                JobEvent.worker(
                    command.jobId,
                    JobEventType.RELEASED,
                    command.workerId,
                    now,
                    detail = eventDetail,
                )
            jobGateway.insertEvents(listOf(event))
            job.copy(
                status = JobStatus.PENDING,
                retries = retries,
                acquiredByWorkerId = null,
                lastAcquiredAt = null,
                availableAt = availableAt,
                updatedAt = now,
            )
        }
    }
}
