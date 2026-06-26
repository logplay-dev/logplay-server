package org.zeplinko.logplay.server.core.job.impl

import java.time.Instant
import org.zeplinko.logplay.server.core.UnitOfWork
import org.zeplinko.logplay.server.core.job.*

class AbortJobUseCaseImpl(private val jobGateway: JobGateway, private val unitOfWork: UnitOfWork) :
    AbortJobUseCase {
    override suspend fun execute(command: AbortJobCommand): Job {
        if (command.jobId.isBlank()) throw BlankJobIdException()
        val now = Instant.now()
        return unitOfWork.transaction {
            val job =
                jobGateway.findAndLockJobById(command.jobId)
                    ?: throw JobNotFoundException(command.jobId)
            when (job.status) {
                JobStatus.PENDING -> jobGateway.removeFromQueue(command.jobId)
                JobStatus.ACQUIRED -> jobGateway.removeFromAcquired(command.jobId)
                JobStatus.FINISHED,
                JobStatus.FAILED,
                JobStatus.ABORTED -> throw JobNotAbortableException(command.jobId, job.status)
            }
            jobGateway.markTerminal(command.jobId, JobStatus.ABORTED, now, null)
            val event = JobEvent.system(command.jobId, JobEventType.ABORTED, now)
            jobGateway.insertEvents(listOf(event))
            job.copy(
                status = JobStatus.ABORTED,
                retries = null,
                acquiredByWorkerId = null,
                lastAcquiredAt = null,
                availableAt = null,
                terminalAt = now,
                updatedAt = now,
            )
        }
    }
}
