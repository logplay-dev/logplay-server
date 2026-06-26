package org.zeplinko.logplay.server.core.job.impl

import java.time.Instant
import org.zeplinko.logplay.server.core.UnitOfWork
import org.zeplinko.logplay.server.core.job.*
import org.zeplinko.logplay.server.core.worker.BlankWorkerIdException

class CompleteJobUseCaseImpl(
    private val jobGateway: JobGateway,
    private val unitOfWork: UnitOfWork,
) : CompleteJobUseCase {
    override suspend fun execute(command: CompleteJobCommand): Job {
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
            jobGateway.removeFromAcquired(command.jobId)
            jobGateway.markTerminal(command.jobId, JobStatus.FINISHED, now, command.outputData)
            val event =
                JobEvent.worker(command.jobId, JobEventType.COMPLETED, command.workerId, now)
            jobGateway.insertEvents(listOf(event))
            job.copy(
                status = JobStatus.FINISHED,
                retries = null,
                acquiredByWorkerId = null,
                lastAcquiredAt = null,
                availableAt = null,
                terminalAt = now,
                updatedAt = now,
                outputData = command.outputData,
            )
        }
    }
}
