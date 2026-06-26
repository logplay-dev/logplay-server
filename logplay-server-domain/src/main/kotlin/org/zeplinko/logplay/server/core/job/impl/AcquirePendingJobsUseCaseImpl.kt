package org.zeplinko.logplay.server.core.job.impl

import java.time.Instant
import org.zeplinko.logplay.server.core.UnitOfWork
import org.zeplinko.logplay.server.core.job.*
import org.zeplinko.logplay.server.core.worker.BlankWorkerIdException
import org.zeplinko.logplay.server.core.worker.WorkerGateway
import org.zeplinko.logplay.server.core.worker.WorkerNotFoundException
import org.zeplinko.logplay.server.core.worker.isDeadAt

class AcquirePendingJobsUseCaseImpl(
    private val jobGateway: JobGateway,
    private val workerGateway: WorkerGateway,
    private val unitOfWork: UnitOfWork,
) : AcquirePendingJobsUseCase {

    companion object {
        const val MAX_LIMIT = 100
    }

    override suspend fun execute(command: AcquirePendingJobsCommand): List<Job> {
        if (command.groupId.isBlank()) throw BlankGroupIdException()
        if (command.type.isBlank()) throw BlankJobTypeException()
        if (command.workerId.isBlank()) throw BlankWorkerIdException()
        if (command.limit !in 1..MAX_LIMIT) throw InvalidLimitException(MAX_LIMIT)
        val now = Instant.now()
        return unitOfWork.transaction {
            // Lock the worker row first (single-table FOR UPDATE), then check liveness. This
            // serialises acquire against cleanup/deregister on the same worker and rejects a
            // dead/missing worker before any job is claimed — so no job_acquired row is ever
            // created
            // for a worker that is being reclaimed.
            val worker =
                workerGateway.findAndLockWorkerById(command.workerId)
                    ?: throw WorkerNotFoundException(command.workerId)
            if (worker.isDeadAt(now)) throw WorkerNotFoundException(command.workerId)
            val acquired =
                jobGateway.acquirePendingJobs(
                    command.groupId,
                    command.type,
                    command.workerId,
                    command.limit,
                )
            if (acquired.isNotEmpty()) {
                jobGateway.insertEvents(
                    acquired.map { job ->
                        JobEvent.worker(job.id, JobEventType.ACQUIRED, command.workerId, now)
                    }
                )
            }
            acquired
        }
    }
}
