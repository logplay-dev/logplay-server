package dev.logplay.server.core.worker.impl

import dev.logplay.server.core.job.JobGateway
import dev.logplay.server.core.worker.CleanupDeadWorkersUseCase
import dev.logplay.server.core.worker.WorkerGateway
import java.time.Instant

class CleanupDeadWorkersUseCaseImpl(
    private val workerGateway: WorkerGateway,
    private val jobGateway: JobGateway,
    private val condemnPeriodMs: Long = 15000L,
) : CleanupDeadWorkersUseCase {

    override suspend fun execute(): Int {
        val now = Instant.now()

        // Phase 2: Evict condemned workers whose condemn period has elapsed
        val condemnedWorkers = workerGateway.findCondemnedWorkers(now, condemnPeriodMs)
        if (condemnedWorkers.isNotEmpty()) {
            val condemnedIds = condemnedWorkers.map { it.id }
            jobGateway.releaseJobsByWorkerIds(condemnedIds, now)
            workerGateway.deleteWorkers(condemnedIds)
        }

        // Phase 1: Atomically condemn newly dead workers
        val condemnedCount = workerGateway.condemnDeadWorkers(now)

        return condemnedWorkers.size + condemnedCount
    }
}
