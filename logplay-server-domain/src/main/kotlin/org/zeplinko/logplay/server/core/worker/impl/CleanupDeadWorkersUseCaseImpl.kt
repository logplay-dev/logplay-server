package org.zeplinko.logplay.server.core.worker.impl

import java.time.Instant
import java.util.UUID
import org.zeplinko.logplay.server.core.job.ActorType
import org.zeplinko.logplay.server.core.job.JobEvent
import org.zeplinko.logplay.server.core.job.JobEventType
import org.zeplinko.logplay.server.core.job.JobGateway
import org.zeplinko.logplay.server.core.worker.CleanupDeadWorkersUseCase
import org.zeplinko.logplay.server.core.worker.WorkerGateway

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
            jobGateway.releaseJobsByWorkerIds(condemnedIds, now) { jobId ->
                JobEvent(
                    id = UUID.randomUUID().toString(),
                    jobId = jobId,
                    eventType = JobEventType.RELEASED,
                    actorType = ActorType.SYSTEM,
                    actorId = null,
                    createdAt = now,
                    eventMessage = "released by dead-worker cleanup after condemn period elapsed",
                    eventDetail = null,
                )
            }
            workerGateway.deleteWorkers(condemnedIds)
        }

        // Phase 1: Atomically condemn newly dead workers
        val condemnedCount = workerGateway.condemnDeadWorkers(now)

        return condemnedWorkers.size + condemnedCount
    }
}
