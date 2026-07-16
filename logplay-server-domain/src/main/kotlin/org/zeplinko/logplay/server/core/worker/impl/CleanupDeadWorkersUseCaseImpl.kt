package org.zeplinko.logplay.server.core.worker.impl

import java.time.Instant
import org.zeplinko.logplay.server.core.UnitOfWork
import org.zeplinko.logplay.server.core.job.JobEvent
import org.zeplinko.logplay.server.core.job.JobGateway
import org.zeplinko.logplay.server.core.worker.CleanupDeadWorkersUseCase
import org.zeplinko.logplay.server.core.worker.WorkerGateway

class CleanupDeadWorkersUseCaseImpl(
    private val workerGateway: WorkerGateway,
    private val jobGateway: JobGateway,
    private val unitOfWork: UnitOfWork,
) : CleanupDeadWorkersUseCase {

    /**
     * Single atomic pass: lock the dead workers (`now - lastHeartbeatAt > sessionTimeout`), release
     * their jobs back to the queue, and delete the worker rows. Locking the worker rows first
     * serialises this against `acquire`/deregister on the same worker; the reaper drains every job
     * before the delete so the worker FK is never violated.
     */
    override suspend fun execute(): Int {
        val now = Instant.now()
        return unitOfWork.transaction {
            val dead = workerGateway.findAndLockDeadWorkers(now)
            if (dead.isEmpty()) return@transaction 0
            val deadIds = dead.map { it.id }
            val releasedIds = jobGateway.releaseJobsByWorkerIds(deadIds, now)
            jobGateway.insertEvents(
                releasedIds.map { jobId ->
                    JobEvent.released(jobId, now, "released by dead-worker cleanup")
                }
            )
            workerGateway.deleteWorkers(deadIds)
            dead.size
        }
    }
}
