package org.zeplinko.logplay.server.core.worker

import java.time.Instant

interface WorkerGateway {
    suspend fun insertWorker(worker: Worker): Worker

    suspend fun updateWorkerHeartbeat(workerId: String, heartbeatAt: Instant): Worker?

    suspend fun findWorkerById(id: String): Worker?

    suspend fun deleteWorker(id: String)

    suspend fun condemnWorker(id: String)

    suspend fun condemnDeadWorkers(now: Instant): Int

    suspend fun deleteWorkers(ids: List<String>)

    suspend fun findCondemnedWorkers(now: Instant, condemnPeriodMs: Long): List<Worker>
}
