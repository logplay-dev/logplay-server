package org.zeplinko.logplay.server.core.worker.fakes

import java.time.Instant
import org.zeplinko.logplay.server.core.worker.Worker
import org.zeplinko.logplay.server.core.worker.WorkerAlreadyRegisteredException
import org.zeplinko.logplay.server.core.worker.WorkerGateway
import org.zeplinko.logplay.server.core.worker.isDeadAt

class InMemoryWorkerGateway : WorkerGateway {

    private val workers = mutableMapOf<String, Worker>()

    override suspend fun insertWorker(worker: Worker): Worker {
        if (workers.containsKey(worker.id)) throw WorkerAlreadyRegisteredException(worker.id)
        workers[worker.id] = worker
        return worker
    }

    override suspend fun updateWorkerHeartbeat(workerId: String, heartbeatAt: Instant): Worker? {
        val worker = workers[workerId] ?: return null
        // A timed-out worker cannot heartbeat back to life.
        if (worker.isDeadAt(heartbeatAt)) return null
        val updated = worker.copy(lastHeartbeatAt = heartbeatAt)
        workers[workerId] = updated
        return updated
    }

    // The in-memory fake is single-threaded, so locking is a no-op read.
    override suspend fun findAndLockWorkerById(id: String): Worker? = workers[id]

    override suspend fun deleteWorker(id: String) {
        workers.remove(id)
    }

    override suspend fun deleteWorkers(ids: List<String>) {
        for (id in ids) workers.remove(id)
    }

    override suspend fun findAndLockDeadWorkers(now: Instant): List<Worker> =
        workers.values.filter { it.isDeadAt(now) }

    // Test helpers
    fun count(): Int = workers.size

    fun save(worker: Worker) {
        workers[worker.id] = worker
    }

    /** Test-only unlocked read for asserting worker state; not part of [WorkerGateway]. */
    fun findWorkerById(id: String): Worker? = workers[id]
}
