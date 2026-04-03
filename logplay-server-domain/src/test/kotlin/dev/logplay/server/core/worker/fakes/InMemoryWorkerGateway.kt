package dev.logplay.server.core.worker.fakes

import dev.logplay.server.core.worker.Worker
import dev.logplay.server.core.worker.WorkerAlreadyRegisteredException
import dev.logplay.server.core.worker.WorkerGateway
import java.time.Instant

class InMemoryWorkerGateway : WorkerGateway {

    private val workers = mutableMapOf<String, Worker>()

    override suspend fun insertWorker(worker: Worker): Worker {
        if (workers.containsKey(worker.id)) throw WorkerAlreadyRegisteredException(worker.id)
        workers[worker.id] = worker
        return worker
    }

    override suspend fun updateWorkerHeartbeat(workerId: String, heartbeatAt: Instant): Worker? {
        val worker = workers[workerId] ?: return null
        if (worker.condemned) return null
        val updated = worker.copy(lastHeartbeatAt = heartbeatAt)
        workers[workerId] = updated
        return updated
    }

    override suspend fun findWorkerById(id: String): Worker? = workers[id]

    override suspend fun deleteWorker(id: String) {
        workers.remove(id)
    }

    override suspend fun condemnWorker(id: String) {
        val worker = workers[id] ?: return
        workers[id] = worker.copy(condemned = true)
    }

    override suspend fun condemnDeadWorkers(now: Instant): Int {
        val dead =
            workers.values.filter { worker ->
                !worker.condemned &&
                    (now.toEpochMilli() - worker.lastHeartbeatAt.toEpochMilli()) >
                        worker.sessionTimeout
            }
        for (worker in dead) {
            workers[worker.id] = worker.copy(condemned = true)
        }
        return dead.size
    }

    override suspend fun deleteWorkers(ids: List<String>) {
        for (id in ids) workers.remove(id)
    }

    override suspend fun findCondemnedWorkers(now: Instant, condemnPeriodMs: Long): List<Worker> =
        workers.values.filter { worker ->
            worker.condemned &&
                (now.toEpochMilli() - worker.lastHeartbeatAt.toEpochMilli()) >
                    (worker.sessionTimeout + condemnPeriodMs)
        }

    // Test helpers
    fun count(): Int = workers.size

    fun save(worker: Worker) {
        workers[worker.id] = worker
    }
}
