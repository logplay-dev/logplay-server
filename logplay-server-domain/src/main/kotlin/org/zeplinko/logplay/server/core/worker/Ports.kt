package org.zeplinko.logplay.server.core.worker

import java.time.Instant

/**
 * Persistence port for worker registration, heartbeat, and dead-worker cleanup. Backend modules
 * implement this interface.
 *
 * **Liveness model.** A worker is *alive* while it has heartbeated within its `sessionTimeout`
 * (`now - lastHeartbeatAt <= sessionTimeout`) and *dead* otherwise — there is no intermediate
 * state. A single cleanup pass reclaims a dead worker's jobs and deletes its row atomically
 * ([findAndLockDeadWorkers] + `releaseJobsByWorkerIds` + [deleteWorkers]). Heartbeat and acquire
 * are gated on liveness, so a timed-out worker is rejected and must re-register. The `workers` row
 * is the per-worker serialisation point: lifecycle ops lock it first ([findAndLockWorkerById] for
 * deregister/acquire, [findAndLockDeadWorkers] for cleanup) so concurrent ops on the same worker
 * serialise rather than racing the worker-delete against a job-acquire/release.
 */
interface WorkerGateway {
    /**
     * Inserts a new worker row.
     *
     * @throws WorkerAlreadyRegisteredException on primary-key collision.
     */
    suspend fun insertWorker(worker: Worker): Worker

    /**
     * Updates the worker's `lastHeartbeatAt`, returning the updated worker. No-op (returns `null`)
     * if the worker does not exist or is already dead (`heartbeatAt - lastHeartbeatAt >
     * sessionTimeout`) — a timed-out worker cannot heartbeat back to life and must re-register.
     */
    suspend fun updateWorkerHeartbeat(workerId: String, heartbeatAt: Instant): Worker?

    /**
     * Looks up a worker by id while taking a `FOR UPDATE` lock on its row, held until the enclosing
     * transaction ends. The `workers` row is the per-worker serialisation point: deregister and
     * acquire take it first (acquire then checks liveness), while cleanup locks dead rows via
     * [findAndLockDeadWorkers]. Taking it before releasing jobs / deleting the row serialises
     * concurrent ops on the same worker and avoids the jobs-row vs worker-row lock-order deadlock
     * an unlocked read would allow.
     */
    suspend fun findAndLockWorkerById(id: String): Worker?

    /** Deletes a worker row. Caller is responsible for releasing the worker's jobs first. */
    suspend fun deleteWorker(id: String)

    /** Bulk-deletes the given worker rows. Used at the end of a cleanup pass. */
    suspend fun deleteWorkers(ids: List<String>)

    /**
     * Returns the dead workers (`now - lastHeartbeatAt > sessionTimeout`), locking their rows with
     * `FOR UPDATE SKIP LOCKED` so concurrent cleanup passes (and a concurrent deregister) never
     * reclaim the same worker twice — a worker already locked by another lifecycle op is skipped,
     * not waited on, keeping cleanup deadlock-free against the blocking [findAndLockWorkerById].
     */
    suspend fun findAndLockDeadWorkers(now: Instant): List<Worker>
}
