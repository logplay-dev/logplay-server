package org.zeplinko.logplay.server.core.worker

import java.time.Instant

/**
 * Persistence port for worker registration, heartbeat, and the two-phase dead-worker cleanup.
 * Backend modules implement this interface.
 *
 * **Two-phase cleanup semantics.** A worker that has not heartbeated within its `sessionTimeout` is
 * first marked `condemned = true` (locking it out of further heartbeats). After a grace period —
 * long enough that any in-flight heartbeat from the same worker would have either landed or be
 * noticed missing — the worker's jobs are released and its row is deleted. [condemnDeadWorkers]
 * implements phase 1; [findCondemnedWorkers] + [deleteWorkers] implement phase 2.
 */
interface WorkerGateway {
    /**
     * Inserts a new worker row.
     *
     * @throws WorkerAlreadyRegisteredException on primary-key collision.
     */
    suspend fun insertWorker(worker: Worker): Worker

    /**
     * Updates the worker's `lastHeartbeatAt`. No-op (returns `null`) if the worker does not exist
     * or is condemned.
     */
    suspend fun updateWorkerHeartbeat(workerId: String, heartbeatAt: Instant): Worker?

    /** Looks up a worker by id. */
    suspend fun findWorkerById(id: String): Worker?

    /** Deletes a worker row. Caller is responsible for releasing the worker's jobs first. */
    suspend fun deleteWorker(id: String)

    /**
     * Marks a single worker `condemned = true`. Subsequent heartbeats from this worker are rejected
     * by [updateWorkerHeartbeat].
     */
    suspend fun condemnWorker(id: String)

    /**
     * Phase 1 of dead-worker cleanup: condemns every worker whose `lastHeartbeatAt +
     * sessionTimeout` is in the past relative to `now` and is not already condemned.
     *
     * @return the number of workers transitioned to `condemned` in this call.
     */
    suspend fun condemnDeadWorkers(now: Instant): Int

    /** Bulk-deletes the given worker rows. Used at the end of phase 2. */
    suspend fun deleteWorkers(ids: List<String>)

    /**
     * Phase 2 query: returns workers that have been condemned for at least `condemnPeriodMs`
     * milliseconds, suitable for job release + deletion.
     */
    suspend fun findCondemnedWorkers(now: Instant, condemnPeriodMs: Long): List<Worker>
}
