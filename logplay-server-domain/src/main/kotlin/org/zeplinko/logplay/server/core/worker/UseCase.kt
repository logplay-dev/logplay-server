package org.zeplinko.logplay.server.core.worker

/**
 * Registers a new worker with the server. The caller chooses the `workerId`; the heartbeat and
 * session timeouts establish liveness expectations and dead-worker cleanup boundaries.
 */
interface RegisterWorkerUseCase {
    /**
     * @throws BlankWorkerIdException, InvalidWorkerIdException for invalid identifiers.
     * @throws InvalidWorkerTimeoutException if `sessionTimeout <= heartbeatTimeout` or either is
     *   non-positive.
     * @throws WorkerAlreadyRegisteredException if a worker with the same id already exists.
     */
    suspend fun execute(command: RegisterWorkerCommand): Worker
}

/**
 * Refreshes a worker's `lastHeartbeatAt` timestamp, proving liveness. A worker that has already
 * timed out (`now - lastHeartbeatAt > sessionTimeout`) cannot heartbeat back to life — it is
 * treated as gone and must re-register.
 */
interface HeartbeatWorkerUseCase {
    /** @throws WorkerNotFoundException if the worker is not registered or has already timed out. */
    suspend fun execute(command: HeartbeatWorkerCommand): Worker
}

/**
 * Explicit, graceful deregistration of a worker. Releases all jobs the worker holds back to
 * `PENDING`, then deletes the worker record.
 */
interface DeregisterWorkerUseCase {
    /** @throws WorkerNotFoundException if the worker is not registered. */
    suspend fun execute(command: DeregisterWorkerCommand)
}

/**
 * Single-pass cleanup of dead workers, intended to run on a periodic schedule: for each worker
 * whose `lastHeartbeatAt` is older than its `sessionTimeout`, release the jobs it holds back to
 * `PENDING` and delete the worker record — atomically, in one transaction.
 */
interface CleanupDeadWorkersUseCase {
    /** @return the number of dead workers reclaimed (jobs released + row deleted) in this run. */
    suspend fun execute(): Int
}
