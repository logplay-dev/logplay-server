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
 * Refreshes a worker's `lastHeartbeatAt` timestamp, proving liveness. Heartbeats from condemned
 * workers are rejected so the cleanup phase can safely release their jobs.
 */
interface HeartbeatWorkerUseCase {
    /**
     * @throws WorkerNotFoundException if the worker is not registered.
     * @throws WorkerCondemnedException if the worker has been condemned and is awaiting deletion.
     */
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
 * Two-phase cleanup of dead workers, intended to run on a periodic schedule:
 * 1. Condemn workers whose `lastHeartbeatAt` is older than their `sessionTimeout`.
 * 2. Release the jobs held by previously-condemned workers (after a grace period) and delete those
 *    worker records.
 *
 * The grace period is configured at the use-case level — see the implementation.
 */
interface CleanupDeadWorkersUseCase {
    /** @return the number of workers deleted in this run (excludes those merely condemned). */
    suspend fun execute(): Int
}
