package org.zeplinko.logplay.server.worker.use.case

import org.zeplinko.logplay.server.core.job.JobGateway
import org.zeplinko.logplay.server.core.worker.*
import org.zeplinko.logplay.server.core.worker.impl.CleanupDeadWorkersUseCaseImpl
import org.zeplinko.logplay.server.core.worker.impl.DeregisterWorkerUseCaseImpl
import org.zeplinko.logplay.server.core.worker.impl.HeartbeatWorkerUseCaseImpl
import org.zeplinko.logplay.server.core.worker.impl.RegisterWorkerUseCaseImpl

/**
 * Manual dependency-injection factory for the worker-domain use cases. Wires both gateways since
 * worker lifecycle interacts with held jobs (deregister releases jobs; cleanup releases jobs of
 * condemned workers).
 *
 * @param condemnPeriodMs grace period in ms between phase 1 (condemn) and phase 2 (release jobs +
 *   delete) of dead-worker cleanup; defaults to 15 seconds.
 */
class WorkerUseCaseLookUp(
    workerGateway: WorkerGateway,
    jobGateway: JobGateway,
    condemnPeriodMs: Long = 15000L,
) {
    val registerWorkerUseCase: RegisterWorkerUseCase = RegisterWorkerUseCaseImpl(workerGateway)
    val heartbeatWorkerUseCase: HeartbeatWorkerUseCase = HeartbeatWorkerUseCaseImpl(workerGateway)
    val deregisterWorkerUseCase: DeregisterWorkerUseCase =
        DeregisterWorkerUseCaseImpl(workerGateway, jobGateway)
    val cleanupDeadWorkersUseCase: CleanupDeadWorkersUseCase =
        CleanupDeadWorkersUseCaseImpl(workerGateway, jobGateway, condemnPeriodMs)
}
