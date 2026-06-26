package org.zeplinko.logplay.server.worker.use.case

import org.zeplinko.logplay.server.core.UnitOfWork
import org.zeplinko.logplay.server.core.job.JobGateway
import org.zeplinko.logplay.server.core.worker.*
import org.zeplinko.logplay.server.core.worker.impl.CleanupDeadWorkersUseCaseImpl
import org.zeplinko.logplay.server.core.worker.impl.DeregisterWorkerUseCaseImpl
import org.zeplinko.logplay.server.core.worker.impl.HeartbeatWorkerUseCaseImpl
import org.zeplinko.logplay.server.core.worker.impl.RegisterWorkerUseCaseImpl

/**
 * Manual dependency-injection factory for the worker-domain use cases. Wires both gateways since
 * worker lifecycle interacts with held jobs (deregister and dead-worker cleanup release the
 * worker's jobs back to the queue).
 */
class WorkerUseCaseLookUp(
    workerGateway: WorkerGateway,
    jobGateway: JobGateway,
    unitOfWork: UnitOfWork,
) {
    val registerWorkerUseCase: RegisterWorkerUseCase =
        RegisterWorkerUseCaseImpl(workerGateway, unitOfWork)
    val heartbeatWorkerUseCase: HeartbeatWorkerUseCase = HeartbeatWorkerUseCaseImpl(workerGateway)
    val deregisterWorkerUseCase: DeregisterWorkerUseCase =
        DeregisterWorkerUseCaseImpl(workerGateway, jobGateway, unitOfWork)
    val cleanupDeadWorkersUseCase: CleanupDeadWorkersUseCase =
        CleanupDeadWorkersUseCaseImpl(workerGateway, jobGateway, unitOfWork)
}
