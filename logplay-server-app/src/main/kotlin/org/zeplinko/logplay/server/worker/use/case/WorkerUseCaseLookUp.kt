package org.zeplinko.logplay.server.worker.use.case

import org.zeplinko.logplay.server.core.job.JobGateway
import org.zeplinko.logplay.server.core.worker.*
import org.zeplinko.logplay.server.core.worker.impl.CleanupDeadWorkersUseCaseImpl
import org.zeplinko.logplay.server.core.worker.impl.DeregisterWorkerUseCaseImpl
import org.zeplinko.logplay.server.core.worker.impl.HeartbeatWorkerUseCaseImpl
import org.zeplinko.logplay.server.core.worker.impl.RegisterWorkerUseCaseImpl

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
