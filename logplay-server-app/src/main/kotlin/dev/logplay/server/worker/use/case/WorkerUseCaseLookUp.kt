package dev.logplay.server.worker.use.case

import dev.logplay.server.core.job.JobGateway
import dev.logplay.server.core.worker.*
import dev.logplay.server.core.worker.impl.CleanupDeadWorkersUseCaseImpl
import dev.logplay.server.core.worker.impl.DeregisterWorkerUseCaseImpl
import dev.logplay.server.core.worker.impl.HeartbeatWorkerUseCaseImpl
import dev.logplay.server.core.worker.impl.RegisterWorkerUseCaseImpl

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
