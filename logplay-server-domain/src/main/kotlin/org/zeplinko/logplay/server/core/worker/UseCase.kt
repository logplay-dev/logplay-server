package org.zeplinko.logplay.server.core.worker

interface RegisterWorkerUseCase {
    suspend fun execute(command: RegisterWorkerCommand): Worker
}

interface HeartbeatWorkerUseCase {
    suspend fun execute(command: HeartbeatWorkerCommand): Worker
}

interface DeregisterWorkerUseCase {
    suspend fun execute(command: DeregisterWorkerCommand)
}

interface CleanupDeadWorkersUseCase {
    suspend fun execute(): Int
}
