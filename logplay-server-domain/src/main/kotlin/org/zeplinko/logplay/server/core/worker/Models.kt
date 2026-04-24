package org.zeplinko.logplay.server.core.worker

import java.time.Instant

data class Worker(
    val id: String,
    val heartbeatTimeout: Long,
    val sessionTimeout: Long,
    val lastHeartbeatAt: Instant,
    val registeredAt: Instant,
    val condemned: Boolean = false,
)

data class RegisterWorkerCommand(
    val workerId: String,
    val heartbeatTimeout: Long,
    val sessionTimeout: Long,
)

data class HeartbeatWorkerCommand(val workerId: String)

data class DeregisterWorkerCommand(val workerId: String)
