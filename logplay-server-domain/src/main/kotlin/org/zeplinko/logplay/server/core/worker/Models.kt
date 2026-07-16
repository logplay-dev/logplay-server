package org.zeplinko.logplay.server.core.worker

import java.time.Instant

/**
 * A registered execution context that acquires and processes jobs.
 *
 * @property id caller-chosen unique identifier.
 * @property heartbeatTimeout expected interval between heartbeats, in ms.
 * @property sessionTimeout time without a heartbeat after which the worker is considered dead, in
 *   ms; required to be greater than [heartbeatTimeout]. A worker is alive while `now -
 *   lastHeartbeatAt <= sessionTimeout`; once dead it is reclaimed by cleanup and must re-register.
 */
data class Worker(
    val id: String,
    val heartbeatTimeout: Long,
    val sessionTimeout: Long,
    val lastHeartbeatAt: Instant,
    val registeredAt: Instant,
)

/** True if the worker has not heartbeated within its [Worker.sessionTimeout] as of [now]. */
fun Worker.isDeadAt(now: Instant): Boolean =
    now.toEpochMilli() - lastHeartbeatAt.toEpochMilli() > sessionTimeout

/** Input for [RegisterWorkerUseCase]. */
data class RegisterWorkerCommand(
    val workerId: String,
    val heartbeatTimeout: Long,
    val sessionTimeout: Long,
)

/** Input for [HeartbeatWorkerUseCase]. */
data class HeartbeatWorkerCommand(val workerId: String)

/** Input for [DeregisterWorkerUseCase]. */
data class DeregisterWorkerCommand(val workerId: String)
