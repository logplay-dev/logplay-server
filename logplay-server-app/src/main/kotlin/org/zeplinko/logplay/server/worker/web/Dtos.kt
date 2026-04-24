package org.zeplinko.logplay.server.worker.web

import org.zeplinko.logplay.server.core.worker.BlankWorkerIdException
import org.zeplinko.logplay.server.core.worker.InvalidWorkerTimeoutException
import org.zeplinko.logplay.server.core.worker.RegisterWorkerCommand
import org.zeplinko.logplay.server.core.worker.Worker

data class RegisterWorkerRequest(
    val workerId: String? = null,
    val heartbeatTimeout: Long? = null,
    val sessionTimeout: Long? = null,
)

data class WorkerResponse(
    val id: String,
    val heartbeatTimeout: Long,
    val sessionTimeout: Long,
    val lastHeartbeatAt: String,
    val registeredAt: String,
)

fun RegisterWorkerRequest.toCommand() =
    RegisterWorkerCommand(
        workerId = workerId ?: throw BlankWorkerIdException(),
        heartbeatTimeout =
            heartbeatTimeout ?: throw InvalidWorkerTimeoutException("heartbeatTimeout is required"),
        sessionTimeout =
            sessionTimeout ?: throw InvalidWorkerTimeoutException("sessionTimeout is required"),
    )

fun Worker.toResponse() =
    WorkerResponse(
        id = id,
        heartbeatTimeout = heartbeatTimeout,
        sessionTimeout = sessionTimeout,
        lastHeartbeatAt = lastHeartbeatAt.toString(),
        registeredAt = registeredAt.toString(),
    )
