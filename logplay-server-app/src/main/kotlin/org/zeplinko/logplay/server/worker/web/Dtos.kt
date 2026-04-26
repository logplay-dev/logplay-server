package org.zeplinko.logplay.server.worker.web

import org.zeplinko.logplay.server.core.worker.BlankWorkerIdException
import org.zeplinko.logplay.server.core.worker.InvalidWorkerTimeoutException
import org.zeplinko.logplay.server.core.worker.RegisterWorkerCommand
import org.zeplinko.logplay.server.core.worker.Worker

/**
 * Request body for `POST /api/v1/workers`. All fields are nullable on the wire so omissions can be
 * reported as specific 400-class domain exceptions; required-field enforcement happens in
 * [toCommand].
 */
data class RegisterWorkerRequest(
    val workerId: String? = null,
    val heartbeatTimeout: Long? = null,
    val sessionTimeout: Long? = null,
)

/**
 * Response shape for worker endpoints. Timestamps are ISO-8601 strings derived from
 * `Instant.toString()`.
 */
data class WorkerResponse(
    val id: String,
    val heartbeatTimeout: Long,
    val sessionTimeout: Long,
    val lastHeartbeatAt: String,
    val registeredAt: String,
)

/**
 * Validates required fields and converts the request to its domain command.
 *
 * @throws BlankWorkerIdException if `workerId` is missing.
 * @throws InvalidWorkerTimeoutException if either timeout field is missing.
 */
fun RegisterWorkerRequest.toCommand() =
    RegisterWorkerCommand(
        workerId = workerId ?: throw BlankWorkerIdException(),
        heartbeatTimeout =
            heartbeatTimeout ?: throw InvalidWorkerTimeoutException("heartbeatTimeout is required"),
        sessionTimeout =
            sessionTimeout ?: throw InvalidWorkerTimeoutException("sessionTimeout is required"),
    )

/** Converts a domain [Worker] to its wire response, formatting timestamps as ISO-8601. */
fun Worker.toResponse() =
    WorkerResponse(
        id = id,
        heartbeatTimeout = heartbeatTimeout,
        sessionTimeout = sessionTimeout,
        lastHeartbeatAt = lastHeartbeatAt.toString(),
        registeredAt = registeredAt.toString(),
    )
