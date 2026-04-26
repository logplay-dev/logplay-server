package org.zeplinko.logplay.server.job.web

import java.util.Base64
import org.zeplinko.logplay.server.core.job.*
import org.zeplinko.logplay.server.core.worker.BlankWorkerIdException

/**
 * Request body for `POST /api/v1/jobs`. All fields are wire-nullable so missing required fields
 * surface as specific domain exceptions in [toCommand]. `inputData` is Base64-encoded.
 */
data class CreateJobRequest(
    val groupId: String? = null,
    val name: String? = null,
    val type: String? = null,
    val maxRetries: Int? = null,
    val idempotencyKey: String? = null,
    val inputData: String? = null,
)

/** Request body for `POST /api/v1/jobs/acquire`. */
data class AcquireJobsRequest(
    val groupId: String? = null,
    val type: String? = null,
    val workerId: String? = null,
    val limit: Int? = null,
)

/** Request body for `POST /api/v1/jobs/:jobId/complete`. `outputData` is Base64-encoded. */
data class CompleteJobRequest(val workerId: String? = null, val outputData: String? = null)

/** Request body for `POST /api/v1/jobs/:jobId/release`. */
data class ReleaseJobRequest(val workerId: String? = null)

/**
 * Request body for `POST /api/v1/jobs/:jobId/error`. The optional `error` is recorded on the
 * resulting `ERROR_REPORTED` event for forensics.
 */
data class ReportExecutionErrorRequest(val workerId: String? = null, val error: String? = null)

/**
 * Request body for `POST /api/v1/jobs/:jobId/checkpoints`. `data` is Base64-encoded; both `data`
 * and `name` are optional.
 */
data class SaveCheckpointRequest(
    val workerId: String? = null,
    val previousCheckpointId: String? = null,
    val name: String? = null,
    val data: String? = null,
)

/**
 * Response shape for every endpoint that returns a single [Job]. Timestamps are ISO-8601;
 * `inputData` and `outputData` are Base64 when present.
 */
data class JobResponse(
    val id: String,
    val groupId: String,
    val name: String,
    val type: String,
    val status: String,
    val retries: Int,
    val maxRetries: Int?,
    val idempotencyKey: String,
    val createdAt: String,
    val updatedAt: String,
    val lastAcquiredAt: String?,
    val acquiredByWorkerId: String?,
    val inputData: String?,
    val outputData: String?,
    val version: Long,
)

/**
 * Response shape for a single [Checkpoint]. `data` is Base64 when present, `null` for chain markers
 * without a payload.
 */
data class CheckpointResponse(
    val id: String,
    val jobId: String,
    val previousCheckpointId: String?,
    val name: String?,
    val createdAt: String,
    val data: String?,
)

/**
 * Response shape for `GET /api/v1/jobs/:jobId/checkpoints`. Caller pages by passing the last
 * checkpoint's id as the next request's `after` cursor while `hasMore` is true.
 */
data class CheckpointPageResponse(val checkpoints: List<CheckpointResponse>, val hasMore: Boolean)

/**
 * Validates required fields and Base64-decodes `inputData` if present.
 *
 * @throws BlankGroupIdException, BlankJobTypeException, BlankIdempotencyKeyException for missing
 *   required fields.
 * @throws InvalidJobInputDataException if `inputData` is not valid Base64.
 */
fun CreateJobRequest.toCommand() =
    try {
        CreateJobCommand(
            groupId = groupId ?: throw BlankGroupIdException(),
            name = name ?: "",
            type = type ?: throw BlankJobTypeException(),
            maxRetries = maxRetries,
            idempotencyKey = idempotencyKey ?: throw BlankIdempotencyKeyException(),
            inputData = inputData?.let { Base64.getDecoder().decode(it) },
        )
    } catch (_: IllegalArgumentException) {
        throw InvalidJobInputDataException()
    }

/**
 * Validates required fields. `limit` defaults to 10 when absent.
 *
 * @throws BlankGroupIdException, BlankJobTypeException, BlankWorkerIdException for missing fields.
 */
fun AcquireJobsRequest.toCommand() =
    AcquirePendingJobsCommand(
        groupId = groupId ?: throw BlankGroupIdException(),
        type = type ?: throw BlankJobTypeException(),
        workerId = workerId ?: throw BlankWorkerIdException(),
        limit = limit ?: 10,
    )

/**
 * Validates required fields and Base64-decodes `outputData` if present.
 *
 * @throws BlankWorkerIdException, InvalidJobOutputDataException.
 */
fun CompleteJobRequest.toCommand(jobId: String) =
    try {
        CompleteJobCommand(
            jobId = jobId,
            workerId = workerId ?: throw BlankWorkerIdException(),
            outputData = outputData?.let { Base64.getDecoder().decode(it) },
        )
    } catch (_: IllegalArgumentException) {
        throw InvalidJobOutputDataException()
    }

/** @throws BlankWorkerIdException if `workerId` is missing. */
fun ReleaseJobRequest.toCommand(jobId: String) =
    ReleaseJobCommand(jobId = jobId, workerId = workerId ?: throw BlankWorkerIdException())

/** @throws BlankWorkerIdException if `workerId` is missing. */
fun ReportExecutionErrorRequest.toCommand(jobId: String) =
    ReportExecutionErrorCommand(
        jobId = jobId,
        workerId = workerId ?: throw BlankWorkerIdException(),
        error = error,
    )

/**
 * Validates required fields and Base64-decodes `data` if present. A null/omitted `data` value is
 * passed through to the domain as `null` (chain marker without payload).
 *
 * @throws BlankWorkerIdException, InvalidCheckpointDataException.
 */
fun SaveCheckpointRequest.toCommand(jobId: String) =
    try {
        SaveJobCheckpointCommand(
            jobId = jobId,
            workerId = workerId ?: throw BlankWorkerIdException(),
            previousCheckpointId = previousCheckpointId,
            name = name,
            data = data?.let { Base64.getDecoder().decode(it) },
        )
    } catch (_: IllegalArgumentException) {
        throw InvalidCheckpointDataException()
    }

/** Converts a domain [Job] to its wire response, Base64-encoding `inputData` and `outputData`. */
fun Job.toResponse() =
    JobResponse(
        id = id,
        groupId = groupId,
        name = name,
        type = type,
        status = status.name,
        retries = retries,
        maxRetries = maxRetries,
        idempotencyKey = idempotencyKey,
        createdAt = createdAt.toString(),
        updatedAt = updatedAt.toString(),
        lastAcquiredAt = lastAcquiredAt?.toString(),
        acquiredByWorkerId = acquiredByWorkerId,
        inputData = inputData?.let { Base64.getEncoder().encodeToString(it) },
        outputData = outputData?.let { Base64.getEncoder().encodeToString(it) },
        version = version,
    )

/** Converts a [CheckpointPage] to its wire response. */
fun CheckpointPage.toResponse() =
    CheckpointPageResponse(checkpoints = checkpoints.map { it.toResponse() }, hasMore = hasMore)

/** Converts a [Checkpoint] to its wire response, Base64-encoding `data` when non-null. */
fun Checkpoint.toResponse() =
    CheckpointResponse(
        id = id,
        jobId = jobId,
        previousCheckpointId = previousCheckpointId,
        name = name,
        createdAt = createdAt.toString(),
        data = data?.let { Base64.getEncoder().encodeToString(it) },
    )

/**
 * Response shape for `GET /api/v1/jobs/:jobId/events`. `eventType` and `actorType` are the `name()`
 * of the corresponding domain enum; `createdAt` is ISO-8601.
 */
data class JobEventResponse(
    val id: String,
    val jobId: String,
    val eventType: String,
    val actorType: String,
    val actorId: String?,
    val createdAt: String,
    val eventMessage: String?,
    val eventDetail: String?,
)

/** Converts a [JobEvent] to its wire response, stringifying enum values and the timestamp. */
fun JobEvent.toResponse() =
    JobEventResponse(
        id = id,
        jobId = jobId,
        eventType = eventType.name,
        actorType = actorType.name,
        actorId = actorId,
        createdAt = createdAt.toString(),
        eventMessage = eventMessage,
        eventDetail = eventDetail,
    )
