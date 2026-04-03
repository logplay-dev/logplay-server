package dev.logplay.server.job.web

import dev.logplay.server.core.job.*
import dev.logplay.server.core.worker.BlankWorkerIdException
import java.util.Base64
import java.util.UUID

data class CreateJobRequest(
    val name: String? = null,
    val type: String? = null,
    val maxRetries: Int? = null,
    val idempotencyKey: String? = null,
    val inputData: String? = null,
)

data class AcquireJobsRequest(val workerId: String? = null, val limit: Int? = null)

data class CompleteJobRequest(val workerId: String? = null, val outputData: String? = null)

data class ReleaseJobRequest(val workerId: String? = null)

data class ReportExecutionErrorRequest(val workerId: String? = null, val error: String? = null)

data class SaveCheckpointRequest(
    val workerId: String? = null,
    val previousCheckpointId: String? = null,
    val name: String? = null,
    val data: String? = null,
)

data class JobResponse(
    val id: String,
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

data class CheckpointResponse(
    val id: String,
    val jobId: String,
    val previousCheckpointId: String?,
    val name: String?,
    val createdAt: String,
    val data: String,
)

data class CheckpointPageResponse(val checkpoints: List<CheckpointResponse>, val hasMore: Boolean)

fun CreateJobRequest.toCommand() =
    try {
        CreateJobCommand(
            name = name ?: "",
            type = type ?: throw BlankJobTypeException(),
            maxRetries = maxRetries,
            idempotencyKey = idempotencyKey ?: UUID.randomUUID().toString(),
            inputData = inputData?.let { Base64.getDecoder().decode(it) },
        )
    } catch (_: IllegalArgumentException) {
        throw InvalidJobInputDataException()
    }

fun AcquireJobsRequest.toCommand() =
    AcquirePendingJobsCommand(
        workerId = workerId ?: throw BlankWorkerIdException(),
        limit = limit ?: 10,
    )

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

fun ReleaseJobRequest.toCommand(jobId: String) =
    ReleaseJobCommand(jobId = jobId, workerId = workerId ?: throw BlankWorkerIdException())

fun ReportExecutionErrorRequest.toCommand(jobId: String) =
    ReportExecutionErrorCommand(
        jobId = jobId,
        workerId = workerId ?: throw BlankWorkerIdException(),
        error = error,
    )

fun SaveCheckpointRequest.toCommand(jobId: String) =
    try {
        SaveJobCheckpointCommand(
            jobId = jobId,
            workerId = workerId ?: throw BlankWorkerIdException(),
            previousCheckpointId = previousCheckpointId,
            name = name,
            data = Base64.getDecoder().decode(data ?: throw InvalidCheckpointDataException()),
        )
    } catch (_: IllegalArgumentException) {
        throw InvalidCheckpointDataException()
    }

fun Job.toResponse() =
    JobResponse(
        id = id,
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

fun CheckpointPage.toResponse() =
    CheckpointPageResponse(checkpoints = checkpoints.map { it.toResponse() }, hasMore = hasMore)

fun Checkpoint.toResponse() =
    CheckpointResponse(
        id = id,
        jobId = jobId,
        previousCheckpointId = previousCheckpointId,
        name = name,
        createdAt = createdAt.toString(),
        data = Base64.getEncoder().encodeToString(data),
    )

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
