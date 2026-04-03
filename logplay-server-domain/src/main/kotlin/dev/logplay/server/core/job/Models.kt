package dev.logplay.server.core.job

import java.time.Instant

data class Job(
    val id: String,
    val name: String,
    val type: String,
    val status: JobStatus,
    val retries: Int,
    val maxRetries: Int? = null,
    val idempotencyKey: String,
    val inputData: ByteArray? = null,
    val createdAt: Instant,
    val updatedAt: Instant,
    val lastAcquiredAt: Instant? = null,
    val acquiredByWorkerId: String? = null,
    val outputData: ByteArray? = null,
    val version: Long,
)

data class Checkpoint(
    val id: String,
    val jobId: String,
    val previousCheckpointId: String?,
    val name: String?,
    val createdAt: Instant,
    val orderKey: Long,
    val data: ByteArray,
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Checkpoint

        return id == other.id
    }

    override fun hashCode(): Int {
        return id.hashCode()
    }
}

data class CreateJobCommand(
    val name: String,
    val type: String,
    val maxRetries: Int? = null,
    val idempotencyKey: String,
    val inputData: ByteArray? = null,
)

data class AcquirePendingJobsCommand(val workerId: String, val limit: Int)

data class SaveJobCheckpointCommand(
    val jobId: String,
    val workerId: String,
    val previousCheckpointId: String?,
    val name: String?,
    val data: ByteArray,
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as SaveJobCheckpointCommand

        if (jobId != other.jobId) return false
        if (workerId != other.workerId) return false
        if (previousCheckpointId != other.previousCheckpointId) return false
        if (name != other.name) return false
        if (!data.contentEquals(other.data)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = jobId.hashCode()
        result = 31 * result + workerId.hashCode()
        result = 31 * result + (previousCheckpointId?.hashCode() ?: 0)
        result = 31 * result + (name?.hashCode() ?: 0)
        result = 31 * result + data.contentHashCode()
        return result
    }
}

data class GetCheckpointsCommand(val jobId: String, val after: String?, val limit: Int?)

data class CheckpointPage(val checkpoints: List<Checkpoint>, val hasMore: Boolean)

data class CompleteJobCommand(
    val jobId: String,
    val workerId: String,
    val outputData: ByteArray? = null,
)

data class ReleaseJobCommand(val jobId: String, val workerId: String)

data class ReportExecutionErrorCommand(val jobId: String, val workerId: String, val error: String?)

data class AbortJobCommand(val jobId: String)

data class ExecutionErrorResult(val status: JobStatus, val retries: Int, val events: List<JobEvent>)

data class JobEvent(
    val id: String,
    val jobId: String,
    val eventType: JobEventType,
    val actorType: ActorType,
    val actorId: String?,
    val createdAt: Instant,
    val eventMessage: String?,
    val eventDetail: String?,
)
