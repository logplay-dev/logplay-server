package dev.logplay.server.core.job

import java.time.Instant

data class Job(
    val id: String,
    val name: String,
    val type: String,
    val status: JobStatus,
    val retries: Int,
    val createdAt: Instant,
    val updatedAt: Instant,
    val version: Long,
)

data class Checkpoint(
    val id: String,
    val jobId: String,
    val classType: String,
    val description: String?,
    val createdAt: Instant,
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

data class CreateJobCommand(val name: String, val type: String)

data class GetPendingJobsCommand(val limit: Int)

data class SaveJobCheckpointCommand(
    val jobId: String,
    val classType: String,
    val description: String?,
    val data: ByteArray,
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as SaveJobCheckpointCommand

        if (jobId != other.jobId) return false
        if (classType != other.classType) return false
        if (description != other.description) return false
        if (!data.contentEquals(other.data)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = jobId.hashCode()
        result = 31 * result + classType.hashCode()
        result = 31 * result + (description?.hashCode() ?: 0)
        result = 31 * result + data.contentHashCode()
        return result
    }
}

data class CompleteJobCommand(val jobId: String)
