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

enum class JobStatus {
    QUEUED,
    RUNNING,
    FINISHED,
    FAILED,
    ABORTED,
}

data class Checkpoint(
    val id: String,
    val jobId: String,
    val description: String,
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
