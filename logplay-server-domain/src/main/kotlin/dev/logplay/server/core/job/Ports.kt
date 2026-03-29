package dev.logplay.server.core.job

interface JobGateway {
    suspend fun insertJob(job: Job): Job

    suspend fun updateJob(job: Job): Job

    suspend fun getPendingJobs(limit: Int): List<Job>

    suspend fun findJobById(id: String): Job?

    suspend fun saveCheckpoint(checkpoint: Checkpoint): Checkpoint
}
