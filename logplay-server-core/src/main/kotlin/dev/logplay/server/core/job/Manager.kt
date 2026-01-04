package dev.logplay.server.core.job

interface JobManager {
    suspend fun insertJob(job: Job): Job

    suspend fun updateJob(job: Job): Job

    suspend fun getPendingJobs(limit: Int): List<Job>
}
