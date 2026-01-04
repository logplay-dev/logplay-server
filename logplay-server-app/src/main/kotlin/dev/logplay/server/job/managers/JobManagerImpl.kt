package dev.logplay.server.job.managers

import dev.logplay.server.core.job.Job
import dev.logplay.server.core.job.JobManager

class InMemoryJobManagerImpl() : JobManager {
    override suspend fun insertJob(job: Job): Job {
        TODO("Not yet implemented")
    }

    override suspend fun updateJob(job: Job): Job {
        TODO("Not yet implemented")
    }

    override suspend fun getPendingJobs(limit: Int): List<Job> {
        TODO("Not yet implemented")
    }
}
