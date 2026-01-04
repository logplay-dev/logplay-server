package dev.logplay.server.core.job.impl

import dev.logplay.server.core.job.GetPendingJobsCommand
import dev.logplay.server.core.job.GetPendingJobsUseCase
import dev.logplay.server.core.job.Job
import dev.logplay.server.core.job.JobManager

class GetPendingJobsUseCaseImpl(private val jobManager: JobManager) : GetPendingJobsUseCase {
    override suspend fun execute(getPendingJobsCommand: GetPendingJobsCommand): List<Job> {
        return jobManager.getPendingJobs(getPendingJobsCommand.limit)
    }
}
