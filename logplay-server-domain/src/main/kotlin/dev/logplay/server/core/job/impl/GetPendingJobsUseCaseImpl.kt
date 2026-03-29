package dev.logplay.server.core.job.impl

import dev.logplay.server.core.job.GetPendingJobsCommand
import dev.logplay.server.core.job.GetPendingJobsUseCase
import dev.logplay.server.core.job.Job
import dev.logplay.server.core.job.JobGateway

class GetPendingJobsUseCaseImpl(private val jobGateway: JobGateway) : GetPendingJobsUseCase {
    override suspend fun execute(getPendingJobsCommand: GetPendingJobsCommand): List<Job> {
        return jobGateway.getPendingJobs(getPendingJobsCommand.limit)
    }
}
