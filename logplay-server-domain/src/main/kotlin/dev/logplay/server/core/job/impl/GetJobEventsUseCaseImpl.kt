package dev.logplay.server.core.job.impl

import dev.logplay.server.core.job.*

class GetJobEventsUseCaseImpl(private val jobGateway: JobGateway) : GetJobEventsUseCase {
    override suspend fun execute(jobId: String): List<JobEvent> {
        if (jobId.isBlank()) throw BlankJobIdException()
        jobGateway.findJobById(jobId) ?: throw JobNotFoundException(jobId)
        return jobGateway.findEventsByJobId(jobId)
    }
}
