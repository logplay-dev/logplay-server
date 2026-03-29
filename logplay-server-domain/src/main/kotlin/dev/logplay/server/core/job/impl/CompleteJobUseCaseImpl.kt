package dev.logplay.server.core.job.impl

import dev.logplay.server.core.job.*
import java.time.Instant

class CompleteJobUseCaseImpl(private val jobGateway: JobGateway) : CompleteJobUseCase {
    override suspend fun execute(command: CompleteJobCommand): Job {
        if (command.jobId.isBlank()) throw BlankJobIdException()
        val job = jobGateway.findJobById(command.jobId) ?: throw JobNotFoundException(command.jobId)
        if (job.status != JobStatus.PENDING) throw JobNotPendingException(command.jobId, job.status)
        return jobGateway.updateJob(
            job.copy(status = JobStatus.FINISHED, updatedAt = Instant.now())
        )
    }
}
