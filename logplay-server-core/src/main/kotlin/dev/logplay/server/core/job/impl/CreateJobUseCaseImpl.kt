package dev.logplay.server.core.job.impl

import dev.logplay.server.core.job.*
import java.time.Instant
import java.util.*

class CreateJobUseCaseImpl(private val jobManager: JobManager) : CreateJobUseCase {
    override suspend fun execute(createJobCommand: CreateJobCommand): Job {
        require(createJobCommand.type.isNotBlank()) { throw BlankJobTypeException() }
        val currentTime = Instant.now()
        val job =
            Job(
                id = UUID.randomUUID().toString(),
                name = createJobCommand.name.ifBlank { UUID.randomUUID().toString() },
                type = createJobCommand.type,
                status = JobStatus.QUEUED,
                retries = 0,
                createdAt = currentTime,
                updatedAt = currentTime,
                version = 1,
            )
        return jobManager.insertJob(job)
    }
}
