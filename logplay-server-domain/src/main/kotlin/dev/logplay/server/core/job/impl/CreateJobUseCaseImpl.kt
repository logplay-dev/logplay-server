package dev.logplay.server.core.job.impl

import dev.logplay.server.core.job.*
import java.time.Instant
import java.util.*

class CreateJobUseCaseImpl(private val jobGateway: JobGateway) : CreateJobUseCase {
    override suspend fun execute(createJobCommand: CreateJobCommand): Job {
        if (createJobCommand.type.isBlank()) throw BlankJobTypeException()
        val currentTime = Instant.now()
        val job =
            Job(
                id = UUID.randomUUID().toString(),
                name = createJobCommand.name.ifBlank { UUID.randomUUID().toString() },
                type = createJobCommand.type,
                status = JobStatus.PENDING,
                retries = 0,
                createdAt = currentTime,
                updatedAt = currentTime,
                version = 1,
            )
        return jobGateway.insertJob(job)
    }
}
