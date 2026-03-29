package dev.logplay.server.core.job.impl

import dev.logplay.server.core.job.*
import java.time.Instant
import java.util.UUID

class SaveJobCheckpointUseCaseImpl(private val jobGateway: JobGateway) : SaveJobCheckpointUseCase {
    override suspend fun execute(command: SaveJobCheckpointCommand): Checkpoint {
        validate(command)
        val job = jobGateway.findJobById(command.jobId) ?: throw JobNotFoundException(command.jobId)
        if (job.status != JobStatus.PENDING) throw JobNotPendingException(command.jobId, job.status)
        val checkpoint =
            Checkpoint(
                id = UUID.randomUUID().toString(),
                jobId = command.jobId,
                classType = command.classType,
                description = command.description,
                createdAt = Instant.now(),
                data = command.data,
            )
        return jobGateway.saveCheckpoint(checkpoint)
    }

    private fun validate(command: SaveJobCheckpointCommand) {
        if (command.jobId.isBlank()) throw BlankJobIdException()
        if (command.classType.isBlank()) throw BlankCheckpointClassTypeException()
        if (command.description != null && command.description.isBlank())
            throw BlankCheckpointDescriptionException()
    }
}
