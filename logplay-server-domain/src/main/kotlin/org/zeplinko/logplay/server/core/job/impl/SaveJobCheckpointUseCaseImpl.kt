package org.zeplinko.logplay.server.core.job.impl

import java.time.Instant
import org.zeplinko.logplay.server.core.job.*
import org.zeplinko.logplay.server.core.worker.BlankWorkerIdException

class SaveJobCheckpointUseCaseImpl(private val jobGateway: JobGateway) : SaveJobCheckpointUseCase {

    companion object {
        const val MAX_NAME_LENGTH = 256
    }

    override suspend fun execute(command: SaveJobCheckpointCommand): Checkpoint {
        validate(command)
        val now = Instant.now()
        val saved =
            jobGateway.saveCheckpoint(command.jobId, command.workerId, now) { _, lastCheckpoint ->
                if (lastCheckpoint?.id != command.previousCheckpointId)
                    throw InvalidCheckpointOrderException(command.jobId)
                Checkpoint(
                    id =
                        CheckpointIdGenerator.fromChainPosition(
                            command.jobId,
                            command.previousCheckpointId,
                        ),
                    jobId = command.jobId,
                    previousCheckpointId = command.previousCheckpointId,
                    name = command.name,
                    createdAt = now,
                    orderKey = (lastCheckpoint?.orderKey ?: 0) + 1,
                    data = command.data,
                )
            }
        if (saved != null) return saved
        val job = jobGateway.findJobById(command.jobId) ?: throw JobNotFoundException(command.jobId)
        if (job.status != JobStatus.ACQUIRED)
            throw JobNotAcquiredException(command.jobId, job.status)
        if (job.acquiredByWorkerId != command.workerId)
            throw JobNotOwnedByWorkerException(command.jobId, command.workerId)
        throw JobConcurrentModificationException(command.jobId)
    }

    private fun validate(command: SaveJobCheckpointCommand) {
        if (command.jobId.isBlank()) throw BlankJobIdException()
        if (command.workerId.isBlank()) throw BlankWorkerIdException()
        if (command.name != null && command.name.isBlank())
            throw InvalidCheckpointNameException("checkpoint name cannot be blank")
        if (command.name != null && command.name.length > MAX_NAME_LENGTH)
            throw InvalidCheckpointNameException(
                "checkpoint name must not exceed $MAX_NAME_LENGTH characters"
            )
    }
}
