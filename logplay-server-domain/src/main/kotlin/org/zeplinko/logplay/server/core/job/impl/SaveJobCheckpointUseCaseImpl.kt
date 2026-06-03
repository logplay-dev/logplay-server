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
        val result =
            jobGateway.saveCheckpoint(command.jobId, command.workerId, now) { lastCheckpoint ->
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
        return when (result) {
            is SaveCheckpointResult.Success -> result.checkpoint
            is SaveCheckpointResult.NotFound -> throw JobNotFoundException(command.jobId)
            is SaveCheckpointResult.WrongStatus ->
                throw JobNotAcquiredException(command.jobId, result.status)
            is SaveCheckpointResult.WrongWorker ->
                throw JobNotOwnedByWorkerException(command.jobId, command.workerId)
        }
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
