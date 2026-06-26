package org.zeplinko.logplay.server.core.job.impl

import java.time.Instant
import org.zeplinko.logplay.server.core.UnitOfWork
import org.zeplinko.logplay.server.core.job.*
import org.zeplinko.logplay.server.core.worker.BlankWorkerIdException

class SaveJobCheckpointUseCaseImpl(
    private val jobGateway: JobGateway,
    private val unitOfWork: UnitOfWork,
) : SaveJobCheckpointUseCase {

    companion object {
        const val MAX_NAME_LENGTH = 256
    }

    override suspend fun execute(command: SaveJobCheckpointCommand): Checkpoint {
        validate(command)
        val now = Instant.now()
        return unitOfWork.transaction {
            val job =
                jobGateway.findAndLockJobById(command.jobId)
                    ?: throw JobNotFoundException(command.jobId)
            when (job.status) {
                JobStatus.ACQUIRED -> Unit
                JobStatus.PENDING,
                JobStatus.FINISHED,
                JobStatus.FAILED,
                JobStatus.ABORTED -> throw JobNotAcquiredException(command.jobId, job.status)
            }
            if (job.acquiredByWorkerId != command.workerId)
                throw JobNotOwnedByWorkerException(command.jobId, command.workerId)
            val tail = jobGateway.latestCheckpoint(command.jobId)
            if (tail?.id != command.previousCheckpointId)
                throw InvalidCheckpointOrderException(command.jobId)
            val checkpoint =
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
                    orderKey = (tail?.orderKey ?: 0) + 1,
                    data = command.data,
                )
            jobGateway.insertCheckpoint(checkpoint)
            jobGateway.resetAcquiredRetries(command.jobId)
            checkpoint
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
