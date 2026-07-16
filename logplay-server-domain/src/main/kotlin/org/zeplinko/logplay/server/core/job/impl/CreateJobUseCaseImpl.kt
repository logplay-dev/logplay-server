package org.zeplinko.logplay.server.core.job.impl

import java.time.Instant
import java.util.*
import org.zeplinko.logplay.server.core.Metrics
import org.zeplinko.logplay.server.core.NoopMetrics
import org.zeplinko.logplay.server.core.UnitOfWork
import org.zeplinko.logplay.server.core.job.*

class CreateJobUseCaseImpl(
    private val jobGateway: JobGateway,
    private val unitOfWork: UnitOfWork,
    private val metrics: Metrics = NoopMetrics,
) : CreateJobUseCase {
    companion object {
        const val MAX_GROUP_ID_LENGTH = 64
        const val MAX_NAME_LENGTH = 256
        const val MAX_TYPE_LENGTH = 512
        const val MAX_IDEMPOTENCY_KEY_LENGTH = 64
    }

    override suspend fun execute(createJobCommand: CreateJobCommand): Job {
        if (createJobCommand.groupId.isBlank()) throw BlankGroupIdException()
        if (createJobCommand.groupId.length > MAX_GROUP_ID_LENGTH)
            throw InvalidGroupIdException("groupId must not exceed $MAX_GROUP_ID_LENGTH characters")
        if (createJobCommand.idempotencyKey.isBlank()) throw BlankIdempotencyKeyException()
        if (createJobCommand.idempotencyKey.length > MAX_IDEMPOTENCY_KEY_LENGTH)
            throw InvalidIdempotencyKeyException(
                "idempotencyKey must not exceed $MAX_IDEMPOTENCY_KEY_LENGTH characters"
            )
        if (createJobCommand.type.isBlank()) throw BlankJobTypeException()
        if (createJobCommand.type.length > MAX_TYPE_LENGTH)
            throw InvalidJobTypeException("type must not exceed $MAX_TYPE_LENGTH characters")
        if (createJobCommand.name.isNotBlank() && createJobCommand.name.length > MAX_NAME_LENGTH)
            throw InvalidJobNameException("name must not exceed $MAX_NAME_LENGTH characters")
        if (createJobCommand.maxRetries != null && createJobCommand.maxRetries <= 0)
            throw InvalidMaxRetriesException()
        val currentTime = Instant.now()
        val newJob =
            NewJob(
                id =
                    JobIdGenerator.fromIdempotencyKey(
                        createJobCommand.groupId,
                        createJobCommand.idempotencyKey,
                    ),
                groupId = createJobCommand.groupId,
                name = createJobCommand.name.ifBlank { UUID.randomUUID().toString() },
                type = createJobCommand.type,
                maxRetries = createJobCommand.maxRetries,
                inputData = createJobCommand.inputData,
                createdAt = currentTime,
            )
        val event = JobEvent.system(newJob.id, JobEventType.CREATED, currentTime)
        return try {
            val created =
                unitOfWork.transaction {
                    jobGateway.insertJob(newJob)
                    jobGateway.insertEvents(listOf(event))
                    newJob.toPendingJob()
                }
            metrics.onJobCreated()
            created
        } catch (e: DuplicateJobIdException) {
            metrics.onJobDuplicateRejected()
            throw DuplicateIdempotencyKeyException(
                createJobCommand.groupId,
                createJobCommand.idempotencyKey,
            )
        }
    }

    private fun NewJob.toPendingJob(): Job =
        Job(
            id = id,
            groupId = groupId,
            name = name,
            type = type,
            status = JobStatus.PENDING,
            retries = 0,
            maxRetries = maxRetries,
            inputData = inputData,
            createdAt = createdAt,
            updatedAt = createdAt,
            lastAcquiredAt = null,
            acquiredByWorkerId = null,
            availableAt = 0L,
            terminalAt = null,
            outputData = null,
        )
}
