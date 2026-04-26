package org.zeplinko.logplay.server.core.job.impl

import java.time.Instant
import java.util.*
import org.zeplinko.logplay.server.core.job.*

class CreateJobUseCaseImpl(private val jobGateway: JobGateway) : CreateJobUseCase {
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
        val event =
            JobEvent(
                id = UUID.randomUUID().toString(),
                jobId = newJob.id,
                eventType = JobEventType.CREATED,
                actorType = ActorType.SYSTEM,
                actorId = null,
                createdAt = currentTime,
                eventMessage = null,
                eventDetail = null,
            )
        return when (val result = jobGateway.insertJobWithEvent(newJob, event)) {
            is InsertJobWithEventResult.Success -> result.job
            is InsertJobWithEventResult.AlreadyExists ->
                throw DuplicateIdempotencyKeyException(
                    createJobCommand.groupId,
                    createJobCommand.idempotencyKey,
                )
        }
    }
}
