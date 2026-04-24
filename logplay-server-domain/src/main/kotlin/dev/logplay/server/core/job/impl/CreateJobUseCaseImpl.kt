package dev.logplay.server.core.job.impl

import dev.logplay.server.core.job.*
import java.time.Instant
import java.util.*

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
        val job =
            Job(
                id =
                    JobIdGenerator.fromIdempotencyKey(
                        createJobCommand.groupId,
                        createJobCommand.idempotencyKey,
                    ),
                groupId = createJobCommand.groupId,
                name = createJobCommand.name.ifBlank { UUID.randomUUID().toString() },
                type = createJobCommand.type,
                status = JobStatus.PENDING,
                retries = 0,
                maxRetries = createJobCommand.maxRetries,
                idempotencyKey = createJobCommand.idempotencyKey,
                inputData = createJobCommand.inputData,
                createdAt = currentTime,
                updatedAt = currentTime,
                version = 1,
            )
        val event =
            JobEvent(
                id = UUID.randomUUID().toString(),
                jobId = job.id,
                eventType = JobEventType.CREATED,
                actorType = ActorType.SYSTEM,
                actorId = null,
                createdAt = currentTime,
                eventMessage = null,
                eventDetail = null,
            )
        return jobGateway.insertJobWithEvent(job, event)
    }
}
