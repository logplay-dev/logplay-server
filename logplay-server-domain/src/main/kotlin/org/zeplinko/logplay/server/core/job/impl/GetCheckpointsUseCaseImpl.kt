package org.zeplinko.logplay.server.core.job.impl

import org.zeplinko.logplay.server.core.job.*

class GetCheckpointsUseCaseImpl(private val jobGateway: JobGateway) : GetCheckpointsUseCase {

    companion object {
        const val MAX_LIMIT = 100
        const val DEFAULT_LIMIT = 20
    }

    override suspend fun execute(command: GetCheckpointsCommand): CheckpointPage {
        if (command.jobId.isBlank()) throw BlankJobIdException()
        jobGateway.findJobById(command.jobId) ?: throw JobNotFoundException(command.jobId)

        val limit = command.limit ?: DEFAULT_LIMIT
        if (limit !in 1..MAX_LIMIT) throw InvalidLimitException(MAX_LIMIT)

        val afterOrderKey =
            if (command.after != null) {
                val cursor =
                    jobGateway.findCheckpointById(command.after)
                        ?: throw CheckpointNotFoundException(command.after)
                if (cursor.jobId != command.jobId) throw CheckpointNotFoundException(command.after)
                cursor.orderKey
            } else {
                null
            }

        val checkpoints = jobGateway.findCheckpointsByJobId(command.jobId, afterOrderKey, limit + 1)
        val hasMore = checkpoints.size > limit
        return CheckpointPage(
            checkpoints = if (hasMore) checkpoints.take(limit) else checkpoints,
            hasMore = hasMore,
        )
    }
}
