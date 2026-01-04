package dev.logplay.server.core.job.impl

import dev.logplay.server.core.job.JobManager
import dev.logplay.server.core.job.SaveJobCheckpointUseCase

class SaveJobCheckpointUseCaseImpl(private val jobManager: JobManager) : SaveJobCheckpointUseCase {
    override suspend fun execute() {
        TODO("Not yet implemented")
    }
}
