package dev.logplay.server.job.use.case

import dev.logplay.server.core.job.CreateJobUseCase
import dev.logplay.server.core.job.GetPendingJobsUseCase
import dev.logplay.server.core.job.JobManager
import dev.logplay.server.core.job.SaveJobCheckpointUseCase
import dev.logplay.server.core.job.impl.CreateJobUseCaseImpl
import dev.logplay.server.core.job.impl.GetPendingJobsUseCaseImpl
import dev.logplay.server.core.job.impl.SaveJobCheckpointUseCaseImpl

class JobUseCaseLookUp(jobManager: JobManager) {
    val createJobUseCase: CreateJobUseCase = CreateJobUseCaseImpl(jobManager)
    val getPendingJobsUseCase: GetPendingJobsUseCase = GetPendingJobsUseCaseImpl(jobManager)
    val saveJobCheckpointUseCase: SaveJobCheckpointUseCase =
        SaveJobCheckpointUseCaseImpl(jobManager)
}
