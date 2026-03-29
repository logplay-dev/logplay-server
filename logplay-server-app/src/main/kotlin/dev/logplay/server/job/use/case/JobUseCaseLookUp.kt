package dev.logplay.server.job.use.case

import dev.logplay.server.core.job.CompleteJobUseCase
import dev.logplay.server.core.job.CreateJobUseCase
import dev.logplay.server.core.job.GetPendingJobsUseCase
import dev.logplay.server.core.job.JobGateway
import dev.logplay.server.core.job.SaveJobCheckpointUseCase
import dev.logplay.server.core.job.impl.CompleteJobUseCaseImpl
import dev.logplay.server.core.job.impl.CreateJobUseCaseImpl
import dev.logplay.server.core.job.impl.GetPendingJobsUseCaseImpl
import dev.logplay.server.core.job.impl.SaveJobCheckpointUseCaseImpl

class JobUseCaseLookUp(jobGateway: JobGateway) {
    val createJobUseCase: CreateJobUseCase = CreateJobUseCaseImpl(jobGateway)
    val getPendingJobsUseCase: GetPendingJobsUseCase = GetPendingJobsUseCaseImpl(jobGateway)
    val saveJobCheckpointUseCase: SaveJobCheckpointUseCase =
        SaveJobCheckpointUseCaseImpl(jobGateway)
    val completeJobUseCase: CompleteJobUseCase = CompleteJobUseCaseImpl(jobGateway)
}
