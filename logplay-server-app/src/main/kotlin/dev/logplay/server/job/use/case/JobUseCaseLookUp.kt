package dev.logplay.server.job.use.case

import dev.logplay.server.core.job.*
import dev.logplay.server.core.job.impl.*
import dev.logplay.server.core.worker.WorkerGateway

class JobUseCaseLookUp(jobGateway: JobGateway, workerGateway: WorkerGateway) {
    val createJobUseCase: CreateJobUseCase = CreateJobUseCaseImpl(jobGateway)
    val acquirePendingJobsUseCase: AcquirePendingJobsUseCase =
        AcquirePendingJobsUseCaseImpl(jobGateway, workerGateway)
    val saveJobCheckpointUseCase: SaveJobCheckpointUseCase =
        SaveJobCheckpointUseCaseImpl(jobGateway)
    val getCheckpointsUseCase: GetCheckpointsUseCase = GetCheckpointsUseCaseImpl(jobGateway)
    val completeJobUseCase: CompleteJobUseCase = CompleteJobUseCaseImpl(jobGateway)
    val releaseJobUseCase: ReleaseJobUseCase = ReleaseJobUseCaseImpl(jobGateway)
    val reportExecutionErrorUseCase: ReportExecutionErrorUseCase =
        ReportExecutionErrorUseCaseImpl(jobGateway)
    val abortJobUseCase: AbortJobUseCase = AbortJobUseCaseImpl(jobGateway)
    val getJobEventsUseCase: GetJobEventsUseCase = GetJobEventsUseCaseImpl(jobGateway)
}
