package org.zeplinko.logplay.server.job.use.case

import org.zeplinko.logplay.server.core.job.*
import org.zeplinko.logplay.server.core.job.impl.*
import org.zeplinko.logplay.server.core.worker.WorkerGateway

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
