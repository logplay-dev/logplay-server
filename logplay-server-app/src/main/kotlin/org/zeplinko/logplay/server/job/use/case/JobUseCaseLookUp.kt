package org.zeplinko.logplay.server.job.use.case

import org.zeplinko.logplay.server.core.UnitOfWork
import org.zeplinko.logplay.server.core.job.*
import org.zeplinko.logplay.server.core.job.impl.*
import org.zeplinko.logplay.server.core.worker.WorkerGateway

/**
 * Manual dependency-injection factory for the job-domain use cases. Constructed once at app startup
 * with the chosen gateway implementations and read by the controllers as a flat namespace of
 * use-case singletons. Keeping this explicit (no DI container) makes the dependency graph trivially
 * navigable.
 */
class JobUseCaseLookUp(
    jobGateway: JobGateway,
    workerGateway: WorkerGateway,
    unitOfWork: UnitOfWork,
) {
    val createJobUseCase: CreateJobUseCase = CreateJobUseCaseImpl(jobGateway, unitOfWork)
    val acquirePendingJobsUseCase: AcquirePendingJobsUseCase =
        AcquirePendingJobsUseCaseImpl(jobGateway, workerGateway, unitOfWork)
    val saveJobCheckpointUseCase: SaveJobCheckpointUseCase =
        SaveJobCheckpointUseCaseImpl(jobGateway, unitOfWork)
    val getCheckpointsUseCase: GetCheckpointsUseCase = GetCheckpointsUseCaseImpl(jobGateway)
    val completeJobUseCase: CompleteJobUseCase = CompleteJobUseCaseImpl(jobGateway, unitOfWork)
    val releaseJobUseCase: ReleaseJobUseCase = ReleaseJobUseCaseImpl(jobGateway, unitOfWork)
    val reportExecutionErrorUseCase: ReportExecutionErrorUseCase =
        ReportExecutionErrorUseCaseImpl(jobGateway, unitOfWork)
    val abortJobUseCase: AbortJobUseCase = AbortJobUseCaseImpl(jobGateway, unitOfWork)
    val getJobEventsUseCase: GetJobEventsUseCase = GetJobEventsUseCaseImpl(jobGateway)
}
