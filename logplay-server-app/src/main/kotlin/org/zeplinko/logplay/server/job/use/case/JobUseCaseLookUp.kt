package org.zeplinko.logplay.server.job.use.case

import org.zeplinko.logplay.server.core.Metrics
import org.zeplinko.logplay.server.core.NoopMetrics
import org.zeplinko.logplay.server.core.UnitOfWork
import org.zeplinko.logplay.server.core.job.*
import org.zeplinko.logplay.server.core.job.impl.*
import org.zeplinko.logplay.server.core.worker.WorkerGateway

/**
 * Manual dependency-injection factory for the job-domain use cases. Constructed once at app startup
 * with the chosen gateway implementations and read by the controllers as a flat namespace of
 * use-case singletons. Keeping this explicit (no DI container) makes the dependency graph trivially
 * navigable.
 *
 * [metrics] defaults to [NoopMetrics] so tests and non-instrumented callers need no wiring; the app
 * passes an OpenTelemetry-backed implementation when telemetry is enabled.
 */
class JobUseCaseLookUp(
    jobGateway: JobGateway,
    workerGateway: WorkerGateway,
    unitOfWork: UnitOfWork,
    metrics: Metrics = NoopMetrics,
) {
    val createJobUseCase: CreateJobUseCase = CreateJobUseCaseImpl(jobGateway, unitOfWork, metrics)
    val acquirePendingJobsUseCase: AcquirePendingJobsUseCase =
        AcquirePendingJobsUseCaseImpl(jobGateway, workerGateway, unitOfWork, metrics)
    val saveJobCheckpointUseCase: SaveJobCheckpointUseCase =
        SaveJobCheckpointUseCaseImpl(jobGateway, unitOfWork, metrics)
    val getCheckpointsUseCase: GetCheckpointsUseCase = GetCheckpointsUseCaseImpl(jobGateway)
    val completeJobUseCase: CompleteJobUseCase =
        CompleteJobUseCaseImpl(jobGateway, unitOfWork, metrics)
    val releaseJobUseCase: ReleaseJobUseCase =
        ReleaseJobUseCaseImpl(jobGateway, unitOfWork, metrics)
    val reportExecutionErrorUseCase: ReportExecutionErrorUseCase =
        ReportExecutionErrorUseCaseImpl(jobGateway, unitOfWork, metrics)
    val abortJobUseCase: AbortJobUseCase = AbortJobUseCaseImpl(jobGateway, unitOfWork, metrics)
    val getJobEventsUseCase: GetJobEventsUseCase = GetJobEventsUseCaseImpl(jobGateway)
}
