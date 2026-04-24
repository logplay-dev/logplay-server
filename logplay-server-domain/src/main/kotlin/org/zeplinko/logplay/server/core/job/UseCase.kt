package org.zeplinko.logplay.server.core.job

interface CreateJobUseCase {
    suspend fun execute(createJobCommand: CreateJobCommand): Job
}

interface AcquirePendingJobsUseCase {
    suspend fun execute(command: AcquirePendingJobsCommand): List<Job>
}

interface SaveJobCheckpointUseCase {
    suspend fun execute(command: SaveJobCheckpointCommand): Checkpoint
}

interface CompleteJobUseCase {
    suspend fun execute(command: CompleteJobCommand): Job
}

interface ReleaseJobUseCase {
    suspend fun execute(command: ReleaseJobCommand): Job
}

interface GetCheckpointsUseCase {
    suspend fun execute(command: GetCheckpointsCommand): CheckpointPage
}

interface ReportExecutionErrorUseCase {
    suspend fun execute(command: ReportExecutionErrorCommand): Job
}

interface AbortJobUseCase {
    suspend fun execute(command: AbortJobCommand): Job
}

interface GetJobEventsUseCase {
    suspend fun execute(jobId: String): List<JobEvent>
}
