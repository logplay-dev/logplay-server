package dev.logplay.server.core.job

interface CreateJobUseCase {
    suspend fun execute(createJobCommand: CreateJobCommand): Job
}

interface GetPendingJobsUseCase {
    suspend fun execute(getPendingJobsCommand: GetPendingJobsCommand): List<Job>
}

interface SaveJobCheckpointUseCase {
    suspend fun execute(command: SaveJobCheckpointCommand): Checkpoint
}

interface CompleteJobUseCase {
    suspend fun execute(command: CompleteJobCommand): Job
}
