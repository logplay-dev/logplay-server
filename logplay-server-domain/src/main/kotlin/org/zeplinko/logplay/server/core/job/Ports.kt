package org.zeplinko.logplay.server.core.job

import java.time.Instant

interface JobGateway {
    suspend fun insertJob(job: Job): Job

    suspend fun acquirePendingJobs(
        groupId: String,
        type: String,
        workerId: String,
        limit: Int,
        eventFactory: ((Job) -> JobEvent)? = null,
    ): List<Job>

    suspend fun findJobById(id: String): Job?

    suspend fun findJobByIdempotencyKey(groupId: String, idempotencyKey: String): Job?

    suspend fun saveCheckpoint(
        jobId: String,
        workerId: String,
        updatedAt: Instant,
        compute: (Job, Checkpoint?) -> Checkpoint,
    ): Checkpoint?

    suspend fun findCheckpointById(id: String): Checkpoint?

    suspend fun findCheckpointsByJobId(
        jobId: String,
        afterOrderKey: Long?,
        limit: Int,
    ): List<Checkpoint>

    suspend fun reportExecutionError(
        jobId: String,
        workerId: String,
        updatedAt: Instant,
        compute: (Job) -> ExecutionErrorResult,
    ): Job?

    suspend fun completeJob(
        jobId: String,
        workerId: String,
        outputData: ByteArray?,
        updatedAt: Instant,
        event: JobEvent,
    ): Job?

    suspend fun abortJob(jobId: String, updatedAt: Instant, event: JobEvent): Job?

    suspend fun releaseJob(
        jobId: String,
        workerId: String,
        updatedAt: Instant,
        event: JobEvent,
    ): Job?

    suspend fun releaseJobsByWorkerId(workerId: String, updatedAt: Instant): Int

    suspend fun releaseJobsByWorkerIds(workerIds: List<String>, updatedAt: Instant): Int

    suspend fun insertJobWithEvent(job: Job, event: JobEvent): Job

    suspend fun findEventsByJobId(jobId: String): List<JobEvent>
}
