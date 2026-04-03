package dev.logplay.server.core.job.fakes

import dev.logplay.server.core.job.*
import dev.logplay.server.core.worker.fakes.InMemoryWorkerGateway
import java.time.Instant

class InMemoryJobGateway(private val workerGateway: InMemoryWorkerGateway? = null) : JobGateway {

    private val jobs = mutableMapOf<String, Job>()
    private val checkpoints = mutableMapOf<String, Checkpoint>()
    private val events = mutableListOf<JobEvent>()

    override suspend fun insertJob(job: Job): Job {
        if (jobs.containsKey(job.id)) throw JobAlreadyExistsException()
        if (jobs.values.any { it.idempotencyKey == job.idempotencyKey })
            throw DuplicateIdempotencyKeyException(job.idempotencyKey)
        jobs[job.id] = job
        return job
    }

    override suspend fun acquirePendingJobs(
        workerId: String,
        limit: Int,
        eventFactory: ((Job) -> JobEvent)?,
    ): List<Job> {
        if (workerGateway != null) {
            val worker = workerGateway.findWorkerById(workerId)
            if (worker == null || worker.condemned) return emptyList()
        }
        val pending =
            jobs.values
                .filter { it.status == JobStatus.PENDING }
                .sortedBy { it.updatedAt }
                .take(limit)
        val now = Instant.now()
        val acquiredJobs =
            pending.map { job ->
                val acquired =
                    job.copy(
                        status = JobStatus.ACQUIRED,
                        lastAcquiredAt = now,
                        acquiredByWorkerId = workerId,
                        updatedAt = now,
                        version = job.version + 1,
                    )
                jobs[job.id] = acquired
                acquired
            }
        if (eventFactory != null) {
            for (job in acquiredJobs) {
                events.add(eventFactory(job))
            }
        }
        return acquiredJobs
    }

    override suspend fun findJobById(id: String): Job? = jobs[id]

    override suspend fun findJobByIdempotencyKey(idempotencyKey: String): Job? =
        jobs.values.find { it.idempotencyKey == idempotencyKey }

    private fun validateCheckpointConstraints(checkpoint: Checkpoint) {
        val hasDuplicateOrder =
            checkpoints.values.any {
                it.jobId == checkpoint.jobId && it.orderKey == checkpoint.orderKey
            }
        if (hasDuplicateOrder) throw InvalidCheckpointOrderException(checkpoint.jobId)
    }

    override suspend fun findCheckpointById(id: String): Checkpoint? = checkpoints[id]

    override suspend fun findCheckpointsByJobId(
        jobId: String,
        afterOrderKey: Long?,
        limit: Int,
    ): List<Checkpoint> =
        checkpoints.values
            .filter { it.jobId == jobId }
            .let { list ->
                if (afterOrderKey != null) list.filter { it.orderKey > afterOrderKey } else list
            }
            .sortedBy { it.orderKey }
            .take(limit)

    override suspend fun insertJobWithEvent(job: Job, event: JobEvent): Job {
        val inserted = insertJob(job)
        events.add(event)
        return inserted
    }

    override suspend fun findEventsByJobId(jobId: String): List<JobEvent> =
        events.filter { it.jobId == jobId }

    override suspend fun saveCheckpoint(
        jobId: String,
        workerId: String,
        updatedAt: Instant,
        compute: (Job, Checkpoint?) -> Checkpoint,
    ): Checkpoint? {
        val job = jobs[jobId] ?: return null
        if (job.status != JobStatus.ACQUIRED || job.acquiredByWorkerId != workerId) return null
        val lastCheckpoint =
            checkpoints.values.filter { it.jobId == jobId }.maxByOrNull { it.orderKey }
        val checkpoint = compute(job, lastCheckpoint)
        validateCheckpointConstraints(checkpoint)
        checkpoints[checkpoint.id] = checkpoint
        jobs[jobId] = job.copy(retries = 0, updatedAt = updatedAt, version = job.version + 1)
        return checkpoint
    }

    override suspend fun reportExecutionError(
        jobId: String,
        workerId: String,
        updatedAt: Instant,
        compute: (Job) -> ExecutionErrorResult,
    ): Job? {
        val job = jobs[jobId] ?: return null
        if (job.status != JobStatus.ACQUIRED || job.acquiredByWorkerId != workerId) return null
        val result = compute(job)
        val updated =
            job.copy(
                status = result.status,
                retries = result.retries,
                acquiredByWorkerId = null,
                updatedAt = updatedAt,
                version = job.version + 1,
            )
        jobs[jobId] = updated
        events.addAll(result.events)
        return updated
    }

    override suspend fun completeJob(
        jobId: String,
        workerId: String,
        outputData: ByteArray?,
        updatedAt: Instant,
        event: JobEvent,
    ): Job? {
        val job = jobs[jobId] ?: return null
        if (job.status != JobStatus.ACQUIRED || job.acquiredByWorkerId != workerId) return null
        val updated =
            job.copy(
                status = JobStatus.FINISHED,
                acquiredByWorkerId = null,
                outputData = outputData,
                updatedAt = updatedAt,
                version = job.version + 1,
            )
        jobs[jobId] = updated
        events.add(event)
        return updated
    }

    override suspend fun abortJob(jobId: String, updatedAt: Instant, event: JobEvent): Job? {
        val job = jobs[jobId] ?: return null
        if (job.status != JobStatus.PENDING && job.status != JobStatus.ACQUIRED) return null
        val updated =
            job.copy(
                status = JobStatus.ABORTED,
                acquiredByWorkerId = null,
                updatedAt = updatedAt,
                version = job.version + 1,
            )
        jobs[jobId] = updated
        events.add(event)
        return updated
    }

    override suspend fun releaseJob(
        jobId: String,
        workerId: String,
        updatedAt: Instant,
        event: JobEvent,
    ): Job? {
        val job = jobs[jobId] ?: return null
        if (job.status != JobStatus.ACQUIRED || job.acquiredByWorkerId != workerId) return null
        val updated =
            job.copy(
                status = JobStatus.PENDING,
                acquiredByWorkerId = null,
                updatedAt = updatedAt,
                version = job.version + 1,
            )
        jobs[jobId] = updated
        events.add(event)
        return updated
    }

    override suspend fun releaseJobsByWorkerId(workerId: String, updatedAt: Instant): Int =
        releaseJobsByWorkerIds(listOf(workerId), updatedAt)

    override suspend fun releaseJobsByWorkerIds(workerIds: List<String>, updatedAt: Instant): Int {
        val acquired =
            jobs.values.filter {
                it.acquiredByWorkerId in workerIds && it.status == JobStatus.ACQUIRED
            }
        for (job in acquired) {
            jobs[job.id] =
                job.copy(
                    status = JobStatus.PENDING,
                    acquiredByWorkerId = null,
                    updatedAt = updatedAt,
                    version = job.version + 1,
                )
        }
        return acquired.size
    }

    // Test helpers
    fun save(job: Job) {
        jobs[job.id] = job
    }

    fun saveTestCheckpoint(checkpoint: Checkpoint) {
        checkpoints[checkpoint.id] = checkpoint
    }

    fun checkpointCount(): Int = checkpoints.size

    fun checkpointsForJob(jobId: String): List<Checkpoint> =
        checkpoints.values.filter { it.jobId == jobId }

    fun allEvents(): List<JobEvent> = events.toList()

    fun eventsForJob(jobId: String): List<JobEvent> = events.filter { it.jobId == jobId }
}
