package dev.logplay.server.core.job.fakes

import dev.logplay.server.core.job.*

class InMemoryJobGateway : JobGateway {

    private val jobs = mutableMapOf<String, Job>()
    private val checkpoints = mutableMapOf<String, Checkpoint>()

    override suspend fun insertJob(job: Job): Job {
        jobs[job.id] = job
        return job
    }

    override suspend fun updateJob(job: Job): Job {
        jobs[job.id] = job
        return job
    }

    override suspend fun getPendingJobs(limit: Int): List<Job> =
        jobs.values.filter { it.status == JobStatus.PENDING }.take(limit)

    override suspend fun findJobById(id: String): Job? = jobs[id]

    override suspend fun saveCheckpoint(checkpoint: Checkpoint): Checkpoint {
        checkpoints[checkpoint.id] = checkpoint
        return checkpoint
    }

    // Test helpers
    fun save(job: Job) {
        jobs[job.id] = job
    }

    fun checkpointCount(): Int = checkpoints.size

    fun checkpointsForJob(jobId: String): List<Checkpoint> =
        checkpoints.values.filter { it.jobId == jobId }
}
