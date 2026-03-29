package dev.logplay.server.job.adapters

import dev.logplay.server.core.job.*
import io.vertx.sqlclient.Pool

class PostgresJobGateway(private val pool: Pool) : JobGateway {

    override suspend fun insertJob(job: Job): Job {
        TODO("Not yet implemented")
    }

    override suspend fun updateJob(job: Job): Job {
        TODO("Not yet implemented")
    }

    override suspend fun getPendingJobs(limit: Int): List<Job> {
        TODO("Not yet implemented")
    }

    override suspend fun findJobById(id: String): Job? {
        TODO("Not yet implemented")
    }

    override suspend fun saveCheckpoint(checkpoint: Checkpoint): Checkpoint {
        TODO("Not yet implemented")
    }
}
