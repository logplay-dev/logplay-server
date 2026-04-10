package dev.logplay.server.job.adapters

import dev.logplay.server.core.job.*
import dev.logplay.server.core.worker.WorkerNotFoundException
import java.sql.ResultSet
import java.sql.SQLException
import java.time.Instant
import javax.sql.DataSource
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

class H2JobGateway(private val dataSource: DataSource) : JobGateway {

    companion object {
        // jobs.id is derived deterministically from (groupId, idempotencyKey) by
        // JobIdGenerator, so a PK collision always means a duplicate idempotency
        // key was submitted. H2 auto-numbers the PK constraint (e.g. PRIMARY_KEY_2),
        // so we match on the stable column reference "PUBLIC.JOBS(ID)" instead of
        // the constraint name itself.
        private const val CONSTRAINT_JOBS_PK = "PUBLIC.JOBS(ID)"
        private const val CONSTRAINT_FK_JOB_ACQUIRED_WORKER = "FK_JOB_ACQUIRED_WORKER"

        // checkpoints.id is derived deterministically from (jobId, previousCheckpointId)
        // by CheckpointIdGenerator, so a PK collision means a duplicate chain position
        // was submitted. H2 auto-numbers the PK constraint, so we match on the stable
        // column reference "PUBLIC.CHECKPOINTS(ID)" instead of the constraint name.
        private const val CONSTRAINT_CHECKPOINTS_PK = "PUBLIC.CHECKPOINTS(ID)"
        private val CHECKPOINT_CONSTRAINTS =
            setOf(CONSTRAINT_CHECKPOINTS_PK, "FK_CHECKPOINT_JOB", "FK_CHECKPOINT_PREVIOUS")
    }

    private fun SQLException.matchesConstraint(constraint: String): Boolean =
        message?.contains(constraint, ignoreCase = true) == true

    private fun SQLException.matchesAnyConstraint(constraints: Set<String>): Boolean =
        constraints.any { matchesConstraint(it) }

    override suspend fun insertJob(job: Job): Job =
        withContext(Dispatchers.IO) {
            try {
                dataSource.connection.use { conn ->
                    conn
                        .prepareStatement(
                            "INSERT INTO jobs (id, group_id, name, type, status, retries, max_retries, idempotency_key, created_at, updated_at, last_acquired_at, acquired_by_worker_id, version, input_data, output_data) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                        )
                        .use { stmt ->
                            stmt.setString(1, job.id)
                            stmt.setString(2, job.groupId)
                            stmt.setString(3, job.name)
                            stmt.setString(4, job.type)
                            stmt.setString(5, job.status.name)
                            stmt.setInt(6, job.retries)
                            stmt.setObject(7, job.maxRetries)
                            stmt.setString(8, job.idempotencyKey)
                            stmt.setLong(9, job.createdAt.toEpochMilli())
                            stmt.setLong(10, job.updatedAt.toEpochMilli())
                            stmt.setObject(11, job.lastAcquiredAt?.toEpochMilli())
                            stmt.setString(12, job.acquiredByWorkerId)
                            stmt.setLong(13, job.version)
                            stmt.setBytes(14, job.inputData)
                            stmt.setBytes(15, job.outputData)
                            stmt.executeUpdate()
                        }
                }
            } catch (e: SQLException) {
                if (e.matchesConstraint(CONSTRAINT_JOBS_PK))
                    throw DuplicateIdempotencyKeyException(job.groupId, job.idempotencyKey)
                throw e
            }
            job
        }

    override suspend fun acquirePendingJobs(
        groupId: String,
        type: String,
        workerId: String,
        limit: Int,
        eventFactory: ((Job) -> JobEvent)?,
    ): List<Job> =
        withContext(Dispatchers.IO) {
            dataSource.connection.use { conn ->
                conn.autoCommit = false
                try {
                    val jobs =
                        conn
                            .prepareStatement(
                                "SELECT j.* FROM jobs j INNER JOIN workers w ON w.id = ? AND w.condemned = FALSE WHERE j.group_id = ? AND j.type = ? AND j.status = ? ORDER BY j.updated_at ASC LIMIT ? FOR UPDATE SKIP LOCKED"
                            )
                            .use { stmt ->
                                stmt.setString(1, workerId)
                                stmt.setString(2, groupId)
                                stmt.setString(3, type)
                                stmt.setString(4, JobStatus.PENDING.name)
                                stmt.setInt(5, limit)
                                stmt.executeQuery().use { rs -> rs.mapToJobs() }
                            }

                    val now = Instant.now()
                    conn
                        .prepareStatement(
                            "UPDATE jobs SET status = ?, updated_at = ?, last_acquired_at = ?, acquired_by_worker_id = ?, version = version + 1 WHERE id = ? AND version = ?"
                        )
                        .use { stmt ->
                            for (job in jobs) {
                                stmt.setString(1, JobStatus.ACQUIRED.name)
                                stmt.setLong(2, now.toEpochMilli())
                                stmt.setLong(3, now.toEpochMilli())
                                stmt.setString(4, workerId)
                                stmt.setString(5, job.id)
                                stmt.setLong(6, job.version)
                                stmt.addBatch()
                            }
                            stmt.executeBatch()
                        }

                    val acquiredJobs =
                        jobs.map {
                            it.copy(
                                status = JobStatus.ACQUIRED,
                                lastAcquiredAt = now,
                                acquiredByWorkerId = workerId,
                                updatedAt = now,
                                version = it.version + 1,
                            )
                        }

                    if (eventFactory != null) {
                        for (job in acquiredJobs) {
                            insertEventStatement(conn, eventFactory(job))
                        }
                    }

                    conn.commit()
                    acquiredJobs
                } catch (e: SQLException) {
                    conn.rollback()
                    if (e.matchesConstraint(CONSTRAINT_FK_JOB_ACQUIRED_WORKER))
                        throw WorkerNotFoundException(workerId)
                    throw e
                } catch (e: Exception) {
                    conn.rollback()
                    throw e
                } finally {
                    conn.autoCommit = true
                }
            }
        }

    override suspend fun findJobById(id: String): Job? =
        withContext(Dispatchers.IO) {
            dataSource.connection.use { conn ->
                conn.prepareStatement("SELECT * FROM jobs WHERE id = ?").use { stmt ->
                    stmt.setString(1, id)
                    stmt.executeQuery().use { rs -> if (rs.next()) rs.toJob() else null }
                }
            }
        }

    override suspend fun findJobByIdempotencyKey(groupId: String, idempotencyKey: String): Job? =
        withContext(Dispatchers.IO) {
            dataSource.connection.use { conn ->
                conn
                    .prepareStatement(
                        "SELECT * FROM jobs WHERE group_id = ? AND idempotency_key = ?"
                    )
                    .use { stmt ->
                        stmt.setString(1, groupId)
                        stmt.setString(2, idempotencyKey)
                        stmt.executeQuery().use { rs -> if (rs.next()) rs.toJob() else null }
                    }
            }
        }

    override suspend fun findCheckpointById(id: String): Checkpoint? =
        withContext(Dispatchers.IO) {
            dataSource.connection.use { conn ->
                conn.prepareStatement("SELECT * FROM checkpoints WHERE id = ?").use { stmt ->
                    stmt.setString(1, id)
                    stmt.executeQuery().use { rs -> if (rs.next()) rs.toCheckpoint() else null }
                }
            }
        }

    override suspend fun findCheckpointsByJobId(
        jobId: String,
        afterOrderKey: Long?,
        limit: Int,
    ): List<Checkpoint> =
        withContext(Dispatchers.IO) {
            dataSource.connection.use { conn ->
                if (afterOrderKey != null) {
                    conn
                        .prepareStatement(
                            "SELECT * FROM checkpoints WHERE job_id = ? AND order_key > ? ORDER BY order_key ASC LIMIT ?"
                        )
                        .use { stmt ->
                            stmt.setString(1, jobId)
                            stmt.setLong(2, afterOrderKey)
                            stmt.setInt(3, limit)
                            stmt.executeQuery().use { rs -> rs.mapToCheckpoints() }
                        }
                } else {
                    conn
                        .prepareStatement(
                            "SELECT * FROM checkpoints WHERE job_id = ? ORDER BY order_key ASC LIMIT ?"
                        )
                        .use { stmt ->
                            stmt.setString(1, jobId)
                            stmt.setInt(2, limit)
                            stmt.executeQuery().use { rs -> rs.mapToCheckpoints() }
                        }
                }
            }
        }

    override suspend fun insertJobWithEvent(job: Job, event: JobEvent): Job =
        withContext(Dispatchers.IO) {
            dataSource.connection.use { conn ->
                conn.autoCommit = false
                try {
                    conn
                        .prepareStatement(
                            "INSERT INTO jobs (id, group_id, name, type, status, retries, max_retries, idempotency_key, created_at, updated_at, last_acquired_at, acquired_by_worker_id, version, input_data, output_data) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                        )
                        .use { stmt ->
                            stmt.setString(1, job.id)
                            stmt.setString(2, job.groupId)
                            stmt.setString(3, job.name)
                            stmt.setString(4, job.type)
                            stmt.setString(5, job.status.name)
                            stmt.setInt(6, job.retries)
                            stmt.setObject(7, job.maxRetries)
                            stmt.setString(8, job.idempotencyKey)
                            stmt.setLong(9, job.createdAt.toEpochMilli())
                            stmt.setLong(10, job.updatedAt.toEpochMilli())
                            stmt.setObject(11, job.lastAcquiredAt?.toEpochMilli())
                            stmt.setString(12, job.acquiredByWorkerId)
                            stmt.setLong(13, job.version)
                            stmt.setBytes(14, job.inputData)
                            stmt.setBytes(15, job.outputData)
                            stmt.executeUpdate()
                        }
                    insertEventStatement(conn, event)
                    conn.commit()
                    job
                } catch (e: SQLException) {
                    conn.rollback()
                    if (e.matchesConstraint(CONSTRAINT_JOBS_PK))
                        throw DuplicateIdempotencyKeyException(job.groupId, job.idempotencyKey)
                    throw e
                } catch (e: Exception) {
                    conn.rollback()
                    throw e
                } finally {
                    conn.autoCommit = true
                }
            }
        }

    override suspend fun findEventsByJobId(jobId: String): List<JobEvent> =
        withContext(Dispatchers.IO) {
            dataSource.connection.use { conn ->
                conn
                    .prepareStatement(
                        "SELECT * FROM job_events WHERE job_id = ? ORDER BY created_at ASC"
                    )
                    .use { stmt ->
                        stmt.setString(1, jobId)
                        stmt.executeQuery().use { rs ->
                            val events = mutableListOf<JobEvent>()
                            while (rs.next()) events.add(rs.toJobEvent())
                            events
                        }
                    }
            }
        }

    private fun insertEventStatement(conn: java.sql.Connection, event: JobEvent) {
        conn
            .prepareStatement(
                "INSERT INTO job_events (id, job_id, event_type, actor_type, actor_id, created_at, event_message, event_detail) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
            )
            .use { stmt ->
                stmt.setString(1, event.id)
                stmt.setString(2, event.jobId)
                stmt.setString(3, event.eventType.name)
                stmt.setString(4, event.actorType.name)
                stmt.setString(5, event.actorId)
                stmt.setLong(6, event.createdAt.toEpochMilli())
                stmt.setString(7, event.eventMessage)
                stmt.setString(8, event.eventDetail)
                stmt.executeUpdate()
            }
    }

    override suspend fun saveCheckpoint(
        jobId: String,
        workerId: String,
        updatedAt: Instant,
        compute: (Job, Checkpoint?) -> Checkpoint,
    ): Checkpoint? =
        withContext(Dispatchers.IO) {
            dataSource.connection.use { conn ->
                conn.autoCommit = false
                try {
                    val job =
                        conn
                            .prepareStatement(
                                "SELECT * FROM jobs WHERE id = ? AND status = ? AND acquired_by_worker_id = ? FOR UPDATE"
                            )
                            .use { stmt ->
                                stmt.setString(1, jobId)
                                stmt.setString(2, JobStatus.ACQUIRED.name)
                                stmt.setString(3, workerId)
                                stmt.executeQuery().use { rs ->
                                    if (rs.next()) rs.toJob() else null
                                }
                            }
                    if (job == null) {
                        conn.rollback()
                        return@withContext null
                    }
                    val lastCheckpoint =
                        conn
                            .prepareStatement(
                                "SELECT * FROM checkpoints WHERE job_id = ? ORDER BY order_key DESC LIMIT 1"
                            )
                            .use { stmt ->
                                stmt.setString(1, jobId)
                                stmt.executeQuery().use { rs ->
                                    if (rs.next()) rs.toCheckpoint() else null
                                }
                            }
                    val checkpoint = compute(job, lastCheckpoint)
                    conn
                        .prepareStatement(
                            "INSERT INTO checkpoints (id, job_id, previous_checkpoint_id, name, created_at, order_key, data) VALUES (?, ?, ?, ?, ?, ?, ?)"
                        )
                        .use { stmt ->
                            stmt.setString(1, checkpoint.id)
                            stmt.setString(2, checkpoint.jobId)
                            stmt.setString(3, checkpoint.previousCheckpointId)
                            stmt.setString(4, checkpoint.name)
                            stmt.setLong(5, checkpoint.createdAt.toEpochMilli())
                            stmt.setLong(6, checkpoint.orderKey)
                            stmt.setBytes(7, checkpoint.data)
                            stmt.executeUpdate()
                        }
                    val rowsUpdated =
                        conn
                            .prepareStatement(
                                "UPDATE jobs SET retries = 0, updated_at = ?, version = version + 1 WHERE id = ? AND version = ?"
                            )
                            .use { stmt ->
                                stmt.setLong(1, updatedAt.toEpochMilli())
                                stmt.setString(2, jobId)
                                stmt.setLong(3, job.version)
                                stmt.executeUpdate()
                            }
                    if (rowsUpdated == 0) {
                        conn.rollback()
                        throw JobConcurrentModificationException(jobId)
                    }
                    conn.commit()
                    checkpoint
                } catch (e: SQLException) {
                    conn.rollback()
                    if (e.matchesAnyConstraint(CHECKPOINT_CONSTRAINTS))
                        throw InvalidCheckpointOrderException(jobId, e)
                    throw e
                } catch (e: Exception) {
                    conn.rollback()
                    throw e
                } finally {
                    conn.autoCommit = true
                }
            }
        }

    override suspend fun reportExecutionError(
        jobId: String,
        workerId: String,
        updatedAt: Instant,
        compute: (Job) -> ExecutionErrorResult,
    ): Job? =
        withContext(Dispatchers.IO) {
            dataSource.connection.use { conn ->
                conn.autoCommit = false
                try {
                    val job =
                        conn
                            .prepareStatement(
                                "SELECT * FROM jobs WHERE id = ? AND status = ? AND acquired_by_worker_id = ? FOR UPDATE"
                            )
                            .use { stmt ->
                                stmt.setString(1, jobId)
                                stmt.setString(2, JobStatus.ACQUIRED.name)
                                stmt.setString(3, workerId)
                                stmt.executeQuery().use { rs ->
                                    if (rs.next()) rs.toJob() else null
                                }
                            }
                    if (job == null) {
                        conn.rollback()
                        return@withContext null
                    }
                    val result = compute(job)
                    val rowsUpdated =
                        conn
                            .prepareStatement(
                                "UPDATE jobs SET status = ?, retries = ?, acquired_by_worker_id = NULL, updated_at = ?, version = version + 1 WHERE id = ? AND version = ?"
                            )
                            .use { stmt ->
                                stmt.setString(1, result.status.name)
                                stmt.setInt(2, result.retries)
                                stmt.setLong(3, updatedAt.toEpochMilli())
                                stmt.setString(4, jobId)
                                stmt.setLong(5, job.version)
                                stmt.executeUpdate()
                            }
                    if (rowsUpdated == 0) {
                        conn.rollback()
                        throw JobConcurrentModificationException(jobId)
                    }
                    for (event in result.events) {
                        insertEventStatement(conn, event)
                    }
                    conn.commit()
                    findJobById(jobId)
                } catch (e: Exception) {
                    conn.rollback()
                    throw e
                } finally {
                    conn.autoCommit = true
                }
            }
        }

    override suspend fun completeJob(
        jobId: String,
        workerId: String,
        outputData: ByteArray?,
        updatedAt: Instant,
        event: JobEvent,
    ): Job? =
        withContext(Dispatchers.IO) {
            dataSource.connection.use { conn ->
                conn.autoCommit = false
                try {
                    val rowsUpdated =
                        conn
                            .prepareStatement(
                                "UPDATE jobs SET status = ?, acquired_by_worker_id = NULL, output_data = ?, updated_at = ?, version = version + 1 WHERE id = ? AND status = ? AND acquired_by_worker_id = ?"
                            )
                            .use { stmt ->
                                stmt.setString(1, JobStatus.FINISHED.name)
                                stmt.setBytes(2, outputData)
                                stmt.setLong(3, updatedAt.toEpochMilli())
                                stmt.setString(4, jobId)
                                stmt.setString(5, JobStatus.ACQUIRED.name)
                                stmt.setString(6, workerId)
                                stmt.executeUpdate()
                            }
                    if (rowsUpdated == 0) {
                        conn.rollback()
                        return@withContext null
                    }
                    insertEventStatement(conn, event)
                    conn.commit()
                    findJobById(jobId)
                } catch (e: Exception) {
                    conn.rollback()
                    throw e
                } finally {
                    conn.autoCommit = true
                }
            }
        }

    override suspend fun abortJob(jobId: String, updatedAt: Instant, event: JobEvent): Job? =
        withContext(Dispatchers.IO) {
            dataSource.connection.use { conn ->
                conn.autoCommit = false
                try {
                    val rowsUpdated =
                        conn
                            .prepareStatement(
                                "UPDATE jobs SET status = ?, acquired_by_worker_id = NULL, updated_at = ?, version = version + 1 WHERE id = ? AND status IN (?, ?)"
                            )
                            .use { stmt ->
                                stmt.setString(1, JobStatus.ABORTED.name)
                                stmt.setLong(2, updatedAt.toEpochMilli())
                                stmt.setString(3, jobId)
                                stmt.setString(4, JobStatus.PENDING.name)
                                stmt.setString(5, JobStatus.ACQUIRED.name)
                                stmt.executeUpdate()
                            }
                    if (rowsUpdated == 0) {
                        conn.rollback()
                        return@withContext null
                    }
                    insertEventStatement(conn, event)
                    conn.commit()
                    findJobById(jobId)
                } catch (e: Exception) {
                    conn.rollback()
                    throw e
                } finally {
                    conn.autoCommit = true
                }
            }
        }

    override suspend fun releaseJob(
        jobId: String,
        workerId: String,
        updatedAt: Instant,
        event: JobEvent,
    ): Job? =
        withContext(Dispatchers.IO) {
            dataSource.connection.use { conn ->
                conn.autoCommit = false
                try {
                    val rowsUpdated =
                        conn
                            .prepareStatement(
                                "UPDATE jobs SET status = ?, acquired_by_worker_id = NULL, updated_at = ?, version = version + 1 WHERE id = ? AND status = ? AND acquired_by_worker_id = ?"
                            )
                            .use { stmt ->
                                stmt.setString(1, JobStatus.PENDING.name)
                                stmt.setLong(2, updatedAt.toEpochMilli())
                                stmt.setString(3, jobId)
                                stmt.setString(4, JobStatus.ACQUIRED.name)
                                stmt.setString(5, workerId)
                                stmt.executeUpdate()
                            }
                    if (rowsUpdated == 0) {
                        conn.rollback()
                        return@withContext null
                    }
                    insertEventStatement(conn, event)
                    conn.commit()
                    findJobById(jobId)
                } catch (e: Exception) {
                    conn.rollback()
                    throw e
                } finally {
                    conn.autoCommit = true
                }
            }
        }

    override suspend fun releaseJobsByWorkerId(workerId: String, updatedAt: Instant): Int =
        releaseJobsByWorkerIds(listOf(workerId), updatedAt)

    override suspend fun releaseJobsByWorkerIds(workerIds: List<String>, updatedAt: Instant): Int =
        withContext(Dispatchers.IO) {
            if (workerIds.isEmpty()) return@withContext 0
            dataSource.connection.use { conn ->
                val placeholders = workerIds.joinToString(",") { "?" }
                conn
                    .prepareStatement(
                        "UPDATE jobs SET status = ?, acquired_by_worker_id = NULL, updated_at = ?, version = version + 1 WHERE acquired_by_worker_id IN ($placeholders) AND status = ?"
                    )
                    .use { stmt ->
                        stmt.setString(1, JobStatus.PENDING.name)
                        stmt.setLong(2, updatedAt.toEpochMilli())
                        workerIds.forEachIndexed { i, id -> stmt.setString(i + 3, id) }
                        stmt.setString(workerIds.size + 3, JobStatus.ACQUIRED.name)
                        stmt.executeUpdate()
                    }
            }
        }

    private fun ResultSet.mapToCheckpoints(): List<Checkpoint> {
        val checkpoints = mutableListOf<Checkpoint>()
        while (next()) checkpoints.add(toCheckpoint())
        return checkpoints
    }

    private fun ResultSet.mapToJobs(): List<Job> {
        val jobs = mutableListOf<Job>()
        while (next()) jobs.add(toJob())
        return jobs
    }

    private fun ResultSet.toCheckpoint(): Checkpoint =
        Checkpoint(
            id = getString("id"),
            jobId = getString("job_id"),
            previousCheckpointId = getString("previous_checkpoint_id"),
            name = getString("name"),
            createdAt = Instant.ofEpochMilli(getLong("created_at")),
            orderKey = getLong("order_key"),
            data = getBytes("data"),
        )

    private fun ResultSet.toJobEvent(): JobEvent =
        JobEvent(
            id = getString("id"),
            jobId = getString("job_id"),
            eventType = JobEventType.valueOf(getString("event_type")),
            actorType = ActorType.valueOf(getString("actor_type")),
            actorId = getString("actor_id"),
            createdAt = Instant.ofEpochMilli(getLong("created_at")),
            eventMessage = getString("event_message"),
            eventDetail = getString("event_detail"),
        )

    private fun ResultSet.toJob(): Job {
        val lastAcquiredAtMillis = getLong("last_acquired_at")
        val lastAcquiredAt = if (wasNull()) null else Instant.ofEpochMilli(lastAcquiredAtMillis)
        val maxRetries = getObject("max_retries") as? Int
        return Job(
            id = getString("id"),
            groupId = getString("group_id"),
            name = getString("name"),
            type = getString("type"),
            status = JobStatus.valueOf(getString("status")),
            retries = getInt("retries"),
            maxRetries = maxRetries,
            idempotencyKey = getString("idempotency_key"),
            inputData = getBytes("input_data"),
            createdAt = Instant.ofEpochMilli(getLong("created_at")),
            updatedAt = Instant.ofEpochMilli(getLong("updated_at")),
            lastAcquiredAt = lastAcquiredAt,
            acquiredByWorkerId = getString("acquired_by_worker_id"),
            outputData = getBytes("output_data"),
            version = getLong("version"),
        )
    }
}
