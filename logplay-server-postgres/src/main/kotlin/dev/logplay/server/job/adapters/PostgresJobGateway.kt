package dev.logplay.server.job.adapters

import dev.logplay.server.core.job.*
import dev.logplay.server.core.worker.WorkerNotFoundException
import io.vertx.core.buffer.Buffer
import io.vertx.kotlin.coroutines.coAwait
import io.vertx.pgclient.PgException
import io.vertx.sqlclient.Pool
import io.vertx.sqlclient.Row
import io.vertx.sqlclient.Tuple
import java.time.Instant

class PostgresJobGateway(private val pool: Pool) : JobGateway {

    companion object {
        // jobs.id is derived deterministically from (groupId, idempotencyKey) by
        // JobIdGenerator, so a PK collision always means a duplicate idempotency
        // key was submitted.
        private const val CONSTRAINT_JOBS_PK = "jobs_pkey"
        private const val CONSTRAINT_FK_JOB_ACQUIRED_WORKER = "fk_job_acquired_worker"

        // checkpoints.id is derived deterministically from (jobId, previousCheckpointId)
        // by CheckpointIdGenerator, so a PK collision means a duplicate chain position
        // was submitted.
        private const val CONSTRAINT_CHECKPOINTS_PK = "checkpoints_pkey"
        private val CHECKPOINT_CONSTRAINTS =
            setOf(CONSTRAINT_CHECKPOINTS_PK, "fk_checkpoint_job", "fk_checkpoint_previous")
    }

    override suspend fun insertJob(job: Job): Job {
        try {
            pool
                .preparedQuery(
                    $$"INSERT INTO jobs (id, group_id, name, type, status, retries, max_retries, idempotency_key, created_at, updated_at, last_acquired_at, acquired_by_worker_id, version, input_data, output_data) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)"
                )
                .execute(
                    Tuple.of(
                        job.id,
                        job.groupId,
                        job.name,
                        job.type,
                        job.status.name,
                        job.retries,
                        job.maxRetries,
                        job.idempotencyKey,
                        job.createdAt.toEpochMilli(),
                        job.updatedAt.toEpochMilli(),
                        job.lastAcquiredAt?.toEpochMilli(),
                        job.acquiredByWorkerId,
                        job.version,
                        job.inputData?.let { Buffer.buffer(it) },
                        job.outputData?.let { Buffer.buffer(it) },
                    )
                )
                .coAwait()
        } catch (e: PgException) {
            if (e.constraint == CONSTRAINT_JOBS_PK)
                throw DuplicateIdempotencyKeyException(job.groupId, job.idempotencyKey)
            throw e
        }
        return job
    }

    override suspend fun acquirePendingJobs(
        groupId: String,
        type: String,
        workerId: String,
        limit: Int,
        eventFactory: ((Job) -> JobEvent)?,
    ): List<Job> {
        val now = Instant.now()
        val conn = pool.connection.coAwait()
        try {
            val tx = conn.begin().coAwait()
            try {
                val rowSet =
                    conn
                        .preparedQuery(
                            $$"""
                            WITH acquired AS (
                                SELECT j.id FROM jobs j
                                INNER JOIN workers w ON w.id = $6 AND w.condemned = FALSE
                                WHERE j.group_id = $7 AND j.type = $8 AND j.status = $1
                                ORDER BY j.updated_at ASC
                                LIMIT $2
                                FOR UPDATE SKIP LOCKED
                            )
                            UPDATE jobs SET status = $3, updated_at = $4, last_acquired_at = $5,
                                acquired_by_worker_id = $6, version = version + 1
                            FROM acquired
                            WHERE jobs.id = acquired.id
                            RETURNING jobs.*
                            """
                                .trimIndent()
                        )
                        .execute(
                            Tuple.of(
                                JobStatus.PENDING.name,
                                limit,
                                JobStatus.ACQUIRED.name,
                                now.toEpochMilli(),
                                now.toEpochMilli(),
                                workerId,
                                groupId,
                                type,
                            )
                        )
                        .coAwait()
                val jobs = rowSet.map { it.toJob() }
                if (eventFactory != null) {
                    for (job in jobs) {
                        insertEventQuery(conn, eventFactory(job))
                    }
                }
                tx.commit().coAwait()
                return jobs
            } catch (e: PgException) {
                tx.rollback().coAwait()
                if (e.constraint == CONSTRAINT_FK_JOB_ACQUIRED_WORKER)
                    throw WorkerNotFoundException(workerId)
                throw e
            } catch (e: Exception) {
                tx.rollback().coAwait()
                throw e
            }
        } finally {
            conn.close().coAwait()
        }
    }

    override suspend fun findJobById(id: String): Job? {
        val rowSet =
            pool.preparedQuery($$"SELECT * FROM jobs WHERE id = $1").execute(Tuple.of(id)).coAwait()
        return if (rowSet.rowCount() > 0) rowSet.first().toJob() else null
    }

    override suspend fun findJobByIdempotencyKey(groupId: String, idempotencyKey: String): Job? {
        val rowSet =
            pool
                .preparedQuery($$"SELECT * FROM jobs WHERE group_id = $1 AND idempotency_key = $2")
                .execute(Tuple.of(groupId, idempotencyKey))
                .coAwait()
        return if (rowSet.rowCount() > 0) rowSet.first().toJob() else null
    }

    override suspend fun findCheckpointById(id: String): Checkpoint? {
        val rowSet =
            pool
                .preparedQuery($$"SELECT * FROM checkpoints WHERE id = $1")
                .execute(Tuple.of(id))
                .coAwait()
        return if (rowSet.rowCount() > 0) rowSet.first().toCheckpoint() else null
    }

    override suspend fun findCheckpointsByJobId(
        jobId: String,
        afterOrderKey: Long?,
        limit: Int,
    ): List<Checkpoint> {
        val rowSet =
            if (afterOrderKey != null) {
                pool
                    .preparedQuery(
                        $$"SELECT * FROM checkpoints WHERE job_id = $1 AND order_key > $2 ORDER BY order_key ASC LIMIT $3"
                    )
                    .execute(Tuple.of(jobId, afterOrderKey, limit))
                    .coAwait()
            } else {
                pool
                    .preparedQuery(
                        $$"SELECT * FROM checkpoints WHERE job_id = $1 ORDER BY order_key ASC LIMIT $2"
                    )
                    .execute(Tuple.of(jobId, limit))
                    .coAwait()
            }
        return rowSet.map { it.toCheckpoint() }
    }

    override suspend fun insertJobWithEvent(job: Job, event: JobEvent): Job {
        val conn = pool.connection.coAwait()
        try {
            val tx = conn.begin().coAwait()
            try {
                conn
                    .preparedQuery(
                        $$"INSERT INTO jobs (id, group_id, name, type, status, retries, max_retries, idempotency_key, created_at, updated_at, last_acquired_at, acquired_by_worker_id, version, input_data, output_data) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)"
                    )
                    .execute(
                        Tuple.of(
                            job.id,
                            job.groupId,
                            job.name,
                            job.type,
                            job.status.name,
                            job.retries,
                            job.maxRetries,
                            job.idempotencyKey,
                            job.createdAt.toEpochMilli(),
                            job.updatedAt.toEpochMilli(),
                            job.lastAcquiredAt?.toEpochMilli(),
                            job.acquiredByWorkerId,
                            job.version,
                            job.inputData?.let { Buffer.buffer(it) },
                            job.outputData?.let { Buffer.buffer(it) },
                        )
                    )
                    .coAwait()
                insertEventQuery(conn, event)
                tx.commit().coAwait()
                return job
            } catch (e: PgException) {
                tx.rollback().coAwait()
                if (e.constraint == CONSTRAINT_JOBS_PK)
                    throw DuplicateIdempotencyKeyException(job.groupId, job.idempotencyKey)
                throw e
            } catch (e: Exception) {
                tx.rollback().coAwait()
                throw e
            }
        } finally {
            conn.close().coAwait()
        }
    }

    override suspend fun findEventsByJobId(jobId: String): List<JobEvent> {
        val rowSet =
            pool
                .preparedQuery(
                    $$"SELECT * FROM job_events WHERE job_id = $1 ORDER BY created_at ASC"
                )
                .execute(Tuple.of(jobId))
                .coAwait()
        return rowSet.map { it.toJobEvent() }
    }

    private suspend fun insertEventQuery(conn: io.vertx.sqlclient.SqlClient, event: JobEvent) {
        conn
            .preparedQuery(
                $$"INSERT INTO job_events (id, job_id, event_type, actor_type, actor_id, created_at, event_message, event_detail) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"
            )
            .execute(
                Tuple.of(
                    event.id,
                    event.jobId,
                    event.eventType.name,
                    event.actorType.name,
                    event.actorId,
                    event.createdAt.toEpochMilli(),
                    event.eventMessage,
                    event.eventDetail,
                )
            )
            .coAwait()
    }

    override suspend fun saveCheckpoint(
        jobId: String,
        workerId: String,
        updatedAt: Instant,
        compute: (Job, Checkpoint?) -> Checkpoint,
    ): Checkpoint? {
        val conn = pool.connection.coAwait()
        try {
            val tx = conn.begin().coAwait()
            try {
                val jobRowSet =
                    conn
                        .preparedQuery(
                            $$"SELECT * FROM jobs WHERE id = $1 AND status = $2 AND acquired_by_worker_id = $3 FOR UPDATE SKIP LOCKED"
                        )
                        .execute(Tuple.of(jobId, JobStatus.ACQUIRED.name, workerId))
                        .coAwait()
                if (jobRowSet.rowCount() == 0) {
                    tx.rollback().coAwait()
                    return null
                }
                val job = jobRowSet.first().toJob()
                val checkpointRowSet =
                    conn
                        .preparedQuery(
                            $$"SELECT * FROM checkpoints WHERE job_id = $1 ORDER BY order_key DESC LIMIT 1"
                        )
                        .execute(Tuple.of(jobId))
                        .coAwait()
                val lastCheckpoint =
                    if (checkpointRowSet.rowCount() > 0) checkpointRowSet.first().toCheckpoint()
                    else null
                val checkpoint = compute(job, lastCheckpoint)
                conn
                    .preparedQuery(
                        $$"INSERT INTO checkpoints (id, job_id, previous_checkpoint_id, name, created_at, order_key, data) VALUES ($1, $2, $3, $4, $5, $6, $7)"
                    )
                    .execute(
                        Tuple.of(
                            checkpoint.id,
                            checkpoint.jobId,
                            checkpoint.previousCheckpointId,
                            checkpoint.name,
                            checkpoint.createdAt.toEpochMilli(),
                            checkpoint.orderKey,
                            Buffer.buffer(checkpoint.data),
                        )
                    )
                    .coAwait()
                val updateRowSet =
                    conn
                        .preparedQuery(
                            $$"UPDATE jobs SET retries = 0, updated_at = $1, version = version + 1 WHERE id = $2 AND version = $3"
                        )
                        .execute(Tuple.of(updatedAt.toEpochMilli(), jobId, job.version))
                        .coAwait()
                if (updateRowSet.rowCount() == 0) {
                    tx.rollback().coAwait()
                    throw JobConcurrentModificationException(jobId)
                }
                tx.commit().coAwait()
                return checkpoint
            } catch (e: PgException) {
                tx.rollback().coAwait()
                if (e.constraint in CHECKPOINT_CONSTRAINTS)
                    throw InvalidCheckpointOrderException(jobId, e)
                throw e
            } catch (e: Exception) {
                tx.rollback().coAwait()
                throw e
            }
        } finally {
            conn.close().coAwait()
        }
    }

    override suspend fun reportExecutionError(
        jobId: String,
        workerId: String,
        updatedAt: Instant,
        compute: (Job) -> ExecutionErrorResult,
    ): Job? {
        val conn = pool.connection.coAwait()
        try {
            val tx = conn.begin().coAwait()
            try {
                val selectRowSet =
                    conn
                        .preparedQuery(
                            $$"SELECT * FROM jobs WHERE id = $1 AND status = $2 AND acquired_by_worker_id = $3 FOR UPDATE SKIP LOCKED"
                        )
                        .execute(Tuple.of(jobId, JobStatus.ACQUIRED.name, workerId))
                        .coAwait()
                if (selectRowSet.rowCount() == 0) {
                    tx.rollback().coAwait()
                    return null
                }
                val job = selectRowSet.first().toJob()
                val result = compute(job)
                val updateRowSet =
                    conn
                        .preparedQuery(
                            $$"UPDATE jobs SET status = $1, retries = $2, acquired_by_worker_id = NULL, updated_at = $3, version = version + 1 WHERE id = $4 AND version = $5 RETURNING *"
                        )
                        .execute(
                            Tuple.of(
                                result.status.name,
                                result.retries,
                                updatedAt.toEpochMilli(),
                                jobId,
                                job.version,
                            )
                        )
                        .coAwait()
                if (updateRowSet.rowCount() == 0) {
                    tx.rollback().coAwait()
                    throw JobConcurrentModificationException(jobId)
                }
                for (event in result.events) {
                    insertEventQuery(conn, event)
                }
                tx.commit().coAwait()
                return updateRowSet.first().toJob()
            } catch (e: Exception) {
                tx.rollback().coAwait()
                throw e
            }
        } finally {
            conn.close().coAwait()
        }
    }

    override suspend fun completeJob(
        jobId: String,
        workerId: String,
        outputData: ByteArray?,
        updatedAt: Instant,
        event: JobEvent,
    ): Job? {
        val conn = pool.connection.coAwait()
        try {
            val tx = conn.begin().coAwait()
            try {
                val rowSet =
                    conn
                        .preparedQuery(
                            $$"UPDATE jobs SET status = $1, acquired_by_worker_id = NULL, output_data = $2, updated_at = $3, version = version + 1 WHERE id = $4 AND status = $5 AND acquired_by_worker_id = $6 RETURNING *"
                        )
                        .execute(
                            Tuple.of(
                                JobStatus.FINISHED.name,
                                outputData?.let { Buffer.buffer(it) },
                                updatedAt.toEpochMilli(),
                                jobId,
                                JobStatus.ACQUIRED.name,
                                workerId,
                            )
                        )
                        .coAwait()
                if (rowSet.rowCount() == 0) {
                    tx.rollback().coAwait()
                    return null
                }
                insertEventQuery(conn, event)
                tx.commit().coAwait()
                return rowSet.first().toJob()
            } catch (e: Exception) {
                tx.rollback().coAwait()
                throw e
            }
        } finally {
            conn.close().coAwait()
        }
    }

    override suspend fun abortJob(jobId: String, updatedAt: Instant, event: JobEvent): Job? {
        val conn = pool.connection.coAwait()
        try {
            val tx = conn.begin().coAwait()
            try {
                val rowSet =
                    conn
                        .preparedQuery(
                            $$"UPDATE jobs SET status = $1, acquired_by_worker_id = NULL, updated_at = $2, version = version + 1 WHERE id = $3 AND status IN ($4, $5) RETURNING *"
                        )
                        .execute(
                            Tuple.of(
                                JobStatus.ABORTED.name,
                                updatedAt.toEpochMilli(),
                                jobId,
                                JobStatus.PENDING.name,
                                JobStatus.ACQUIRED.name,
                            )
                        )
                        .coAwait()
                if (rowSet.rowCount() == 0) {
                    tx.rollback().coAwait()
                    return null
                }
                insertEventQuery(conn, event)
                tx.commit().coAwait()
                return rowSet.first().toJob()
            } catch (e: Exception) {
                tx.rollback().coAwait()
                throw e
            }
        } finally {
            conn.close().coAwait()
        }
    }

    override suspend fun releaseJob(
        jobId: String,
        workerId: String,
        updatedAt: Instant,
        event: JobEvent,
    ): Job? {
        val conn = pool.connection.coAwait()
        try {
            val tx = conn.begin().coAwait()
            try {
                val rowSet =
                    conn
                        .preparedQuery(
                            $$"UPDATE jobs SET status = $1, acquired_by_worker_id = NULL, updated_at = $2, version = version + 1 WHERE id = $3 AND status = $4 AND acquired_by_worker_id = $5 RETURNING *"
                        )
                        .execute(
                            Tuple.of(
                                JobStatus.PENDING.name,
                                updatedAt.toEpochMilli(),
                                jobId,
                                JobStatus.ACQUIRED.name,
                                workerId,
                            )
                        )
                        .coAwait()
                if (rowSet.rowCount() == 0) {
                    tx.rollback().coAwait()
                    return null
                }
                insertEventQuery(conn, event)
                tx.commit().coAwait()
                return rowSet.first().toJob()
            } catch (e: Exception) {
                tx.rollback().coAwait()
                throw e
            }
        } finally {
            conn.close().coAwait()
        }
    }

    override suspend fun releaseJobsByWorkerId(workerId: String, updatedAt: Instant): Int =
        releaseJobsByWorkerIds(listOf(workerId), updatedAt)

    override suspend fun releaseJobsByWorkerIds(workerIds: List<String>, updatedAt: Instant): Int {
        if (workerIds.isEmpty()) return 0
        val placeholders = workerIds.indices.joinToString(",") { $$"$$${it + 3}" }
        val tuple = Tuple.tuple()
        tuple.addString(JobStatus.PENDING.name)
        tuple.addLong(updatedAt.toEpochMilli())
        workerIds.forEach { tuple.addString(it) }
        tuple.addString(JobStatus.ACQUIRED.name)
        val statusParamIdx = workerIds.size + 3
        val rowSet =
            pool
                .preparedQuery(
                    $$"UPDATE jobs SET status = $1, acquired_by_worker_id = NULL, updated_at = $2, version = version + 1 WHERE acquired_by_worker_id IN ($$placeholders) AND status = $$$statusParamIdx"
                )
                .execute(tuple)
                .coAwait()
        return rowSet.rowCount()
    }

    private fun Row.toCheckpoint(): Checkpoint =
        Checkpoint(
            id = getString("id"),
            jobId = getString("job_id"),
            previousCheckpointId = getString("previous_checkpoint_id"),
            name = getString("name"),
            createdAt = Instant.ofEpochMilli(getLong("created_at")),
            orderKey = getLong("order_key"),
            data = getBuffer("data").bytes,
        )

    private fun Row.toJobEvent(): JobEvent =
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

    private fun Row.toJob(): Job =
        Job(
            id = getString("id"),
            groupId = getString("group_id"),
            name = getString("name"),
            type = getString("type"),
            status = JobStatus.valueOf(getString("status")),
            retries = getInteger("retries"),
            maxRetries = getInteger("max_retries"),
            idempotencyKey = getString("idempotency_key"),
            inputData = getBuffer("input_data")?.bytes,
            createdAt = Instant.ofEpochMilli(getLong("created_at")),
            updatedAt = Instant.ofEpochMilli(getLong("updated_at")),
            lastAcquiredAt = getLong("last_acquired_at")?.let { Instant.ofEpochMilli(it) },
            acquiredByWorkerId = getString("acquired_by_worker_id"),
            outputData = getBuffer("output_data")?.bytes,
            version = getLong("version"),
        )
}
