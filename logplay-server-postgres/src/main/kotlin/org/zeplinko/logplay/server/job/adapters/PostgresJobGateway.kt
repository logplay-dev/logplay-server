package org.zeplinko.logplay.server.job.adapters

import io.vertx.core.buffer.Buffer
import io.vertx.kotlin.coroutines.coAwait
import io.vertx.pgclient.PgException
import io.vertx.sqlclient.Pool
import io.vertx.sqlclient.Row
import io.vertx.sqlclient.SqlConnection
import io.vertx.sqlclient.Tuple
import java.time.Instant
import org.zeplinko.logplay.server.core.job.*
import org.zeplinko.logplay.server.core.worker.WorkerNotFoundException

/**
 * Postgres implementation of [JobGateway] over the three-table state machine (`jobs`, `job_queue`,
 * `job_acquired`).
 *
 * **Serialisation model.** Per-job state transitions ([saveCheckpoint], [reportExecutionError],
 * [completeJob], [abortJob], [releaseJob]) open with `SELECT 1 FROM jobs WHERE id = ? FOR UPDATE`
 * and then perform rowcount-guarded DELETE+INSERT pairs across the secondary tables.
 *
 * [acquirePendingJobs] and [releaseJobsByWorkerIds] skip that per-job lock for throughput. They
 * still serialise against per-job transitions through (a) the FK-induced `FOR KEY SHARE` on `jobs`
 * triggered by inserting/deleting `job_acquired` rows, which conflicts with `lockJob`'s `FOR
 * UPDATE`, and (b) row-level locks on the `job_acquired` rows themselves. The FK on
 * `job_acquired.job_id → jobs.id` is therefore load-bearing for correctness and must not be dropped
 * without replacing the lock.
 */
class PostgresJobGateway(private val pool: Pool) : JobGateway {

    companion object {
        // jobs.id is derived deterministically from (groupId, idempotencyKey); a PK collision is
        // always a duplicate idempotency key.
        private const val CONSTRAINT_JOBS_PK = "jobs_pkey"
        private const val CONSTRAINT_FK_JOB_ACQUIRED_WORKER = "fk_job_acquired_worker"

        // checkpoints.id is derived deterministically from (jobId, previousCheckpointId).
        private const val CONSTRAINT_CHECKPOINTS_PK = "checkpoints_pkey"
        private val CHECKPOINT_CONSTRAINTS =
            setOf(CONSTRAINT_CHECKPOINTS_PK, "fk_checkpoint_job", "fk_checkpoint_previous")

        // Canonical column projections per table. Kept as ordered sets so callers can join,
        // qualify with a table alias, or iterate without duplicating literals across queries.
        // Insertion order is preserved by the underlying LinkedHashSet implementation, which
        // matters when a column list feeds an INSERT and must line up with its VALUES clause.
        private val JOB_COLUMNS =
            linkedSetOf(
                "id",
                "group_id",
                "name",
                "type",
                "max_retries",
                "input_data",
                "output_data",
                "created_at",
                "terminal_status",
                "terminal_at",
            )
        private val CHECKPOINT_COLUMNS =
            linkedSetOf(
                "id",
                "job_id",
                "previous_checkpoint_id",
                "name",
                "created_at",
                "order_key",
                "data",
            )
        private val JOB_EVENT_COLUMNS =
            linkedSetOf(
                "id",
                "job_id",
                "event_type",
                "actor_type",
                "actor_id",
                "created_at",
                "event_message",
                "event_detail",
            )

        private fun Set<String>.list(): String = joinToString(", ")

        private fun Set<String>.qualified(alias: String): String =
            joinToString(", ") { "$alias.$it" }

        /** "$1, $2, ..., $N" matching the size of this set, for Postgres VALUES clauses. */
        private fun Set<String>.placeholders(): String = (1..size).joinToString(", ") { $"$$it" }
    }

    private suspend fun lockJob(conn: SqlConnection, jobId: String): Boolean {
        val rs =
            conn
                .preparedQuery($$"SELECT 1 FROM jobs WHERE id = $1 FOR UPDATE")
                .execute(Tuple.of(jobId))
                .coAwait()
        return rs.rowCount() > 0
    }

    override suspend fun acquirePendingJobs(
        groupId: String,
        type: String,
        workerId: String,
        limit: Int,
        eventFactory: ((Job) -> JobEvent)?,
    ): List<Job> {
        val now = Instant.now().toEpochMilli()
        val conn = pool.connection.coAwait()
        try {
            val tx = conn.begin().coAwait()
            try {
                val rowSet =
                    conn
                        .preparedQuery(
                            $$"""
                            WITH ready AS (
                                SELECT q.job_id
                                FROM job_queue q
                                INNER JOIN workers w ON w.id = $1 AND w.condemned = FALSE
                                WHERE q.group_id = $2 AND q.type = $3 AND q.available_at <= $4
                                ORDER BY q.enqueued_at ASC
                                LIMIT $5
                                FOR UPDATE SKIP LOCKED
                            ),
                            deleted AS (
                                DELETE FROM job_queue q
                                USING ready r
                                WHERE q.job_id = r.job_id
                                RETURNING q.job_id, q.retries
                            )
                            INSERT INTO job_acquired (job_id, group_id, type, acquired_by_worker_id, acquired_at, retries)
                            SELECT job_id, $2, $3, $1, $4, retries FROM deleted
                            RETURNING job_id, retries
                            """
                                .trimIndent()
                        )
                        .execute(Tuple.of(workerId, groupId, type, now, limit))
                        .coAwait()
                if (rowSet.rowCount() == 0) {
                    tx.commit().coAwait()
                    return emptyList()
                }
                val acquiredRows = rowSet.map { it.getString("job_id") to it.getInteger("retries") }
                val ids = acquiredRows.map { it.first }
                // Fetch base `jobs` rows in a single round-trip and assemble Job views in
                // memory. We just inserted into job_acquired ourselves, so we already know the
                // workerId/now/retries — only the immutable base columns need a query.
                val baseRows = readBaseJobs(conn, ids)
                val acquiredJobs =
                    acquiredRows.map { (jobId, retries) ->
                        val row =
                            baseRows[jobId] ?: error("base jobs row missing for acquired $jobId")
                        // Defensive tripwire: the three-table invariant forbids a terminal `jobs`
                        // row coexisting with a `job_queue` entry. Test coverage enforces this for
                        // gateway-driven writes, so this only fires if the invariant is broken by
                        // external means (manual SQL, restored backup).
                        check(row.getString("terminal_status") == null) {
                            "three-table invariant violated at acquire: job $jobId has terminal_status=${row.getString("terminal_status")} but was in job_queue"
                        }
                        row.toAcquiredJob(
                            workerId = workerId,
                            acquiredAt = Instant.ofEpochMilli(now),
                            retries = retries,
                        )
                    }
                // Bulk-insert events in one round-trip via Vert.x batch.
                if (eventFactory != null) {
                    insertEventsBatch(conn, acquiredJobs.map { eventFactory(it) })
                }
                tx.commit().coAwait()
                return acquiredJobs
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
        val conn = pool.connection.coAwait()
        try {
            return readJob(conn, id)
        } finally {
            conn.close().coAwait()
        }
    }

    override suspend fun findCheckpointById(id: String): Checkpoint? {
        val rowSet =
            pool
                .preparedQuery(
                    $$"SELECT $${CHECKPOINT_COLUMNS.list()} FROM checkpoints WHERE id = $1"
                )
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
                        $$"SELECT $${CHECKPOINT_COLUMNS.list()} FROM checkpoints WHERE job_id = $1 AND order_key > $2 ORDER BY order_key ASC LIMIT $3"
                    )
                    .execute(Tuple.of(jobId, afterOrderKey, limit))
                    .coAwait()
            } else {
                pool
                    .preparedQuery(
                        $$"SELECT $${CHECKPOINT_COLUMNS.list()} FROM checkpoints WHERE job_id = $1 ORDER BY order_key ASC LIMIT $2"
                    )
                    .execute(Tuple.of(jobId, limit))
                    .coAwait()
            }
        return rowSet.map { it.toCheckpoint() }
    }

    override suspend fun insertJobWithEvent(
        newJob: NewJob,
        event: JobEvent,
    ): InsertJobWithEventResult {
        // Three independent inserts (jobs, job_queue, job_events) folded into one CTE so the
        // whole creation is a single auto-commit round-trip. PK collision on `jobs` aborts the
        // CTE and surfaces as InsertJobWithEventResult.AlreadyExists; FK constraint triggers
        // fire after the statement completes and see the parent row written by ins_job.
        // ins_queue and the event INSERT both `SELECT FROM ins_job` so PG evaluates ins_job
        // first — when the job already exists, the `jobs_pkey` violation surfaces
        // deterministically (rather than `job_queue_pkey` racing with it).
        try {
            pool
                .preparedQuery(
                    $$"""
                    WITH ins_job AS (
                        INSERT INTO jobs (id, group_id, name, type, max_retries, input_data,
                                          output_data, created_at, terminal_status, terminal_at)
                        VALUES ($1, $2, $3, $4, $5, $6, NULL, $7, NULL, NULL)
                        RETURNING id
                    ),
                    ins_queue AS (
                        INSERT INTO job_queue (job_id, group_id, type, enqueued_at, retries, available_at)
                        SELECT $1, $2, $4, $7, 0, 0 FROM ins_job
                        RETURNING 1
                    )
                    INSERT INTO job_events ($${JOB_EVENT_COLUMNS.list()})
                    SELECT $8, $9, $10, $11, $12, $13, $14, $15 FROM ins_job
                    """
                        .trimIndent()
                )
                .execute(
                    Tuple.of(
                        newJob.id,
                        newJob.groupId,
                        newJob.name,
                        newJob.type,
                        newJob.maxRetries,
                        newJob.inputData?.let { Buffer.buffer(it) },
                        newJob.createdAt.toEpochMilli(),
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
            return InsertJobWithEventResult.Success(newJob.toPendingJob())
        } catch (e: PgException) {
            if (e.constraint == CONSTRAINT_JOBS_PK) return InsertJobWithEventResult.AlreadyExists
            throw e
        }
    }

    /**
     * Project a freshly-inserted [NewJob] into its assembled PENDING [Job] view. Mirrors what
     * `readJob` would synthesise from the just-written `jobs` + `job_queue` rows.
     */
    private fun NewJob.toPendingJob(): Job =
        Job(
            id = id,
            groupId = groupId,
            name = name,
            type = type,
            status = JobStatus.PENDING,
            retries = 0,
            maxRetries = maxRetries,
            inputData = inputData,
            createdAt = createdAt,
            updatedAt = createdAt,
            lastAcquiredAt = null,
            acquiredByWorkerId = null,
            availableAt = 0L,
            terminalAt = null,
            outputData = null,
        )

    override suspend fun findEventsByJobId(jobId: String): List<JobEvent> {
        val rowSet =
            pool
                .preparedQuery(
                    $$"SELECT $${JOB_EVENT_COLUMNS.list()} FROM job_events WHERE job_id = $1 ORDER BY created_at ASC"
                )
                .execute(Tuple.of(jobId))
                .coAwait()
        return rowSet.map { it.toJobEvent() }
    }

    /** Project a [JobEvent] into a [Tuple] in the column order of [JOB_EVENT_COLUMNS]. */
    private fun JobEvent.toTuple(): Tuple =
        Tuple.of(
            id,
            jobId,
            eventType.name,
            actorType.name,
            actorId,
            createdAt.toEpochMilli(),
            eventMessage,
            eventDetail,
        )

    private suspend fun insertEventsBatch(conn: SqlConnection, events: List<JobEvent>) {
        if (events.isEmpty()) return
        conn
            .preparedQuery(
                $$"INSERT INTO job_events ($${JOB_EVENT_COLUMNS.list()}) VALUES ($${JOB_EVENT_COLUMNS.placeholders()})"
            )
            .executeBatch(events.map { it.toTuple() })
            .coAwait()
    }

    /** One-shot fetch of `jobs` rows for a set of ids, keyed by id. */
    private suspend fun readBaseJobs(conn: SqlConnection, ids: List<String>): Map<String, Row> {
        if (ids.isEmpty()) return emptyMap()
        val rs =
            conn
                .preparedQuery($$"SELECT $${JOB_COLUMNS.list()} FROM jobs WHERE id = ANY($1)")
                .execute(Tuple.of(ids.toTypedArray()))
                .coAwait()
        val map = HashMap<String, Row>(ids.size)
        for (row in rs) map[row.getString("id")] = row
        return map
    }

    override suspend fun saveCheckpoint(
        jobId: String,
        workerId: String,
        updatedAt: Instant,
        compute: (Checkpoint?) -> Checkpoint,
    ): SaveCheckpointResult {
        val conn = pool.connection.coAwait()
        try {
            val tx = conn.begin().coAwait()
            try {
                // Round-trip 1: lock jobs and pull terminal_status, the acquired owner, and the
                // latest checkpoint in one statement. The LEFT JOINs let us classify
                // NotFound / WrongStatus / WrongWorker under the same lock that gates the write.
                val leaseRowSet =
                    conn
                        .preparedQuery(
                            $$"""
                            SELECT j.terminal_status,
                                   a.acquired_by_worker_id,
                                   $${CHECKPOINT_COLUMNS.qualified("c")}
                            FROM jobs j
                            LEFT JOIN job_acquired a ON a.job_id = j.id
                            LEFT JOIN LATERAL (
                                SELECT $${CHECKPOINT_COLUMNS.list()}
                                FROM checkpoints
                                WHERE job_id = j.id
                                ORDER BY order_key DESC
                                LIMIT 1
                            ) c ON TRUE
                            WHERE j.id = $1
                            FOR UPDATE OF j
                            """
                                .trimIndent()
                        )
                        .execute(Tuple.of(jobId))
                        .coAwait()
                if (leaseRowSet.rowCount() == 0) {
                    tx.rollback().coAwait()
                    return SaveCheckpointResult.NotFound
                }
                val leaseRow = leaseRowSet.first()
                leaseRow.getString("terminal_status")?.let { status ->
                    tx.rollback().coAwait()
                    return SaveCheckpointResult.WrongStatus(JobStatus.valueOf(status))
                }
                val acquiredBy = leaseRow.getString("acquired_by_worker_id")
                if (acquiredBy == null) {
                    tx.rollback().coAwait()
                    return SaveCheckpointResult.WrongStatus(JobStatus.PENDING)
                }
                if (acquiredBy != workerId) {
                    tx.rollback().coAwait()
                    return SaveCheckpointResult.WrongWorker(acquiredBy)
                }
                val lastCheckpoint =
                    if (leaseRow.getString("id") != null) leaseRow.toCheckpoint() else null
                val checkpoint = compute(lastCheckpoint)
                // Round-trip 2: INSERT the checkpoint and reset retries atomically. If the
                // lease was revoked by a concurrent bulk release in the gap above, the UPDATE
                // matches 0 rows and we roll back the INSERT — same null signal callers see for
                // any other ownership-precondition failure.
                val writeRowSet =
                    conn
                        .preparedQuery(
                            $$"""
                            WITH ins AS (
                                INSERT INTO checkpoints (id, job_id, previous_checkpoint_id, name, created_at, order_key, data)
                                VALUES ($1, $2, $3, $4, $5, $6, $7)
                                RETURNING job_id
                            ),
                            upd AS (
                                UPDATE job_acquired SET retries = 0
                                WHERE job_id = $2 AND acquired_by_worker_id = $8
                                RETURNING job_id
                            )
                            SELECT
                                (SELECT count(*) FROM ins)::int AS ins_count,
                                (SELECT count(*) FROM upd)::int AS upd_count
                            """
                                .trimIndent()
                        )
                        .execute(
                            Tuple.of(
                                checkpoint.id,
                                checkpoint.jobId,
                                checkpoint.previousCheckpointId,
                                checkpoint.name,
                                checkpoint.createdAt.toEpochMilli(),
                                checkpoint.orderKey,
                                checkpoint.data?.let { Buffer.buffer(it) },
                                workerId,
                            )
                        )
                        .coAwait()
                val updCount = writeRowSet.first().getInteger("upd_count") ?: 0
                if (updCount == 0) {
                    // The lock above gates all per-job transitions; the acquired row we just
                    // read cannot vanish under it.
                    tx.rollback().coAwait()
                    error("three-table invariant violated: job $jobId acquired row vanished")
                }
                tx.commit().coAwait()
                return SaveCheckpointResult.Success(checkpoint)
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
    ): ReportExecutionErrorResult {
        val conn = pool.connection.coAwait()
        try {
            val tx = conn.begin().coAwait()
            try {
                // Round-trip 1: lock jobs and read the full assembled Job in one statement.
                // compute() needs max_retries (jobs) + retries (job_acquired) — both come from
                // the same 3-way LEFT JOIN that readJob already does.
                val priorJob = readJob(conn, jobId, lockJobs = true)
                if (priorJob == null) {
                    tx.rollback().coAwait()
                    return ReportExecutionErrorResult.NotFound
                }
                if (priorJob.status != JobStatus.ACQUIRED) {
                    tx.rollback().coAwait()
                    return ReportExecutionErrorResult.WrongStatus(priorJob.status)
                }
                if (priorJob.acquiredByWorkerId != workerId) {
                    tx.rollback().coAwait()
                    return ReportExecutionErrorResult.WrongWorker(priorJob.acquiredByWorkerId!!)
                }
                val deleted =
                    conn
                        .preparedQuery(
                            $$"DELETE FROM job_acquired WHERE job_id = $1 AND acquired_by_worker_id = $2"
                        )
                        .execute(Tuple.of(jobId, workerId))
                        .coAwait()
                if (deleted.rowCount() == 0) {
                    // The lock above gates all per-job transitions; the acquired row we just
                    // read cannot vanish under it.
                    tx.rollback().coAwait()
                    error("three-table invariant violated: job $jobId acquired row vanished")
                }
                val result = compute(priorJob)
                val resultJob: Job =
                    when (result.status) {
                        JobStatus.PENDING -> {
                            conn
                                .preparedQuery(
                                    $$"INSERT INTO job_queue (job_id, group_id, type, enqueued_at, retries, available_at) VALUES ($1, $2, $3, $4, $5, 0)"
                                )
                                .execute(
                                    Tuple.of(
                                        jobId,
                                        priorJob.groupId,
                                        priorJob.type,
                                        updatedAt.toEpochMilli(),
                                        result.retries,
                                    )
                                )
                                .coAwait()
                            priorJob.copy(
                                status = JobStatus.PENDING,
                                retries = result.retries,
                                acquiredByWorkerId = null,
                                lastAcquiredAt = null,
                                availableAt = 0L,
                                updatedAt = updatedAt,
                            )
                        }
                        JobStatus.FAILED -> {
                            conn
                                .preparedQuery(
                                    $$"UPDATE jobs SET terminal_status = $1, terminal_at = $2 WHERE id = $3"
                                )
                                .execute(
                                    Tuple.of(JobStatus.FAILED.name, updatedAt.toEpochMilli(), jobId)
                                )
                                .coAwait()
                            priorJob.copy(
                                status = JobStatus.FAILED,
                                retries = null,
                                acquiredByWorkerId = null,
                                lastAcquiredAt = null,
                                availableAt = null,
                                terminalAt = updatedAt,
                                updatedAt = updatedAt,
                            )
                        }
                        else ->
                            error(
                                "reportExecutionError can only resolve to PENDING or FAILED, got ${result.status}"
                            )
                    }
                insertEventsBatch(conn, result.events)
                tx.commit().coAwait()
                return ReportExecutionErrorResult.Success(resultJob)
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
    ): CompleteJobResult {
        val conn = pool.connection.coAwait()
        try {
            val tx = conn.begin().coAwait()
            try {
                // Lock + fetch terminal_status in one round-trip; classify
                // NotFound / WrongStatus(terminal) before touching job_acquired.
                val lockRowSet =
                    conn
                        .preparedQuery(
                            $$"SELECT terminal_status FROM jobs WHERE id = $1 FOR UPDATE"
                        )
                        .execute(Tuple.of(jobId))
                        .coAwait()
                if (lockRowSet.rowCount() == 0) {
                    tx.rollback().coAwait()
                    return CompleteJobResult.NotFound
                }
                lockRowSet.first().getString("terminal_status")?.let { status ->
                    tx.rollback().coAwait()
                    return CompleteJobResult.WrongStatus(JobStatus.valueOf(status))
                }
                val deleted =
                    conn
                        .preparedQuery(
                            $$"DELETE FROM job_acquired WHERE job_id = $1 AND acquired_by_worker_id = $2"
                        )
                        .execute(Tuple.of(jobId, workerId))
                        .coAwait()
                if (deleted.rowCount() == 0) {
                    // Cold path: classify the failure under the same lock. Non-terminal job that
                    // didn't yield an owned acquired row is either owned by someone else or
                    // PENDING.
                    val ownerRowSet =
                        conn
                            .preparedQuery(
                                $$"SELECT acquired_by_worker_id FROM job_acquired WHERE job_id = $1"
                            )
                            .execute(Tuple.of(jobId))
                            .coAwait()
                    val acquiredBy =
                        if (ownerRowSet.rowCount() > 0)
                            ownerRowSet.first().getString("acquired_by_worker_id")
                        else null
                    tx.rollback().coAwait()
                    return if (acquiredBy != null) CompleteJobResult.WrongWorker(acquiredBy)
                    else CompleteJobResult.WrongStatus(JobStatus.PENDING)
                }
                // Combined UPDATE + event INSERT in one round-trip. UPDATE...RETURNING gives us
                // every base column we'd otherwise have to re-fetch via readJob to assemble the
                // terminal Job view.
                val updatedRowSet =
                    conn
                        .preparedQuery(
                            $$"""
                            WITH upd AS (
                                UPDATE jobs SET terminal_status = $1, terminal_at = $2, output_data = $3
                                WHERE id = $4
                                RETURNING $${JOB_COLUMNS.list()}
                            ),
                            ev AS (
                                INSERT INTO job_events ($${JOB_EVENT_COLUMNS.list()})
                                VALUES ($5, $6, $7, $8, $9, $10, $11, $12)
                                RETURNING 1
                            )
                            SELECT $${JOB_COLUMNS.list()} FROM upd
                            """
                                .trimIndent()
                        )
                        .execute(
                            Tuple.of(
                                JobStatus.FINISHED.name,
                                updatedAt.toEpochMilli(),
                                outputData?.let { Buffer.buffer(it) },
                                jobId,
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
                tx.commit().coAwait()
                return CompleteJobResult.Success(
                    updatedRowSet.first().toTerminalJob(JobStatus.FINISHED, updatedAt)
                )
            } catch (e: Exception) {
                tx.rollback().coAwait()
                throw e
            }
        } finally {
            conn.close().coAwait()
        }
    }

    override suspend fun abortJob(
        jobId: String,
        updatedAt: Instant,
        event: JobEvent,
    ): AbortJobResult {
        val conn = pool.connection.coAwait()
        try {
            val tx = conn.begin().coAwait()
            try {
                // Lock + fetch terminal_status in one round-trip so we can classify
                // NotFound / AlreadyTerminal before touching the secondary tables.
                val lockRowSet =
                    conn
                        .preparedQuery(
                            $$"SELECT terminal_status FROM jobs WHERE id = $1 FOR UPDATE"
                        )
                        .execute(Tuple.of(jobId))
                        .coAwait()
                if (lockRowSet.rowCount() == 0) {
                    tx.rollback().coAwait()
                    return AbortJobResult.NotFound
                }
                lockRowSet.first().getString("terminal_status")?.let { status ->
                    tx.rollback().coAwait()
                    return AbortJobResult.AlreadyTerminal(JobStatus.valueOf(status))
                }
                // Probe both secondary tables in one round-trip. One must match — the
                // non-terminal check above guarantees the job lives in queue or acquired.
                val deletedRow =
                    conn
                        .preparedQuery(
                            $$"""
                            WITH q_del AS (DELETE FROM job_queue WHERE job_id = $1 RETURNING 1),
                                 a_del AS (DELETE FROM job_acquired WHERE job_id = $1 RETURNING 1)
                            SELECT (SELECT count(*) FROM q_del)::int
                                 + (SELECT count(*) FROM a_del)::int AS total
                            """
                                .trimIndent()
                        )
                        .execute(Tuple.of(jobId))
                        .coAwait()
                        .first()
                if ((deletedRow.getInteger("total") ?: 0) == 0) {
                    tx.rollback().coAwait()
                    error("three-table invariant violated: job $jobId has no state row")
                }
                val updatedRowSet =
                    conn
                        .preparedQuery(
                            $$"""
                            WITH upd AS (
                                UPDATE jobs SET terminal_status = $1, terminal_at = $2
                                WHERE id = $3
                                RETURNING $${JOB_COLUMNS.list()}
                            ),
                            ev AS (
                                INSERT INTO job_events ($${JOB_EVENT_COLUMNS.list()})
                                VALUES ($4, $5, $6, $7, $8, $9, $10, $11)
                                RETURNING 1
                            )
                            SELECT $${JOB_COLUMNS.list()} FROM upd
                            """
                                .trimIndent()
                        )
                        .execute(
                            Tuple.of(
                                JobStatus.ABORTED.name,
                                updatedAt.toEpochMilli(),
                                jobId,
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
                tx.commit().coAwait()
                return AbortJobResult.Success(
                    updatedRowSet.first().toTerminalJob(JobStatus.ABORTED, updatedAt)
                )
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
        availableAt: Long?,
        event: JobEvent,
    ): ReleaseJobResult {
        val conn = pool.connection.coAwait()
        try {
            val tx = conn.begin().coAwait()
            try {
                // Lock + read base columns + acquired columns in one LEFT JOIN so we can
                // classify NotFound / WrongStatus(terminal) / WrongStatus(PENDING) /
                // WrongWorker upfront — single round-trip for the lookup phase.
                val leaseRowSet =
                    conn
                        .preparedQuery(
                            $$"""
                            SELECT $${JOB_COLUMNS.qualified("j")},
                                   a.acquired_by_worker_id,
                                   a.group_id AS acquired_group_id,
                                   a.type     AS acquired_type,
                                   a.retries  AS acquired_retries
                            FROM jobs j
                            LEFT JOIN job_acquired a ON a.job_id = j.id
                            WHERE j.id = $1
                            FOR UPDATE OF j
                            """
                                .trimIndent()
                        )
                        .execute(Tuple.of(jobId))
                        .coAwait()
                if (leaseRowSet.rowCount() == 0) {
                    tx.rollback().coAwait()
                    return ReleaseJobResult.NotFound
                }
                val leaseRow = leaseRowSet.first()
                leaseRow.getString("terminal_status")?.let { status ->
                    tx.rollback().coAwait()
                    return ReleaseJobResult.WrongStatus(JobStatus.valueOf(status))
                }
                val acquiredOwner = leaseRow.getString("acquired_by_worker_id")
                if (acquiredOwner == null) {
                    tx.rollback().coAwait()
                    return ReleaseJobResult.WrongStatus(JobStatus.PENDING)
                }
                if (acquiredOwner != workerId) {
                    tx.rollback().coAwait()
                    return ReleaseJobResult.WrongWorker(acquiredOwner)
                }
                val acquiredGroupId = leaseRow.getString("acquired_group_id")
                val acquiredType = leaseRow.getString("acquired_type")
                val acquiredRetries = leaseRow.getInteger("acquired_retries")
                val effectiveAvailableAt = availableAt ?: 0L

                val deleted =
                    conn
                        .preparedQuery(
                            $$"DELETE FROM job_acquired WHERE job_id = $1 AND acquired_by_worker_id = $2"
                        )
                        .execute(Tuple.of(jobId, workerId))
                        .coAwait()
                if (deleted.rowCount() == 0) {
                    // The lock above gates all per-job transitions; the acquired row we just
                    // read cannot vanish under it.
                    tx.rollback().coAwait()
                    error("three-table invariant violated: job $jobId acquired row vanished")
                }
                // Round-trip 3: INSERT job_queue + INSERT event in one CTE.
                conn
                    .preparedQuery(
                        $$"""
                        WITH q AS (
                            INSERT INTO job_queue (job_id, group_id, type, enqueued_at, retries, available_at)
                            VALUES ($1, $2, $3, $4, $5, $6)
                            RETURNING 1
                        ),
                        ev AS (
                            INSERT INTO job_events ($${JOB_EVENT_COLUMNS.list()})
                            VALUES ($7, $8, $9, $10, $11, $12, $13, $14)
                            RETURNING 1
                        )
                        SELECT (SELECT count(*) FROM q) + (SELECT count(*) FROM ev)
                        """
                            .trimIndent()
                    )
                    .execute(
                        Tuple.of(
                            jobId,
                            acquiredGroupId,
                            acquiredType,
                            updatedAt.toEpochMilli(),
                            acquiredRetries,
                            effectiveAvailableAt,
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
                tx.commit().coAwait()
                return ReleaseJobResult.Success(
                    leaseRow.toPendingJob(
                        retries = acquiredRetries,
                        availableAt = effectiveAvailableAt,
                        enqueuedAt = updatedAt,
                    )
                )
            } catch (e: Exception) {
                tx.rollback().coAwait()
                throw e
            }
        } finally {
            conn.close().coAwait()
        }
    }

    override suspend fun releaseJobsByWorkerId(
        workerId: String,
        updatedAt: Instant,
        eventFactory: (String) -> JobEvent,
    ): Int = releaseJobsByWorkerIds(listOf(workerId), updatedAt, eventFactory)

    override suspend fun releaseJobsByWorkerIds(
        workerIds: List<String>,
        updatedAt: Instant,
        eventFactory: (String) -> JobEvent,
    ): Int {
        if (workerIds.isEmpty()) return 0
        val conn = pool.connection.coAwait()
        try {
            val tx = conn.begin().coAwait()
            try {
                val placeholders = workerIds.indices.joinToString(",") { "$${it + 2}" }
                val tuple = Tuple.tuple()
                tuple.addLong(updatedAt.toEpochMilli())
                workerIds.forEach { tuple.addString(it) }
                // Atomic move: delete from job_acquired and re-insert into job_queue with
                // available_at = 0. The deadline is deliberately not preserved — the SDK's replay
                // path re-derives it from the checkpoint chain.
                val rs =
                    conn
                        .preparedQuery(
                            """
                            WITH released AS (
                                DELETE FROM job_acquired
                                WHERE acquired_by_worker_id IN ($placeholders)
                                RETURNING job_id, group_id, type, retries
                            )
                            INSERT INTO job_queue (job_id, group_id, type, enqueued_at, retries, available_at)
                            SELECT job_id, group_id, type, ${'$'}1, retries, 0 FROM released
                            RETURNING job_id
                            """
                                .trimIndent()
                        )
                        .execute(tuple)
                        .coAwait()
                val releasedIds = rs.map { it.getString("job_id") }
                if (releasedIds.isNotEmpty()) {
                    insertEventsBatch(conn, releasedIds.map { eventFactory(it) })
                }
                tx.commit().coAwait()
                return releasedIds.size
            } catch (e: Exception) {
                tx.rollback().coAwait()
                throw e
            }
        } finally {
            conn.close().coAwait()
        }
    }

    // --- Read helpers ---

    private data class AcquiredRow(
        val workerId: String,
        val acquiredAt: Long,
        val retries: Int,
        val groupId: String,
        val type: String,
    )

    /**
     * Assemble a denormalised [Job] from the three tables. Returns `null` if `jobs(id)` does not
     * exist; otherwise inspects `terminal_status`, then `job_acquired`, then `job_queue` to derive
     * the live status and per-state fields.
     */
    private suspend fun readJob(
        conn: SqlConnection,
        jobId: String,
        lockJobs: Boolean = false,
    ): Job? {
        // `FOR UPDATE OF j` locks only the jobs row; the LEFT-JOINed acquired/queue rows are
        // read without locks. Callers that intend to mutate the job pass lockJobs=true to fold
        // the per-job FOR UPDATE into the same trip as the state read.
        val lockClause = if (lockJobs) "FOR UPDATE OF j" else ""
        val rs =
            conn
                .preparedQuery(
                    $$"""
                    SELECT $${JOB_COLUMNS.qualified("j")},
                           a.acquired_by_worker_id, a.acquired_at,
                           a.retries AS acquired_retries,
                           q.retries AS queue_retries, q.available_at, q.enqueued_at
                    FROM jobs j
                    LEFT JOIN job_acquired a ON a.job_id = j.id
                    LEFT JOIN job_queue q ON q.job_id = j.id
                    WHERE j.id = $1
                    $$lockClause
                    """
                        .trimIndent()
                )
                .execute(Tuple.of(jobId))
                .coAwait()
        if (rs.rowCount() == 0) return null
        val row = rs.first()
        val terminalStatus = row.getString("terminal_status")
        if (terminalStatus != null) {
            // terminal_status and terminal_at are written together in the same UPDATE; if
            // terminal_status is non-null, terminal_at must be too.
            val terminalAt =
                Instant.ofEpochMilli(
                    row.getLong("terminal_at")
                        ?: error("terminal_at missing for terminal job ${row.getString("id")}")
                )
            return row.toJob(
                status = JobStatus.valueOf(terminalStatus),
                retries = null,
                acquiredByWorkerId = null,
                lastAcquiredAt = null,
                availableAt = null,
                terminalAt = terminalAt,
                updatedAt = terminalAt,
            )
        }
        val acquiredWorkerId = row.getString("acquired_by_worker_id")
        if (acquiredWorkerId != null) {
            val acquiredAt = Instant.ofEpochMilli(row.getLong("acquired_at"))
            return row.toJob(
                status = JobStatus.ACQUIRED,
                retries = row.getInteger("acquired_retries"),
                acquiredByWorkerId = acquiredWorkerId,
                lastAcquiredAt = acquiredAt,
                availableAt = null,
                terminalAt = null,
                updatedAt = acquiredAt,
            )
        }
        val queueAvailableAt = row.getLong("available_at")
        if (queueAvailableAt != null) {
            return row.toJob(
                status = JobStatus.PENDING,
                retries = row.getInteger("queue_retries"),
                acquiredByWorkerId = null,
                lastAcquiredAt = null,
                availableAt = queueAvailableAt,
                terminalAt = null,
                updatedAt = Instant.ofEpochMilli(row.getLong("enqueued_at")),
            )
        }
        // Three-table invariant: a `jobs` row must always be matched by exactly one of
        // `terminal_status` set, a `job_acquired` row, or a `job_queue` row. Every transition
        // holds `SELECT 1 FROM jobs WHERE id = ? FOR UPDATE` over its entire DELETE+INSERT
        // pair, and unlocked readers see Postgres MVCC snapshots — never the in-flight empty
        // window. Reaching this branch means the invariant has been violated outside the
        // gateway (cascade misfire, manual surgery, schema drift); fail loudly.
        error("three-table invariant violated: job ${row.getString("id")} has no state row")
    }

    private fun Row.toJob(
        status: JobStatus,
        retries: Int?,
        acquiredByWorkerId: String?,
        lastAcquiredAt: Instant?,
        availableAt: Long?,
        terminalAt: Instant?,
        updatedAt: Instant,
    ): Job =
        Job(
            id = getString("id"),
            groupId = getString("group_id"),
            name = getString("name"),
            type = getString("type"),
            status = status,
            retries = retries,
            maxRetries = getInteger("max_retries"),
            inputData = getBuffer("input_data")?.bytes,
            createdAt = Instant.ofEpochMilli(getLong("created_at")),
            updatedAt = updatedAt,
            lastAcquiredAt = lastAcquiredAt,
            acquiredByWorkerId = acquiredByWorkerId,
            availableAt = availableAt,
            terminalAt = terminalAt,
            outputData = getBuffer("output_data")?.bytes,
        )

    private fun Row.toAcquiredJob(workerId: String, acquiredAt: Instant, retries: Int): Job =
        toJob(
            status = JobStatus.ACQUIRED,
            retries = retries,
            acquiredByWorkerId = workerId,
            lastAcquiredAt = acquiredAt,
            availableAt = null,
            terminalAt = null,
            updatedAt = acquiredAt,
        )

    private fun Row.toTerminalJob(status: JobStatus, terminalAt: Instant): Job =
        toJob(
            status = status,
            retries = null,
            acquiredByWorkerId = null,
            lastAcquiredAt = null,
            availableAt = null,
            terminalAt = terminalAt,
            updatedAt = terminalAt,
        )

    private fun Row.toPendingJob(retries: Int, availableAt: Long, enqueuedAt: Instant): Job =
        toJob(
            status = JobStatus.PENDING,
            retries = retries,
            acquiredByWorkerId = null,
            lastAcquiredAt = null,
            availableAt = availableAt,
            terminalAt = null,
            updatedAt = enqueuedAt,
        )

    private fun Row.toCheckpoint(): Checkpoint =
        Checkpoint(
            id = getString("id"),
            jobId = getString("job_id"),
            previousCheckpointId = getString("previous_checkpoint_id"),
            name = getString("name"),
            createdAt = Instant.ofEpochMilli(getLong("created_at")),
            orderKey = getLong("order_key"),
            data = getBuffer("data")?.bytes,
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
}
