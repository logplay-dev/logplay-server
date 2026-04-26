package org.zeplinko.logplay.server.job.adapters

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import java.time.Instant
import javax.sql.DataSource
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.zeplinko.logplay.server.core.job.*
import org.zeplinko.logplay.server.core.worker.WorkerNotFoundException

/**
 * H2 implementation of [JobGateway] over the three-table state machine (`jobs`, `job_queue`,
 * `job_acquired`).
 *
 * **Serialisation model.** Per-job state transitions ([saveCheckpoint], [reportExecutionError],
 * [completeJob], [abortJob], [releaseJob]) open with `SELECT 1 FROM jobs WHERE id = ? FOR UPDATE`
 * and then perform rowcount-guarded DELETE+INSERT pairs across the secondary tables.
 *
 * [acquirePendingJobs] and [releaseJobsByWorkerIds] skip that per-job lock for throughput. They
 * still serialise against per-job transitions through (a) the FK-induced row lock on `jobs`
 * triggered by inserting/deleting `job_acquired` rows, which conflicts with `lockJob`'s `FOR
 * UPDATE`, and (b) row-level locks on the `job_acquired` rows themselves. The FK on
 * `job_acquired.job_id → jobs.id` is therefore load-bearing for correctness and must not be dropped
 * without replacing the lock.
 */
class H2JobGateway(private val dataSource: DataSource) : JobGateway {

    companion object {
        // jobs.id is derived deterministically from (groupId, idempotencyKey) by JobIdGenerator;
        // a PK collision always means a duplicate idempotency key was submitted. H2 auto-numbers
        // the PK constraint name, so we match on the stable column reference instead.
        private const val CONSTRAINT_JOBS_PK = "PUBLIC.JOBS(ID)"
        private const val CONSTRAINT_FK_JOB_ACQUIRED_WORKER = "FK_JOB_ACQUIRED_WORKER"

        // checkpoints.id is derived deterministically from (jobId, previousCheckpointId);
        // a PK or FK collision indicates a chain conflict.
        private const val CONSTRAINT_CHECKPOINTS_PK = "PUBLIC.CHECKPOINTS(ID)"
        private val CHECKPOINT_CONSTRAINTS =
            setOf(CONSTRAINT_CHECKPOINTS_PK, "FK_CHECKPOINT_JOB", "FK_CHECKPOINT_PREVIOUS")

        // Canonical column projections per table. Kept as ordered sets so callers can join,
        // qualify with a table alias, or generate placeholder lists without duplicating literals.
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

        /** "?, ?, ..., ?" matching the size of this set, for JDBC INSERT VALUES clauses. */
        private fun Set<String>.placeholders(): String = joinToString(", ") { "?" }
    }

    private fun SQLException.matchesConstraint(constraint: String): Boolean =
        message?.contains(constraint, ignoreCase = true) == true

    private fun SQLException.matchesAnyConstraint(constraints: Set<String>): Boolean =
        constraints.any { matchesConstraint(it) }

    /**
     * Take the per-job lock that serialises concurrent transitions. Returns `false` if the job does
     * not exist; callers must short-circuit in that case so they don't write events or
     * secondary-table rows for a vanished job.
     */
    private fun lockJob(conn: Connection, jobId: String): Boolean =
        conn.prepareStatement("SELECT 1 FROM jobs WHERE id = ? FOR UPDATE").use { stmt ->
            stmt.setString(1, jobId)
            stmt.executeQuery().use { rs -> rs.next() }
        }

    /**
     * Take the per-job lock and return the full base `jobs` row in a single round-trip. Callers
     * that mutate the job and want to return an assembled [Job] view use this instead of
     * [lockJob] + a trailing [readJob], avoiding one round-trip per call. Returns `null` if the job
     * row does not exist.
     */
    private fun lockJobReadingBase(conn: Connection, jobId: String): BaseJobRow? =
        conn
            .prepareStatement("SELECT ${JOB_COLUMNS.list()} FROM jobs WHERE id = ? FOR UPDATE")
            .use { stmt ->
                stmt.setString(1, jobId)
                stmt.executeQuery().use { rs -> if (rs.next()) rs.toBaseJob() else null }
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
                    val now = Instant.now().toEpochMilli()
                    // 1) Pick eligible queue rows under SKIP LOCKED. Worker existence + condemned
                    //    check is folded into the same query so a missing/condemned worker simply
                    //    returns no rows.
                    val ready =
                        conn
                            .prepareStatement(
                                """
                                SELECT q.job_id, q.group_id, q.type, q.enqueued_at, q.retries, q.available_at
                                FROM job_queue q
                                INNER JOIN workers w ON w.id = ? AND w.condemned = FALSE
                                WHERE q.group_id = ? AND q.type = ? AND q.available_at <= ?
                                ORDER BY q.enqueued_at ASC
                                LIMIT ?
                                FOR UPDATE SKIP LOCKED
                                """
                                    .trimIndent()
                            )
                            .use { stmt ->
                                stmt.setString(1, workerId)
                                stmt.setString(2, groupId)
                                stmt.setString(3, type)
                                stmt.setLong(4, now)
                                stmt.setInt(5, limit)
                                stmt.executeQuery().use { rs ->
                                    val list = mutableListOf<QueueRow>()
                                    while (rs.next()) list.add(rs.toQueueRow())
                                    list
                                }
                            }
                    if (ready.isEmpty()) {
                        conn.commit()
                        return@withContext emptyList()
                    }
                    // 2) Delete all picked queue rows in one IN-list statement.
                    val placeholders = ready.joinToString(",") { "?" }
                    conn
                        .prepareStatement("DELETE FROM job_queue WHERE job_id IN ($placeholders)")
                        .use { stmt ->
                            ready.forEachIndexed { i, r -> stmt.setString(i + 1, r.jobId) }
                            stmt.executeUpdate()
                        }
                    // 3) Insert all corresponding job_acquired rows in one multi-row INSERT.
                    val rowSql = "(?, ?, ?, ?, ?, ?)"
                    val multiRowPlaceholders = ready.joinToString(",") { rowSql }
                    conn
                        .prepareStatement(
                            "INSERT INTO job_acquired (job_id, group_id, type, acquired_by_worker_id, acquired_at, retries) VALUES $multiRowPlaceholders"
                        )
                        .use { stmt ->
                            var idx = 1
                            for (r in ready) {
                                stmt.setString(idx++, r.jobId)
                                stmt.setString(idx++, r.groupId)
                                stmt.setString(idx++, r.type)
                                stmt.setString(idx++, workerId)
                                stmt.setLong(idx++, now)
                                stmt.setInt(idx++, r.retries)
                            }
                            stmt.executeUpdate()
                        }
                    // 4) Fetch base `jobs` rows in a single round-trip, then assemble the
                    //    ACQUIRED-shape Job views in memory. Skips the per-job readJob loop —
                    //    we just inserted into job_acquired ourselves so we already know the
                    //    workerId/now/retries; only the immutable base columns need a query.
                    val baseRows = readBaseJobs(conn, ready.map { it.jobId })
                    val acquiredAt = Instant.ofEpochMilli(now)
                    val acquiredJobs =
                        ready.map { r ->
                            val base =
                                baseRows[r.jobId]
                                    ?: error("base jobs row missing for acquired ${r.jobId}")
                            // Defensive tripwire: the three-table invariant forbids a terminal
                            // `jobs` row coexisting with a `job_queue` entry. Test coverage
                            // enforces this for gateway-driven writes, so this only fires if
                            // the invariant is broken by external means (manual SQL, restored
                            // backup).
                            check(base.terminalStatus == null) {
                                "three-table invariant violated at acquire: job ${r.jobId} has terminal_status=${base.terminalStatus} but was in job_queue"
                            }
                            base.toJob(
                                status = JobStatus.ACQUIRED,
                                retries = r.retries,
                                acquiredByWorkerId = workerId,
                                lastAcquiredAt = acquiredAt,
                                availableAt = null,
                                terminalAt = null,
                                updatedAt = acquiredAt,
                            )
                        }
                    // 6) Bulk-insert events in one round-trip via JDBC batch.
                    if (eventFactory != null) {
                        insertEventsBatch(conn, acquiredJobs.map { eventFactory(it) })
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
        withContext(Dispatchers.IO) { dataSource.connection.use { conn -> readJob(conn, id) } }

    override suspend fun findCheckpointById(id: String): Checkpoint? =
        withContext(Dispatchers.IO) {
            dataSource.connection.use { conn ->
                conn
                    .prepareStatement(
                        "SELECT ${CHECKPOINT_COLUMNS.list()} FROM checkpoints WHERE id = ?"
                    )
                    .use { stmt ->
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
                            "SELECT ${CHECKPOINT_COLUMNS.list()} FROM checkpoints WHERE job_id = ? AND order_key > ? ORDER BY order_key ASC LIMIT ?"
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
                            "SELECT ${CHECKPOINT_COLUMNS.list()} FROM checkpoints WHERE job_id = ? ORDER BY order_key ASC LIMIT ?"
                        )
                        .use { stmt ->
                            stmt.setString(1, jobId)
                            stmt.setInt(2, limit)
                            stmt.executeQuery().use { rs -> rs.mapToCheckpoints() }
                        }
                }
            }
        }

    override suspend fun insertJobWithEvent(
        newJob: NewJob,
        event: JobEvent,
    ): InsertJobWithEventResult =
        withContext(Dispatchers.IO) {
            dataSource.connection.use { conn ->
                conn.autoCommit = false
                try {
                    // output_data is null for a freshly created job; terminal_status / terminal_at
                    // are also null until a terminal transition fires.
                    conn
                        .prepareStatement(
                            "INSERT INTO jobs (id, group_id, name, type, max_retries, input_data, output_data, created_at, terminal_status, terminal_at) VALUES (?, ?, ?, ?, ?, ?, NULL, ?, NULL, NULL)"
                        )
                        .use { stmt ->
                            stmt.setString(1, newJob.id)
                            stmt.setString(2, newJob.groupId)
                            stmt.setString(3, newJob.name)
                            stmt.setString(4, newJob.type)
                            stmt.setObject(5, newJob.maxRetries)
                            stmt.setBytes(6, newJob.inputData)
                            stmt.setLong(7, newJob.createdAt.toEpochMilli())
                            stmt.executeUpdate()
                        }
                    conn
                        .prepareStatement(
                            "INSERT INTO job_queue (job_id, group_id, type, enqueued_at, retries, available_at) VALUES (?, ?, ?, ?, 0, 0)"
                        )
                        .use { stmt ->
                            stmt.setString(1, newJob.id)
                            stmt.setString(2, newJob.groupId)
                            stmt.setString(3, newJob.type)
                            stmt.setLong(4, newJob.createdAt.toEpochMilli())
                            stmt.executeUpdate()
                        }
                    insertEventStatement(conn, event)
                    conn.commit()
                    InsertJobWithEventResult.Success(newJob.toPendingJob())
                } catch (e: SQLException) {
                    conn.rollback()
                    if (e.matchesConstraint(CONSTRAINT_JOBS_PK))
                        InsertJobWithEventResult.AlreadyExists
                    else throw e
                } catch (e: Exception) {
                    conn.rollback()
                    throw e
                } finally {
                    conn.autoCommit = true
                }
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

    override suspend fun findEventsByJobId(jobId: String): List<JobEvent> =
        withContext(Dispatchers.IO) {
            dataSource.connection.use { conn ->
                conn
                    .prepareStatement(
                        "SELECT ${JOB_EVENT_COLUMNS.list()} FROM job_events WHERE job_id = ? ORDER BY created_at ASC"
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

    /** Bind a [JobEvent] to a [PreparedStatement] in the column order of [JOB_EVENT_COLUMNS]. */
    private fun PreparedStatement.bindEvent(event: JobEvent) {
        setString(1, event.id)
        setString(2, event.jobId)
        setString(3, event.eventType.name)
        setString(4, event.actorType.name)
        setString(5, event.actorId)
        setLong(6, event.createdAt.toEpochMilli())
        setString(7, event.eventMessage)
        setString(8, event.eventDetail)
    }

    private fun insertEventStatement(conn: Connection, event: JobEvent) =
        insertEventsBatch(conn, listOf(event))

    private fun insertEventsBatch(conn: Connection, events: List<JobEvent>) {
        if (events.isEmpty()) return
        conn
            .prepareStatement(
                "INSERT INTO job_events (${JOB_EVENT_COLUMNS.list()}) VALUES (${JOB_EVENT_COLUMNS.placeholders()})"
            )
            .use { stmt ->
                for (event in events) {
                    stmt.bindEvent(event)
                    stmt.addBatch()
                }
                stmt.executeBatch()
            }
    }

    /** One-shot fetch of the immutable `jobs` columns for a set of ids, keyed by id. */
    private fun readBaseJobs(conn: Connection, ids: List<String>): Map<String, BaseJobRow> {
        if (ids.isEmpty()) return emptyMap()
        val placeholders = ids.joinToString(",") { "?" }
        return conn
            .prepareStatement("SELECT ${JOB_COLUMNS.list()} FROM jobs WHERE id IN ($placeholders)")
            .use { stmt ->
                ids.forEachIndexed { i, id -> stmt.setString(i + 1, id) }
                stmt.executeQuery().use { rs ->
                    val map = HashMap<String, BaseJobRow>(ids.size)
                    while (rs.next()) {
                        val row = rs.toBaseJob()
                        map[row.id] = row
                    }
                    map
                }
            }
    }

    override suspend fun saveCheckpoint(
        jobId: String,
        workerId: String,
        updatedAt: Instant,
        compute: (Checkpoint?) -> Checkpoint,
    ): SaveCheckpointResult =
        withContext(Dispatchers.IO) {
            dataSource.connection.use { conn ->
                conn.autoCommit = false
                try {
                    // Round-trip 1: lock jobs and pull terminal_status, the acquired owner, and
                    // the latest checkpoint in one statement. The LEFT JOINs let us classify
                    // NotFound / WrongStatus / WrongWorker under the same lock that gates the
                    // write below.
                    val probe =
                        conn
                            .prepareStatement(
                                """
                                SELECT j.terminal_status,
                                       a.acquired_by_worker_id,
                                       ${CHECKPOINT_COLUMNS.qualified("c")}
                                FROM jobs j
                                LEFT JOIN job_acquired a ON a.job_id = j.id
                                LEFT JOIN checkpoints c
                                    ON c.job_id = j.id
                                    AND c.order_key = (SELECT MAX(order_key) FROM checkpoints WHERE job_id = j.id)
                                WHERE j.id = ?
                                FOR UPDATE
                                """
                                    .trimIndent()
                            )
                            .use { stmt ->
                                stmt.setString(1, jobId)
                                stmt.executeQuery().use { rs ->
                                    if (!rs.next()) return@use null
                                    LeaseProbe(
                                        terminalStatus = rs.getString("terminal_status"),
                                        acquiredBy = rs.getString("acquired_by_worker_id"),
                                        lastCheckpoint =
                                            if (rs.getString("id") == null) null
                                            else rs.toCheckpoint(),
                                    )
                                }
                            }
                    if (probe == null) {
                        conn.rollback()
                        return@withContext SaveCheckpointResult.NotFound
                    }
                    probe.terminalStatus?.let { status ->
                        conn.rollback()
                        return@withContext SaveCheckpointResult.WrongStatus(
                            JobStatus.valueOf(status)
                        )
                    }
                    if (probe.acquiredBy == null) {
                        conn.rollback()
                        return@withContext SaveCheckpointResult.WrongStatus(JobStatus.PENDING)
                    }
                    if (probe.acquiredBy != workerId) {
                        conn.rollback()
                        return@withContext SaveCheckpointResult.WrongWorker(probe.acquiredBy)
                    }
                    val checkpoint = compute(probe.lastCheckpoint)
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
                    val updated =
                        conn
                            .prepareStatement(
                                "UPDATE job_acquired SET retries = 0 WHERE job_id = ? AND acquired_by_worker_id = ?"
                            )
                            .use { stmt ->
                                stmt.setString(1, jobId)
                                stmt.setString(2, workerId)
                                stmt.executeUpdate()
                            }
                    if (updated == 0) {
                        // The lock above gates all per-job transitions; the acquired row we just
                        // read cannot vanish under it.
                        conn.rollback()
                        error("three-table invariant violated: job $jobId acquired row vanished")
                    }
                    conn.commit()
                    SaveCheckpointResult.Success(checkpoint)
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
    ): ReportExecutionErrorResult =
        withContext(Dispatchers.IO) {
            dataSource.connection.use { conn ->
                conn.autoCommit = false
                try {
                    // Round-trip 1: lock + assembled-Job read in one statement. compute() needs
                    // max_retries (jobs) + retries (job_acquired) — both surface from the same
                    // 3-way LEFT JOIN.
                    val priorJob = readJob(conn, jobId, lockJobs = true)
                    if (priorJob == null) {
                        conn.rollback()
                        return@withContext ReportExecutionErrorResult.NotFound
                    }
                    if (priorJob.status != JobStatus.ACQUIRED) {
                        conn.rollback()
                        return@withContext ReportExecutionErrorResult.WrongStatus(priorJob.status)
                    }
                    if (priorJob.acquiredByWorkerId != workerId) {
                        conn.rollback()
                        return@withContext ReportExecutionErrorResult.WrongWorker(
                            priorJob.acquiredByWorkerId!!
                        )
                    }
                    val deleted =
                        conn
                            .prepareStatement(
                                "DELETE FROM job_acquired WHERE job_id = ? AND acquired_by_worker_id = ?"
                            )
                            .use { stmt ->
                                stmt.setString(1, jobId)
                                stmt.setString(2, workerId)
                                stmt.executeUpdate()
                            }
                    if (deleted == 0) {
                        // The lock above gates all per-job transitions; the acquired row we just
                        // read cannot vanish under it.
                        conn.rollback()
                        error("three-table invariant violated: job $jobId acquired row vanished")
                    }
                    val result = compute(priorJob)
                    val resultJob: Job =
                        when (result.status) {
                            JobStatus.PENDING -> {
                                conn
                                    .prepareStatement(
                                        "INSERT INTO job_queue (job_id, group_id, type, enqueued_at, retries, available_at) VALUES (?, ?, ?, ?, ?, 0)"
                                    )
                                    .use { stmt ->
                                        stmt.setString(1, jobId)
                                        stmt.setString(2, priorJob.groupId)
                                        stmt.setString(3, priorJob.type)
                                        stmt.setLong(4, updatedAt.toEpochMilli())
                                        stmt.setInt(5, result.retries)
                                        stmt.executeUpdate()
                                    }
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
                                    .prepareStatement(
                                        "UPDATE jobs SET terminal_status = ?, terminal_at = ? WHERE id = ?"
                                    )
                                    .use { stmt ->
                                        stmt.setString(1, JobStatus.FAILED.name)
                                        stmt.setLong(2, updatedAt.toEpochMilli())
                                        stmt.setString(3, jobId)
                                        stmt.executeUpdate()
                                    }
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
                    conn.commit()
                    ReportExecutionErrorResult.Success(resultJob)
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
    ): CompleteJobResult =
        withContext(Dispatchers.IO) {
            dataSource.connection.use { conn ->
                conn.autoCommit = false
                try {
                    val baseRow =
                        lockJobReadingBase(conn, jobId)
                            ?: run {
                                conn.rollback()
                                return@withContext CompleteJobResult.NotFound
                            }
                    baseRow.terminalStatus?.let { status ->
                        conn.rollback()
                        return@withContext CompleteJobResult.WrongStatus(JobStatus.valueOf(status))
                    }
                    val deleted =
                        conn
                            .prepareStatement(
                                "DELETE FROM job_acquired WHERE job_id = ? AND acquired_by_worker_id = ?"
                            )
                            .use { stmt ->
                                stmt.setString(1, jobId)
                                stmt.setString(2, workerId)
                                stmt.executeUpdate()
                            }
                    if (deleted == 0) {
                        // Cold path: classify the failure under the same lock. If the row exists
                        // in job_acquired, it's owned by a different worker; otherwise the job is
                        // PENDING (in job_queue) — terminal was ruled out above.
                        val acquiredBy =
                            conn
                                .prepareStatement(
                                    "SELECT acquired_by_worker_id FROM job_acquired WHERE job_id = ?"
                                )
                                .use { stmt ->
                                    stmt.setString(1, jobId)
                                    stmt.executeQuery().use { rs ->
                                        if (rs.next()) rs.getString(1) else null
                                    }
                                }
                        conn.rollback()
                        return@withContext if (acquiredBy != null)
                            CompleteJobResult.WrongWorker(acquiredBy)
                        else CompleteJobResult.WrongStatus(JobStatus.PENDING)
                    }
                    conn
                        .prepareStatement(
                            "UPDATE jobs SET terminal_status = ?, terminal_at = ?, output_data = ? WHERE id = ?"
                        )
                        .use { stmt ->
                            stmt.setString(1, JobStatus.FINISHED.name)
                            stmt.setLong(2, updatedAt.toEpochMilli())
                            stmt.setBytes(3, outputData)
                            stmt.setString(4, jobId)
                            stmt.executeUpdate()
                        }
                    insertEventStatement(conn, event)
                    conn.commit()
                    CompleteJobResult.Success(
                        baseRow.toTerminalJob(JobStatus.FINISHED, updatedAt, outputData)
                    )
                } catch (e: Exception) {
                    conn.rollback()
                    throw e
                } finally {
                    conn.autoCommit = true
                }
            }
        }

    override suspend fun abortJob(
        jobId: String,
        updatedAt: Instant,
        event: JobEvent,
    ): AbortJobResult =
        withContext(Dispatchers.IO) {
            dataSource.connection.use { conn ->
                conn.autoCommit = false
                try {
                    val baseRow =
                        lockJobReadingBase(conn, jobId)
                            ?: run {
                                conn.rollback()
                                return@withContext AbortJobResult.NotFound
                            }
                    baseRow.terminalStatus?.let { status ->
                        conn.rollback()
                        return@withContext AbortJobResult.AlreadyTerminal(JobStatus.valueOf(status))
                    }
                    val queueDeleted =
                        conn.prepareStatement("DELETE FROM job_queue WHERE job_id = ?").use { stmt
                            ->
                            stmt.setString(1, jobId)
                            stmt.executeUpdate()
                        }
                    val acquiredDeleted =
                        if (queueDeleted == 0) {
                            conn
                                .prepareStatement("DELETE FROM job_acquired WHERE job_id = ?")
                                .use { stmt ->
                                    stmt.setString(1, jobId)
                                    stmt.executeUpdate()
                                }
                        } else 0
                    if (queueDeleted == 0 && acquiredDeleted == 0) {
                        // Non-terminal job (checked above) must live in queue or acquired.
                        conn.rollback()
                        error("three-table invariant violated: job $jobId has no state row")
                    }
                    conn
                        .prepareStatement(
                            "UPDATE jobs SET terminal_status = ?, terminal_at = ? WHERE id = ?"
                        )
                        .use { stmt ->
                            stmt.setString(1, JobStatus.ABORTED.name)
                            stmt.setLong(2, updatedAt.toEpochMilli())
                            stmt.setString(3, jobId)
                            stmt.executeUpdate()
                        }
                    insertEventStatement(conn, event)
                    conn.commit()
                    AbortJobResult.Success(baseRow.toTerminalJob(JobStatus.ABORTED, updatedAt))
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
        availableAt: Long?,
        event: JobEvent,
    ): ReleaseJobResult =
        withContext(Dispatchers.IO) {
            dataSource.connection.use { conn ->
                conn.autoCommit = false
                try {
                    // Lock + read base columns + acquired columns in one LEFT JOIN so we can
                    // classify NotFound / WrongStatus(terminal) / WrongStatus(PENDING) /
                    // WrongWorker upfront — single round-trip for the lookup phase.
                    val probe =
                        conn
                            .prepareStatement(
                                """
                                SELECT ${JOB_COLUMNS.qualified("j")},
                                       a.acquired_by_worker_id,
                                       a.group_id  AS acquired_group_id,
                                       a.type      AS acquired_type,
                                       a.retries   AS acquired_retries
                                FROM jobs j
                                LEFT JOIN job_acquired a ON a.job_id = j.id
                                WHERE j.id = ?
                                FOR UPDATE
                                """
                                    .trimIndent()
                            )
                            .use { stmt ->
                                stmt.setString(1, jobId)
                                stmt.executeQuery().use { rs ->
                                    if (!rs.next()) return@use null
                                    rs.toBaseJob() to
                                        rs.getString("acquired_by_worker_id")?.let { ow ->
                                            AcquiredOwnership(
                                                workerId = ow,
                                                groupId = rs.getString("acquired_group_id"),
                                                type = rs.getString("acquired_type"),
                                                retries = rs.getInt("acquired_retries"),
                                            )
                                        }
                                }
                            }
                    if (probe == null) {
                        conn.rollback()
                        return@withContext ReleaseJobResult.NotFound
                    }
                    val (baseRow, acquired) = probe
                    baseRow.terminalStatus?.let { status ->
                        conn.rollback()
                        return@withContext ReleaseJobResult.WrongStatus(JobStatus.valueOf(status))
                    }
                    if (acquired == null) {
                        // Non-terminal job with no acquired row must be PENDING (in job_queue).
                        conn.rollback()
                        return@withContext ReleaseJobResult.WrongStatus(JobStatus.PENDING)
                    }
                    if (acquired.workerId != workerId) {
                        conn.rollback()
                        return@withContext ReleaseJobResult.WrongWorker(acquired.workerId)
                    }
                    val effectiveAvailableAt = availableAt ?: 0L
                    val acquiredGroupId = acquired.groupId
                    val acquiredType = acquired.type
                    val acquiredRetries = acquired.retries

                    val deleted =
                        conn
                            .prepareStatement(
                                "DELETE FROM job_acquired WHERE job_id = ? AND acquired_by_worker_id = ?"
                            )
                            .use { stmt ->
                                stmt.setString(1, jobId)
                                stmt.setString(2, workerId)
                                stmt.executeUpdate()
                            }
                    if (deleted == 0) {
                        // The lock above gates all per-job transitions; the acquired row we just
                        // read cannot vanish under it.
                        conn.rollback()
                        error("three-table invariant violated: job $jobId acquired row vanished")
                    }
                    conn
                        .prepareStatement(
                            "INSERT INTO job_queue (job_id, group_id, type, enqueued_at, retries, available_at) VALUES (?, ?, ?, ?, ?, ?)"
                        )
                        .use { stmt ->
                            stmt.setString(1, jobId)
                            stmt.setString(2, acquiredGroupId)
                            stmt.setString(3, acquiredType)
                            stmt.setLong(4, updatedAt.toEpochMilli())
                            stmt.setInt(5, acquiredRetries)
                            stmt.setLong(6, effectiveAvailableAt)
                            stmt.executeUpdate()
                        }
                    insertEventStatement(conn, event)
                    conn.commit()
                    ReleaseJobResult.Success(
                        baseRow.toPendingJob(
                            retries = acquiredRetries,
                            availableAt = effectiveAvailableAt,
                            enqueuedAt = updatedAt,
                        )
                    )
                } catch (e: Exception) {
                    conn.rollback()
                    throw e
                } finally {
                    conn.autoCommit = true
                }
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
    ): Int =
        withContext(Dispatchers.IO) {
            if (workerIds.isEmpty()) return@withContext 0
            dataSource.connection.use { conn ->
                conn.autoCommit = false
                try {
                    val placeholders = workerIds.joinToString(",") { "?" }
                    // Snapshot the rows to be released so we can re-insert into job_queue.
                    val rows =
                        conn
                            .prepareStatement(
                                "SELECT job_id, group_id, type, retries FROM job_acquired WHERE acquired_by_worker_id IN ($placeholders) FOR UPDATE"
                            )
                            .use { stmt ->
                                workerIds.forEachIndexed { i, id -> stmt.setString(i + 1, id) }
                                stmt.executeQuery().use { rs ->
                                    val list = mutableListOf<AcquiredRowSnapshot>()
                                    while (rs.next()) {
                                        list.add(
                                            AcquiredRowSnapshot(
                                                jobId = rs.getString("job_id"),
                                                groupId = rs.getString("group_id"),
                                                type = rs.getString("type"),
                                                retries = rs.getInt("retries"),
                                            )
                                        )
                                    }
                                    list
                                }
                            }
                    if (rows.isEmpty()) {
                        conn.commit()
                        return@withContext 0
                    }
                    val jobPlaceholders = rows.joinToString(",") { "?" }
                    conn
                        .prepareStatement(
                            "DELETE FROM job_acquired WHERE job_id IN ($jobPlaceholders)"
                        )
                        .use { stmt ->
                            rows.forEachIndexed { i, r -> stmt.setString(i + 1, r.jobId) }
                            stmt.executeUpdate()
                        }
                    conn
                        .prepareStatement(
                            "INSERT INTO job_queue (job_id, group_id, type, enqueued_at, retries, available_at) VALUES (?, ?, ?, ?, ?, 0)"
                        )
                        .use { stmt ->
                            for (r in rows) {
                                stmt.setString(1, r.jobId)
                                stmt.setString(2, r.groupId)
                                stmt.setString(3, r.type)
                                stmt.setLong(4, updatedAt.toEpochMilli())
                                stmt.setInt(5, r.retries)
                                stmt.addBatch()
                            }
                            stmt.executeBatch()
                        }
                    insertEventsBatch(conn, rows.map { eventFactory(it.jobId) })
                    conn.commit()
                    rows.size
                } catch (e: Exception) {
                    conn.rollback()
                    throw e
                } finally {
                    conn.autoCommit = true
                }
            }
        }

    // --- Read helpers ---

    private data class QueueRow(
        val jobId: String,
        val groupId: String,
        val type: String,
        val enqueuedAt: Long,
        val retries: Int,
        val availableAt: Long,
    )

    private data class AcquiredRowSnapshot(
        val jobId: String,
        val groupId: String,
        val type: String,
        val retries: Int,
    )

    private data class LeaseProbe(
        val terminalStatus: String?,
        val acquiredBy: String?,
        val lastCheckpoint: Checkpoint?,
    )

    private data class AcquiredOwnership(
        val workerId: String,
        val groupId: String,
        val type: String,
        val retries: Int,
    )

    private data class AcquiredRow(
        val workerId: String,
        val acquiredAt: Long,
        val retries: Int,
        val groupId: String,
        val type: String,
    )

    private fun ResultSet.toQueueRow(): QueueRow =
        QueueRow(
            jobId = getString("job_id"),
            groupId = getString("group_id"),
            type = getString("type"),
            enqueuedAt = getLong("enqueued_at"),
            retries = getInt("retries"),
            availableAt = getLong("available_at"),
        )

    private fun readAcquired(conn: Connection, jobId: String): AcquiredRow? =
        conn
            .prepareStatement(
                "SELECT acquired_by_worker_id, acquired_at, retries, group_id, type FROM job_acquired WHERE job_id = ?"
            )
            .use { stmt ->
                stmt.setString(1, jobId)
                stmt.executeQuery().use { rs ->
                    if (rs.next())
                        AcquiredRow(
                            workerId = rs.getString("acquired_by_worker_id"),
                            acquiredAt = rs.getLong("acquired_at"),
                            retries = rs.getInt("retries"),
                            groupId = rs.getString("group_id"),
                            type = rs.getString("type"),
                        )
                    else null
                }
            }

    /**
     * Assemble a denormalised [Job] from the three tables. Returns `null` if `jobs(id)` does not
     * exist; otherwise inspects `terminal_status`, then `job_acquired`, then `job_queue` to derive
     * the live status and per-state fields.
     */
    private fun readJob(conn: Connection, jobId: String, lockJobs: Boolean = false): Job? {
        // H2's `FOR UPDATE` locks all base tables touched by the SELECT. For the assembled-Job
        // read this is acceptable since callers that pass lockJobs=true intend to mutate the
        // secondary tables in the same transaction anyway.
        val lockClause = if (lockJobs) "FOR UPDATE" else ""
        return conn
            .prepareStatement(
                """
                SELECT ${JOB_COLUMNS.qualified("j")},
                       a.acquired_by_worker_id, a.acquired_at,
                       a.retries AS acquired_retries,
                       q.retries AS queue_retries, q.available_at, q.enqueued_at
                FROM jobs j
                LEFT JOIN job_acquired a ON a.job_id = j.id
                LEFT JOIN job_queue q ON q.job_id = j.id
                WHERE j.id = ?
                $lockClause
                """
                    .trimIndent()
            )
            .use { stmt ->
                stmt.setString(1, jobId)
                stmt.executeQuery().use { rs ->
                    if (!rs.next()) return null
                    val jobRow = rs.toBaseJob()
                    if (jobRow.terminalStatus != null) {
                        // terminal_status and terminal_at are written together in the same
                        // UPDATE; if terminal_status is non-null, terminal_at must be too.
                        val terminalAt =
                            Instant.ofEpochMilli(
                                jobRow.terminalAt
                                    ?: error("terminal_at missing for terminal job ${jobRow.id}")
                            )
                        return jobRow.toJob(
                            status = JobStatus.valueOf(jobRow.terminalStatus),
                            retries = null,
                            acquiredByWorkerId = null,
                            lastAcquiredAt = null,
                            availableAt = null,
                            terminalAt = terminalAt,
                            updatedAt = terminalAt,
                        )
                    }
                    val acquiredWorkerId = rs.getString("acquired_by_worker_id")
                    if (acquiredWorkerId != null) {
                        val acquiredAt = Instant.ofEpochMilli(rs.getLong("acquired_at"))
                        return jobRow.toJob(
                            status = JobStatus.ACQUIRED,
                            retries = rs.getInt("acquired_retries"),
                            acquiredByWorkerId = acquiredWorkerId,
                            lastAcquiredAt = acquiredAt,
                            availableAt = null,
                            terminalAt = null,
                            updatedAt = acquiredAt,
                        )
                    }
                    val availableAt = rs.getLong("available_at")
                    if (!rs.wasNull()) {
                        return jobRow.toJob(
                            status = JobStatus.PENDING,
                            retries = rs.getInt("queue_retries"),
                            acquiredByWorkerId = null,
                            lastAcquiredAt = null,
                            availableAt = availableAt,
                            terminalAt = null,
                            updatedAt = Instant.ofEpochMilli(rs.getLong("enqueued_at")),
                        )
                    }
                    // Three-table invariant: a `jobs` row must always be matched by exactly
                    // one of `terminal_status` set, a `job_acquired` row, or a `job_queue`
                    // row. Every transition holds `SELECT 1 FROM jobs WHERE id = ? FOR UPDATE`
                    // over its entire DELETE+INSERT pair, so the in-flight empty window is
                    // unreachable from another reader. Reaching this branch means the
                    // invariant has been violated outside the gateway (cascade misfire,
                    // manual surgery, schema drift); fail loudly.
                    error("three-table invariant violated: job ${jobRow.id} has no state row")
                }
            }
    }

    private data class BaseJobRow(
        val id: String,
        val groupId: String,
        val name: String,
        val type: String,
        val maxRetries: Int?,
        val inputData: ByteArray?,
        val outputData: ByteArray?,
        val createdAt: Long,
        val terminalStatus: String?,
        val terminalAt: Long?,
    )

    private fun BaseJobRow.toJob(
        status: JobStatus,
        retries: Int?,
        acquiredByWorkerId: String?,
        lastAcquiredAt: Instant?,
        availableAt: Long?,
        terminalAt: Instant?,
        updatedAt: Instant,
        outputDataOverride: ByteArray? = outputData,
    ): Job =
        Job(
            id = id,
            groupId = groupId,
            name = name,
            type = type,
            status = status,
            retries = retries,
            maxRetries = maxRetries,
            inputData = inputData,
            createdAt = Instant.ofEpochMilli(createdAt),
            updatedAt = updatedAt,
            lastAcquiredAt = lastAcquiredAt,
            acquiredByWorkerId = acquiredByWorkerId,
            availableAt = availableAt,
            terminalAt = terminalAt,
            outputData = outputDataOverride,
        )

    private fun BaseJobRow.toTerminalJob(
        status: JobStatus,
        terminalAt: Instant,
        outputData: ByteArray? = this.outputData,
    ): Job =
        toJob(
            status = status,
            retries = null,
            acquiredByWorkerId = null,
            lastAcquiredAt = null,
            availableAt = null,
            terminalAt = terminalAt,
            updatedAt = terminalAt,
            outputDataOverride = outputData,
        )

    private fun BaseJobRow.toPendingJob(retries: Int, availableAt: Long, enqueuedAt: Instant): Job =
        toJob(
            status = JobStatus.PENDING,
            retries = retries,
            acquiredByWorkerId = null,
            lastAcquiredAt = null,
            availableAt = availableAt,
            terminalAt = null,
            updatedAt = enqueuedAt,
        )

    private fun ResultSet.toBaseJob(): BaseJobRow {
        val maxRetries = getObject("max_retries") as? Int
        val terminalAtMillis = getLong("terminal_at")
        val terminalAt = if (wasNull()) null else terminalAtMillis
        return BaseJobRow(
            id = getString("id"),
            groupId = getString("group_id"),
            name = getString("name"),
            type = getString("type"),
            maxRetries = maxRetries,
            inputData = getBytes("input_data"),
            outputData = getBytes("output_data"),
            createdAt = getLong("created_at"),
            terminalStatus = getString("terminal_status"),
            terminalAt = terminalAt,
        )
    }

    private fun ResultSet.mapToCheckpoints(): List<Checkpoint> {
        val checkpoints = mutableListOf<Checkpoint>()
        while (next()) checkpoints.add(toCheckpoint())
        return checkpoints
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
}
