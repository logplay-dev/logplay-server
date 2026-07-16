package org.zeplinko.logplay.server.job.adapters

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import java.time.Instant
import javax.sql.DataSource
import org.zeplinko.logplay.server.core.job.*
import org.zeplinko.logplay.server.core.worker.WorkerNotFoundException

/**
 * H2 implementation of [JobGateway] over the three-table state machine (`jobs`, `job_queue`,
 * `job_acquired`).
 *
 * **Serialisation model.** The `jobs` row is the single per-job serialisation point. Per-job
 * transitions (abort/complete/error/release/checkpoint) take `SELECT ... FOR UPDATE` on it via
 * [findAndLockJobById] for the lifetime of the enclosing transaction, then compose the primitive
 * writes across the secondary tables.
 *
 * The bulk movers serialise on the same `jobs` lock, but differently. [acquirePendingJobs] uses
 * `SELECT ... FOR UPDATE SKIP LOCKED` (see [lockJobRowsSkipLocked]) and acts only on the jobs it
 * locked — a contended job is skipped (it stays queued), so acquire never blocks or deadlocks.
 * [releaseJobsByWorkerIds] (the dead-worker reaper) instead BLOCKS and drains *every* one of the
 * worker's jobs (lock them jobs-first with `SELECT id FROM jobs WHERE id IN (…) FOR UPDATE`, then
 * re-read and move): it must leave no `job_acquired` row before the FK-constrained worker delete,
 * so it cannot skip; blocking is deadlock-free because worker-lifecycle ops serialise on the
 * `workers` row and the jobs-first order matches the per-job transitions. NOTE: unlike Postgres,
 * H2's FK validation takes no conflicting lock on the parent `jobs` row, so the explicit `jobs`
 * lock is load-bearing for both movers.
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

    override suspend fun acquirePendingJobs(
        groupId: String,
        type: String,
        workerId: String,
        limit: Int,
    ): List<Job> =
        dataSource.withConn(requireTransaction = true) { conn ->
            acquireOnConn(conn, groupId, type, workerId, limit, Instant.now().toEpochMilli())
        }

    /**
     * Core SKIP-LOCKED claim backing [acquirePendingJobs]: runs the queue→acquired move on [conn]
     * and assembles the [Job] views. No event insertion or transaction management — the caller owns
     * those.
     */
    private fun acquireOnConn(
        conn: Connection,
        groupId: String,
        type: String,
        workerId: String,
        limit: Int,
        now: Long,
    ): List<Job> {
        try {
            // 1) Pick eligible queue rows under SKIP LOCKED. The use case has already locked and
            //    liveness-checked the worker (findAndLockWorkerById), so no worker join is needed.
            val ready =
                conn
                    .prepareStatement(
                        """
                        SELECT q.job_id, q.group_id, q.type, q.enqueued_at, q.retries, q.available_at
                        FROM job_queue q
                        WHERE q.group_id = ? AND q.type = ? AND q.available_at <= ?
                        ORDER BY q.enqueued_at ASC
                        LIMIT ?
                        FOR UPDATE SKIP LOCKED
                        """
                            .trimIndent()
                    )
                    .use { stmt ->
                        stmt.setString(1, groupId)
                        stmt.setString(2, type)
                        stmt.setLong(3, now)
                        stmt.setInt(4, limit)
                        stmt.executeQuery().use { rs ->
                            val list = mutableListOf<QueueRow>()
                            while (rs.next()) list.add(rs.toQueueRow())
                            list
                        }
                    }
            if (ready.isEmpty()) return emptyList()
            // 1b) Lock the jobs rows for the claimed queue entries, skipping any that a concurrent
            //     per-job transition (abort/complete/error/release) holds via findAndLockJobById's
            //     `FOR UPDATE`. Only proceed with jobs we locked in BOTH tables — this is what
            //     serialises bulk acquire against those transitions (H2's FK does not). SKIP LOCKED
            //     keeps acquire non-blocking, so it can never deadlock against a transition that
            //     locks jobs-then-queue.
            val lockedIds = lockJobRowsSkipLocked(conn, ready.map { it.jobId })
            val claimable = ready.filter { it.jobId in lockedIds }
            if (claimable.isEmpty()) return emptyList()
            // 2) Delete all picked queue rows in one IN-list statement.
            val placeholders = claimable.joinToString(",") { "?" }
            conn.prepareStatement("DELETE FROM job_queue WHERE job_id IN ($placeholders)").use {
                stmt ->
                claimable.forEachIndexed { i, r -> stmt.setString(i + 1, r.jobId) }
                stmt.executeUpdate()
            }
            // 3) Insert all corresponding job_acquired rows in one multi-row INSERT.
            val rowSql = "(?, ?, ?, ?, ?, ?)"
            val multiRowPlaceholders = claimable.joinToString(",") { rowSql }
            conn
                .prepareStatement(
                    "INSERT INTO job_acquired (job_id, group_id, type, acquired_by_worker_id, acquired_at, retries) VALUES $multiRowPlaceholders"
                )
                .use { stmt ->
                    var idx = 1
                    for (r in claimable) {
                        stmt.setString(idx++, r.jobId)
                        stmt.setString(idx++, r.groupId)
                        stmt.setString(idx++, r.type)
                        stmt.setString(idx++, workerId)
                        stmt.setLong(idx++, now)
                        stmt.setInt(idx++, r.retries)
                    }
                    stmt.executeUpdate()
                }
            // 4) Fetch base `jobs` rows in a single round-trip, then assemble the ACQUIRED-shape
            // Job
            //    views in memory. Skips the per-job readJob loop — we just inserted into
            //    job_acquired ourselves so we already know the workerId/now/retries; only the
            //    immutable base columns need a query.
            val baseRows = readBaseJobs(conn, claimable.map { it.jobId })
            val acquiredAt = Instant.ofEpochMilli(now)
            return claimable.map { r ->
                val base =
                    baseRows[r.jobId] ?: error("base jobs row missing for acquired ${r.jobId}")
                // Defensive tripwire: the three-table invariant forbids a terminal `jobs` row
                // coexisting with a `job_queue` entry. Test coverage enforces this for
                // gateway-driven writes, so this only fires if the invariant is broken by external
                // means (manual SQL, restored backup).
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
        } catch (e: SQLException) {
            if (e.matchesConstraint(CONSTRAINT_FK_JOB_ACQUIRED_WORKER))
                throw WorkerNotFoundException(workerId)
            throw e
        }
    }

    /**
     * Lock the given `jobs` rows with `FOR UPDATE SKIP LOCKED`, returning the ids actually locked.
     * Bulk movers ([acquirePendingJobs], [releaseJobsByWorkerIds]) call this so they serialise on
     * the same `jobs` row that per-job transitions take via [findAndLockJobById] — a job currently
     * locked by an abort/complete/error/release is skipped (left for that transition). Using SKIP
     * LOCKED (never blocking) is what keeps the bulk movers out of any deadlock cycle with those
     * transitions, which lock jobs-then-secondary in the opposite order.
     */
    private fun lockJobRowsSkipLocked(conn: Connection, jobIds: List<String>): Set<String> {
        if (jobIds.isEmpty()) return emptySet()
        val placeholders = jobIds.joinToString(",") { "?" }
        return conn
            .prepareStatement(
                "SELECT id FROM jobs WHERE id IN ($placeholders) FOR UPDATE SKIP LOCKED"
            )
            .use { stmt ->
                jobIds.forEachIndexed { i, id -> stmt.setString(i + 1, id) }
                stmt.executeQuery().use { rs ->
                    val locked = mutableSetOf<String>()
                    while (rs.next()) locked.add(rs.getString("id"))
                    locked
                }
            }
    }

    override suspend fun findJobById(id: String): Job? =
        dataSource.withConn { conn -> readJob(conn, id) }

    override suspend fun findAndLockJobById(id: String): Job? =
        dataSource.withConn(requireTransaction = true) { conn ->
            // H2 honours FOR UPDATE only on single-table queries, so take the jobs-row lock with an
            // explicit statement — this is what serialises concurrent per-job transitions (and,
            // via the FK-induced lock on `jobs`, against acquire). A FOR UPDATE on the assembled
            // 3-way LEFT JOIN does not lock the row in H2.
            val locked =
                conn.prepareStatement("SELECT 1 FROM jobs WHERE id = ? FOR UPDATE").use { stmt ->
                    stmt.setString(1, id)
                    stmt.executeQuery().use { it.next() }
                }
            if (!locked) null else readJob(conn, id)
        }

    override suspend fun latestCheckpoint(jobId: String): Checkpoint? =
        dataSource.withConn { conn ->
            conn
                .prepareStatement(
                    "SELECT ${CHECKPOINT_COLUMNS.list()} FROM checkpoints WHERE job_id = ? ORDER BY order_key DESC LIMIT 1"
                )
                .use { stmt ->
                    stmt.setString(1, jobId)
                    stmt.executeQuery().use { rs -> if (rs.next()) rs.toCheckpoint() else null }
                }
        }

    override suspend fun findCheckpointById(id: String): Checkpoint? =
        dataSource.withConn { conn ->
            conn
                .prepareStatement(
                    "SELECT ${CHECKPOINT_COLUMNS.list()} FROM checkpoints WHERE id = ?"
                )
                .use { stmt ->
                    stmt.setString(1, id)
                    stmt.executeQuery().use { rs -> if (rs.next()) rs.toCheckpoint() else null }
                }
        }

    override suspend fun findCheckpointsByJobId(
        jobId: String,
        afterOrderKey: Long?,
        limit: Int,
    ): List<Checkpoint> =
        dataSource.withConn { conn ->
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

    override suspend fun insertJob(newJob: NewJob) {
        dataSource.withConn { conn ->
            try {
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
            } catch (e: SQLException) {
                if (e.matchesConstraint(CONSTRAINT_JOBS_PK))
                    throw DuplicateJobIdException(newJob.id, e)
                throw e
            }
        }
    }

    override suspend fun insertEvents(events: List<JobEvent>) {
        dataSource.withConn { conn -> insertEventsBatch(conn, events) }
    }

    override suspend fun removeFromQueue(jobId: String) {
        dataSource.withConn { conn ->
            conn.prepareStatement("DELETE FROM job_queue WHERE job_id = ?").use { stmt ->
                stmt.setString(1, jobId)
                stmt.executeUpdate()
            }
        }
    }

    override suspend fun removeFromAcquired(jobId: String) {
        dataSource.withConn { conn ->
            conn.prepareStatement("DELETE FROM job_acquired WHERE job_id = ?").use { stmt ->
                stmt.setString(1, jobId)
                stmt.executeUpdate()
            }
        }
    }

    override suspend fun enqueue(
        jobId: String,
        groupId: String,
        type: String,
        enqueuedAt: Instant,
        retries: Int,
        availableAt: Long,
    ) {
        dataSource.withConn { conn ->
            conn
                .prepareStatement(
                    "INSERT INTO job_queue (job_id, group_id, type, enqueued_at, retries, available_at) VALUES (?, ?, ?, ?, ?, ?)"
                )
                .use { stmt ->
                    stmt.setString(1, jobId)
                    stmt.setString(2, groupId)
                    stmt.setString(3, type)
                    stmt.setLong(4, enqueuedAt.toEpochMilli())
                    stmt.setInt(5, retries)
                    stmt.setLong(6, availableAt)
                    stmt.executeUpdate()
                }
        }
    }

    override suspend fun markTerminal(
        jobId: String,
        status: JobStatus,
        terminalAt: Instant,
        outputData: ByteArray?,
    ) {
        dataSource.withConn { conn ->
            conn
                .prepareStatement(
                    "UPDATE jobs SET terminal_status = ?, terminal_at = ?, output_data = ? WHERE id = ?"
                )
                .use { stmt ->
                    stmt.setString(1, status.name)
                    stmt.setLong(2, terminalAt.toEpochMilli())
                    stmt.setBytes(3, outputData)
                    stmt.setString(4, jobId)
                    stmt.executeUpdate()
                }
        }
    }

    override suspend fun insertCheckpoint(checkpoint: Checkpoint) {
        dataSource.withConn { conn ->
            try {
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
            } catch (e: SQLException) {
                if (e.matchesAnyConstraint(CHECKPOINT_CONSTRAINTS))
                    throw InvalidCheckpointOrderException(checkpoint.jobId, e)
                throw e
            }
        }
    }

    override suspend fun resetAcquiredRetries(jobId: String) {
        dataSource.withConn { conn ->
            conn.prepareStatement("UPDATE job_acquired SET retries = 0 WHERE job_id = ?").use { stmt
                ->
                stmt.setString(1, jobId)
                stmt.executeUpdate()
            }
        }
    }

    override suspend fun findEventsByJobId(jobId: String): List<JobEvent> =
        dataSource.withConn { conn ->
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

    override suspend fun releaseJobsByWorkerIds(
        workerIds: List<String>,
        updatedAt: Instant,
    ): List<String> =
        dataSource.withConn(requireTransaction = true) { conn ->
            releaseOnConn(conn, workerIds, updatedAt)
        }

    /**
     * Core bulk release backing [releaseJobsByWorkerIds]: atomically moves every `job_acquired` row
     * owned by [workerIds] back into `job_queue` (with `available_at = 0`) on [conn] and returns
     * the released job ids. No event insertion or transaction management.
     */
    private fun releaseOnConn(
        conn: Connection,
        workerIds: List<String>,
        updatedAt: Instant,
    ): List<String> {
        if (workerIds.isEmpty()) return emptyList()
        val workerPlaceholders = workerIds.joinToString(",") { "?" }
        // 1) Candidate jobs of these workers (unlocked read). The caller holds the worker-row lock,
        //    which blocks new acquisitions, so this set only shrinks.
        val candidateIds =
            conn
                .prepareStatement(
                    "SELECT job_id FROM job_acquired WHERE acquired_by_worker_id IN ($workerPlaceholders)"
                )
                .use { stmt ->
                    workerIds.forEachIndexed { i, id -> stmt.setString(i + 1, id) }
                    stmt.executeQuery().use { rs ->
                        val ids = mutableListOf<String>()
                        while (rs.next()) ids.add(rs.getString(1))
                        ids
                    }
                }
        if (candidateIds.isEmpty()) return emptyList()
        // 2) Lock those jobs rows — blocking, single-table, jobs-first. Waits out any in-flight
        //    per-job transition so we DRAIN every job: skipping a locked job would leave its
        //    job_acquired row and make the subsequent worker delete violate fk_job_acquired_worker.
        //    jobs-first + single-table avoids both a lock-order deadlock and a stale read.
        val jobPlaceholders = candidateIds.joinToString(",") { "?" }
        conn
            .prepareStatement("SELECT id FROM jobs WHERE id IN ($jobPlaceholders) FOR UPDATE")
            .use { stmt ->
                candidateIds.forEachIndexed { i, id -> stmt.setString(i + 1, id) }
                stmt.executeQuery().use { rs -> while (rs.next()) {} }
            }
        // 3) Fresh re-read of the rows still acquired (now stable under the jobs locks).
        val rows =
            conn
                .prepareStatement(
                    "SELECT job_id, group_id, type, retries FROM job_acquired WHERE acquired_by_worker_id IN ($workerPlaceholders)"
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
        if (rows.isEmpty()) return emptyList()
        // 4) Move every remaining acquired row back to the queue.
        val movePlaceholders = rows.joinToString(",") { "?" }
        conn.prepareStatement("DELETE FROM job_acquired WHERE job_id IN ($movePlaceholders)").use {
            stmt ->
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
        return rows.map { it.jobId }
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

    private fun ResultSet.toQueueRow(): QueueRow =
        QueueRow(
            jobId = getString("job_id"),
            groupId = getString("group_id"),
            type = getString("type"),
            enqueuedAt = getLong("enqueued_at"),
            retries = getInt("retries"),
            availableAt = getLong("available_at"),
        )

    /**
     * Assemble a denormalised [Job] from the three tables. Returns `null` if `jobs(id)` does not
     * exist; otherwise inspects `terminal_status`, then `job_acquired`, then `job_queue` to derive
     * the live status and per-state fields.
     */
    private fun readJob(conn: Connection, jobId: String): Job? {
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
