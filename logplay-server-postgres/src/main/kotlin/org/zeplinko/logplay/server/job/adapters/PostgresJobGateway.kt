package org.zeplinko.logplay.server.job.adapters

import io.vertx.core.buffer.Buffer
import io.vertx.kotlin.coroutines.coAwait
import io.vertx.pgclient.PgException
import io.vertx.sqlclient.Pool
import io.vertx.sqlclient.Row
import io.vertx.sqlclient.SqlClient
import io.vertx.sqlclient.SqlConnection
import io.vertx.sqlclient.Tuple
import java.time.Instant
import org.zeplinko.logplay.server.core.job.*
import org.zeplinko.logplay.server.core.worker.WorkerNotFoundException

/**
 * Postgres implementation of [JobGateway] over the three-table state machine (`jobs`, `job_queue`,
 * `job_acquired`).
 *
 * **Serialisation model.** The `jobs` row is the single per-job serialisation point. Per-job
 * transitions (abort/complete/error/release/checkpoint) take `SELECT ... FOR UPDATE` on it via
 * [findAndLockJobById] for the lifetime of the enclosing transaction, then compose the primitive
 * writes across the secondary tables.
 *
 * The bulk movers serialise on the same `jobs` lock, but differently. [acquirePendingJobs] uses
 * `FOR UPDATE OF j SKIP LOCKED` and acts only on the jobs it locked — a contended job is skipped
 * (it just stays queued), so acquire never blocks or deadlocks. [releaseJobsByWorkerIds] (the
 * dead-worker reaper) instead BLOCKS and drains *every* one of the worker's jobs: it locks them
 * single-table and jobs-first (`SELECT id FROM jobs WHERE id IN (…) FOR UPDATE`), then moves them
 * in a separate statement. It must leave no `job_acquired` row before the FK-constrained worker
 * delete (`fk_job_acquired_worker`), so it cannot skip; blocking is deadlock-free because
 * worker-lifecycle ops serialise on the `workers` row and the jobs-first order matches the per-job
 * transitions, and the single-table lock + separate read avoids the EvalPlanQual stale read.
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

    override suspend fun acquirePendingJobs(
        groupId: String,
        type: String,
        workerId: String,
        limit: Int,
    ): List<Job> =
        acquireOnClient(
            pool.activeClient(requireTransaction = true),
            groupId,
            type,
            workerId,
            limit,
            Instant.now().toEpochMilli(),
        )

    /**
     * Core SKIP-LOCKED claim backing [acquirePendingJobs]: runs the queue→acquired move on [client]
     * and assembles the [Job] views. No event insertion or transaction management — the caller owns
     * those.
     */
    private suspend fun acquireOnClient(
        client: SqlClient,
        groupId: String,
        type: String,
        workerId: String,
        limit: Int,
        now: Long,
    ): List<Job> {
        val rowSet =
            try {
                client
                    .preparedQuery(
                        $$"""
                        WITH ready AS (
                            -- The use case has already locked + liveness-checked the worker
                            -- (findAndLockWorkerById), so no worker join is needed here.
                            SELECT q.job_id
                            FROM job_queue q
                            WHERE q.group_id = $2 AND q.type = $3 AND q.available_at <= $4
                            ORDER BY q.enqueued_at ASC
                            LIMIT $5
                            FOR UPDATE SKIP LOCKED
                        ),
                        locked AS (
                            -- Lock the jobs rows too, skipping any a concurrent per-job transition
                            -- (abort/complete/error/release) holds via findAndLockJobById's FOR UPDATE. Only
                            -- jobs locked in BOTH tables are moved; SKIP LOCKED keeps acquire
                            -- non-blocking so it cannot deadlock against jobs-then-queue lockers.
                            SELECT j.id AS job_id
                            FROM jobs j
                            JOIN ready r ON r.job_id = j.id
                            FOR UPDATE OF j SKIP LOCKED
                        ),
                        deleted AS (
                            DELETE FROM job_queue q
                            USING locked l
                            WHERE q.job_id = l.job_id
                            RETURNING q.job_id, q.retries, q.enqueued_at
                        ),
                        inserted AS (
                            -- Data-modifying CTE: Postgres always runs it to completion even though
                            -- the final SELECT does not read from it.
                            INSERT INTO job_acquired (job_id, group_id, type, acquired_by_worker_id, acquired_at, retries)
                            SELECT job_id, $2, $3, $1, $4, retries FROM deleted
                            RETURNING job_id
                        )
                        -- A top-level ORDER BY is the only way to GUARANTEE the contract's
                        -- `enqueued_at ASC` batch order: RETURNING from the INSERT/DELETE above is
                        -- not ordered (its row order is plan-dependent, not the ready CTE's sort).
                        SELECT d.job_id, d.retries FROM deleted d ORDER BY d.enqueued_at ASC
                        """
                            .trimIndent()
                    )
                    .execute(Tuple.of(workerId, groupId, type, now, limit))
                    .coAwait()
            } catch (e: PgException) {
                if (e.constraint == CONSTRAINT_FK_JOB_ACQUIRED_WORKER)
                    throw WorkerNotFoundException(workerId)
                throw e
            }
        if (rowSet.rowCount() == 0) return emptyList()
        val acquiredRows = rowSet.map { it.getString("job_id") to it.getInteger("retries") }
        val ids = acquiredRows.map { it.first }
        // Fetch base `jobs` rows in a single round-trip and assemble Job views in memory. We just
        // inserted into job_acquired ourselves, so we already know the workerId/now/retries — only
        // the immutable base columns need a query.
        val baseRows = readBaseJobs(client, ids)
        return acquiredRows.map { (jobId, retries) ->
            val row = baseRows[jobId] ?: error("base jobs row missing for acquired $jobId")
            // Defensive tripwire: the three-table invariant forbids a terminal `jobs` row
            // coexisting with a `job_queue` entry. Test coverage enforces this for gateway-driven
            // writes, so this only fires if the invariant is broken by external means (manual SQL,
            // restored backup).
            check(row.getString("terminal_status") == null) {
                "three-table invariant violated at acquire: job $jobId has terminal_status=${row.getString("terminal_status")} but was in job_queue"
            }
            row.toAcquiredJob(
                workerId = workerId,
                acquiredAt = Instant.ofEpochMilli(now),
                retries = retries,
            )
        }
    }

    override suspend fun findJobById(id: String): Job? = readJob(pool.activeClient(), id)

    override suspend fun findAndLockJobById(id: String): Job? {
        val client = pool.activeClient(requireTransaction = true)
        // Take the jobs-row lock in its own single-table statement, then assemble the Job from a
        // SEPARATE read. Folding the lock into the assembled read (`... LEFT JOIN ... FOR UPDATE OF
        // j`) locks only the jobs row, and under READ COMMITTED a blocked-then-resumed statement
        // re-reads the locked jobs row at the latest version but evaluates the LEFT-JOINed
        // acquired/queue rows against its original snapshot — so the assembled status can be stale
        // (e.g. still ACQUIRED after a concurrent release moved the job to the queue), which drives
        // the caller to mutate the wrong table and breaks the three-table invariant. Reading in a
        // fresh statement after the lock is held sees the true committed state. Mirrors
        // H2JobGateway.findAndLockJobById; guarded by the abort+release concurrency stress test.
        val locked =
            client
                .preparedQuery($$"SELECT 1 FROM jobs WHERE id = $1 FOR UPDATE")
                .execute(Tuple.of(id))
                .coAwait()
                .size() > 0
        return if (!locked) null else readJob(client, id)
    }

    override suspend fun latestCheckpoint(jobId: String): Checkpoint? {
        val rowSet =
            pool
                .activeClient()
                .preparedQuery(
                    $$"SELECT $${CHECKPOINT_COLUMNS.list()} FROM checkpoints WHERE job_id = $1 ORDER BY order_key DESC LIMIT 1"
                )
                .execute(Tuple.of(jobId))
                .coAwait()
        return if (rowSet.rowCount() > 0) rowSet.first().toCheckpoint() else null
    }

    override suspend fun findCheckpointById(id: String): Checkpoint? {
        val rowSet =
            pool
                .activeClient()
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
                    .activeClient()
                    .preparedQuery(
                        $$"SELECT $${CHECKPOINT_COLUMNS.list()} FROM checkpoints WHERE job_id = $1 AND order_key > $2 ORDER BY order_key ASC LIMIT $3"
                    )
                    .execute(Tuple.of(jobId, afterOrderKey, limit))
                    .coAwait()
            } else {
                pool
                    .activeClient()
                    .preparedQuery(
                        $$"SELECT $${CHECKPOINT_COLUMNS.list()} FROM checkpoints WHERE job_id = $1 ORDER BY order_key ASC LIMIT $2"
                    )
                    .execute(Tuple.of(jobId, limit))
                    .coAwait()
            }
        return rowSet.map { it.toCheckpoint() }
    }

    override suspend fun insertJob(newJob: NewJob) {
        try {
            pool
                .activeClient()
                .preparedQuery(
                    $$"""
                    WITH ins_job AS (
                        INSERT INTO jobs (id, group_id, name, type, max_retries, input_data,
                                          output_data, created_at, terminal_status, terminal_at)
                        VALUES ($1, $2, $3, $4, $5, $6, NULL, $7, NULL, NULL)
                        RETURNING id
                    )
                    INSERT INTO job_queue (job_id, group_id, type, enqueued_at, retries, available_at)
                    SELECT $1, $2, $4, $7, 0, 0 FROM ins_job
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
                    )
                )
                .coAwait()
        } catch (e: PgException) {
            if (e.constraint == CONSTRAINT_JOBS_PK) throw DuplicateJobIdException(newJob.id, e)
            throw e
        }
    }

    override suspend fun insertEvents(events: List<JobEvent>) {
        if (events.isEmpty()) return
        pool
            .activeClient()
            .preparedQuery(
                $$"INSERT INTO job_events ($${JOB_EVENT_COLUMNS.list()}) VALUES ($${JOB_EVENT_COLUMNS.placeholders()})"
            )
            .executeBatch(events.map { it.toTuple() })
            .coAwait()
    }

    override suspend fun removeFromQueue(jobId: String) {
        pool
            .activeClient()
            .preparedQuery($$"DELETE FROM job_queue WHERE job_id = $1")
            .execute(Tuple.of(jobId))
            .coAwait()
    }

    override suspend fun removeFromAcquired(jobId: String) {
        pool
            .activeClient()
            .preparedQuery($$"DELETE FROM job_acquired WHERE job_id = $1")
            .execute(Tuple.of(jobId))
            .coAwait()
    }

    override suspend fun enqueue(
        jobId: String,
        groupId: String,
        type: String,
        enqueuedAt: Instant,
        retries: Int,
        availableAt: Long,
    ) {
        pool
            .activeClient()
            .preparedQuery(
                $$"INSERT INTO job_queue (job_id, group_id, type, enqueued_at, retries, available_at) VALUES ($1, $2, $3, $4, $5, $6)"
            )
            .execute(
                Tuple.of(jobId, groupId, type, enqueuedAt.toEpochMilli(), retries, availableAt)
            )
            .coAwait()
    }

    override suspend fun markTerminal(
        jobId: String,
        status: JobStatus,
        terminalAt: Instant,
        outputData: ByteArray?,
    ) {
        pool
            .activeClient()
            .preparedQuery(
                $$"UPDATE jobs SET terminal_status = $1, terminal_at = $2, output_data = $3 WHERE id = $4"
            )
            .execute(
                Tuple.of(
                    status.name,
                    terminalAt.toEpochMilli(),
                    outputData?.let { Buffer.buffer(it) },
                    jobId,
                )
            )
            .coAwait()
    }

    override suspend fun insertCheckpoint(checkpoint: Checkpoint) {
        try {
            pool
                .activeClient()
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
                        checkpoint.data?.let { Buffer.buffer(it) },
                    )
                )
                .coAwait()
        } catch (e: PgException) {
            if (e.constraint in CHECKPOINT_CONSTRAINTS)
                throw InvalidCheckpointOrderException(checkpoint.jobId, e)
            throw e
        }
    }

    override suspend fun resetAcquiredRetries(jobId: String) {
        pool
            .activeClient()
            .preparedQuery($$"UPDATE job_acquired SET retries = 0 WHERE job_id = $1")
            .execute(Tuple.of(jobId))
            .coAwait()
    }

    override suspend fun findEventsByJobId(jobId: String): List<JobEvent> {
        val rowSet =
            pool
                .activeClient()
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
    private suspend fun readBaseJobs(client: SqlClient, ids: List<String>): Map<String, Row> {
        if (ids.isEmpty()) return emptyMap()
        val rs =
            client
                .preparedQuery($$"SELECT $${JOB_COLUMNS.list()} FROM jobs WHERE id = ANY($1)")
                .execute(Tuple.of(ids.toTypedArray()))
                .coAwait()
        val map = HashMap<String, Row>(ids.size)
        for (row in rs) map[row.getString("id")] = row
        return map
    }

    override suspend fun releaseJobsByWorkerIds(
        workerIds: List<String>,
        updatedAt: Instant,
    ): List<String> =
        releaseOnClient(pool.activeClient(requireTransaction = true), workerIds, updatedAt)

    /**
     * Core bulk release backing [releaseJobsByWorkerIds]: atomically moves every `job_acquired` row
     * owned by [workerIds] back into `job_queue` (with `available_at = 0`) on [client] and returns
     * the released job ids. No event insertion or transaction management.
     */
    private suspend fun releaseOnClient(
        client: SqlClient,
        workerIds: List<String>,
        updatedAt: Instant,
    ): List<String> {
        if (workerIds.isEmpty()) return emptyList()
        // 1) Lock the jobs rows of these workers — single-table, blocking, jobs-first. Waits out
        // any
        //    in-flight per-job transition so we DRAIN every job; skipping a locked job would leave
        //    its job_acquired row and make the worker delete violate fk_job_acquired_worker.
        //    Single-table (not the join + FOR UPDATE OF j) avoids the EvalPlanQual stale read, and
        //    jobs-first matches the per-job transitions' order so there is no deadlock.
        val lockPlaceholders = workerIds.indices.joinToString(",") { "$${it + 1}" }
        val lockTuple = Tuple.tuple()
        workerIds.forEach { lockTuple.addString(it) }
        client
            .preparedQuery(
                "SELECT id FROM jobs WHERE id IN (SELECT job_id FROM job_acquired WHERE acquired_by_worker_id IN ($lockPlaceholders)) FOR UPDATE"
            )
            .execute(lockTuple)
            .coAwait()
        // 2) Drain: move every remaining acquired row to the queue (available_at = 0). The deadline
        //    is deliberately not preserved — the SDK's replay path re-derives it from the
        // checkpoint
        //    chain. The jobs are locked, so the state is stable and this read is fresh (not stale).
        val movePlaceholders = workerIds.indices.joinToString(",") { "$${it + 2}" }
        val moveTuple = Tuple.tuple()
        moveTuple.addLong(updatedAt.toEpochMilli())
        workerIds.forEach { moveTuple.addString(it) }
        val rs =
            client
                .preparedQuery(
                    """
                    WITH released AS (
                        DELETE FROM job_acquired
                        WHERE acquired_by_worker_id IN ($movePlaceholders)
                        RETURNING job_id, group_id, type, retries
                    )
                    INSERT INTO job_queue (job_id, group_id, type, enqueued_at, retries, available_at)
                    SELECT job_id, group_id, type, ${'$'}1, retries, 0 FROM released
                    RETURNING job_id
                    """
                        .trimIndent()
                )
                .execute(moveTuple)
                .coAwait()
        return rs.map { it.getString("job_id") }
    }

    // --- Read helpers ---

    /**
     * Assemble a denormalised [Job] from the three tables. Returns `null` if `jobs(id)` does not
     * exist; otherwise inspects `terminal_status`, then `job_acquired`, then `job_queue` to derive
     * the live status and per-state fields.
     */
    private suspend fun readJob(client: SqlClient, jobId: String): Job? {
        // Pure assembled read with no lock. Callers that need to mutate the job take the jobs-row
        // lock first via `findAndLockJobById` (a single-table `SELECT 1 ... FOR UPDATE`), so this
        // read runs in
        // a fresh statement that observes the true committed state.
        val rs =
            client
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
