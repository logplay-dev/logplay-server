package dev.logplay.server.job.adapters

import dev.logplay.server.core.worker.Worker
import dev.logplay.server.core.worker.WorkerAlreadyRegisteredException
import dev.logplay.server.core.worker.WorkerGateway
import io.vertx.kotlin.coroutines.coAwait
import io.vertx.pgclient.PgException
import io.vertx.sqlclient.Pool
import io.vertx.sqlclient.Row
import io.vertx.sqlclient.Tuple
import java.time.Instant

class PostgresWorkerGateway(private val pool: Pool) : WorkerGateway {

    companion object {
        private const val CONSTRAINT_WORKERS_PK = "workers_pkey"
    }

    override suspend fun insertWorker(worker: Worker): Worker {
        try {
            pool
                .preparedQuery(
                    $$"INSERT INTO workers (id, heartbeat_timeout, session_timeout, last_heartbeat_at, registered_at) VALUES ($1, $2, $3, $4, $5)"
                )
                .execute(
                    Tuple.of(
                        worker.id,
                        worker.heartbeatTimeout,
                        worker.sessionTimeout,
                        worker.lastHeartbeatAt.toEpochMilli(),
                        worker.registeredAt.toEpochMilli(),
                    )
                )
                .coAwait()
        } catch (e: PgException) {
            if (e.constraint == CONSTRAINT_WORKERS_PK)
                throw WorkerAlreadyRegisteredException(worker.id)
            throw e
        }
        return worker
    }

    override suspend fun updateWorkerHeartbeat(workerId: String, heartbeatAt: Instant): Worker? {
        val rowSet =
            pool
                .preparedQuery(
                    $$"UPDATE workers SET last_heartbeat_at = $1 WHERE id = $2 AND condemned = FALSE RETURNING *"
                )
                .execute(Tuple.of(heartbeatAt.toEpochMilli(), workerId))
                .coAwait()
        return if (rowSet.rowCount() > 0) rowSet.first().toWorker() else null
    }

    override suspend fun findWorkerById(id: String): Worker? {
        val rowSet =
            pool
                .preparedQuery($$"SELECT * FROM workers WHERE id = $1")
                .execute(Tuple.of(id))
                .coAwait()
        return if (rowSet.rowCount() > 0) rowSet.first().toWorker() else null
    }

    override suspend fun deleteWorker(id: String) {
        pool.preparedQuery($$"DELETE FROM workers WHERE id = $1").execute(Tuple.of(id)).coAwait()
    }

    override suspend fun condemnWorker(id: String) {
        pool
            .preparedQuery($$"UPDATE workers SET condemned = TRUE WHERE id = $1")
            .execute(Tuple.of(id))
            .coAwait()
    }

    override suspend fun condemnDeadWorkers(now: Instant): Int {
        val rowSet =
            pool
                .preparedQuery(
                    $$"UPDATE workers SET condemned = TRUE WHERE condemned = FALSE AND ($1 - last_heartbeat_at) > session_timeout"
                )
                .execute(Tuple.of(now.toEpochMilli()))
                .coAwait()
        return rowSet.rowCount()
    }

    override suspend fun deleteWorkers(ids: List<String>) {
        if (ids.isEmpty()) return
        val placeholders = ids.indices.joinToString(",") { $$"$$${it + 1}" }
        val tuple = Tuple.tuple()
        ids.forEach { tuple.addString(it) }
        pool
            .preparedQuery("DELETE FROM workers WHERE id IN ($placeholders)")
            .execute(tuple)
            .coAwait()
    }

    override suspend fun findCondemnedWorkers(now: Instant, condemnPeriodMs: Long): List<Worker> {
        val rowSet =
            pool
                .preparedQuery(
                    $$"SELECT * FROM workers WHERE condemned = TRUE AND ($1 - last_heartbeat_at) > (session_timeout + $2)"
                )
                .execute(Tuple.of(now.toEpochMilli(), condemnPeriodMs))
                .coAwait()
        return rowSet.map { it.toWorker() }
    }

    private fun Row.toWorker(): Worker =
        Worker(
            id = getString("id"),
            heartbeatTimeout = getLong("heartbeat_timeout"),
            sessionTimeout = getLong("session_timeout"),
            lastHeartbeatAt = Instant.ofEpochMilli(getLong("last_heartbeat_at")),
            registeredAt = Instant.ofEpochMilli(getLong("registered_at")),
            condemned = getBoolean("condemned"),
        )
}
