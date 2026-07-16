package org.zeplinko.logplay.server.job.adapters

import io.vertx.kotlin.coroutines.coAwait
import io.vertx.pgclient.PgException
import io.vertx.sqlclient.Pool
import io.vertx.sqlclient.Row
import io.vertx.sqlclient.Tuple
import java.time.Instant
import org.zeplinko.logplay.server.core.worker.Worker
import org.zeplinko.logplay.server.core.worker.WorkerAlreadyRegisteredException
import org.zeplinko.logplay.server.core.worker.WorkerGateway

class PostgresWorkerGateway(private val pool: Pool) : WorkerGateway {

    companion object {
        private const val CONSTRAINT_WORKERS_PK = "workers_pkey"
    }

    override suspend fun insertWorker(worker: Worker): Worker {
        try {
            pool
                .activeClient()
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
                .activeClient()
                .preparedQuery(
                    $$"UPDATE workers SET last_heartbeat_at = $1 WHERE id = $2 AND ($3 - last_heartbeat_at) <= session_timeout RETURNING *"
                )
                .execute(Tuple.of(heartbeatAt.toEpochMilli(), workerId, heartbeatAt.toEpochMilli()))
                .coAwait()
        return if (rowSet.rowCount() > 0) rowSet.first().toWorker() else null
    }

    override suspend fun findAndLockWorkerById(id: String): Worker? {
        val rowSet =
            pool
                .activeClient(requireTransaction = true)
                .preparedQuery($$"SELECT * FROM workers WHERE id = $1 FOR UPDATE")
                .execute(Tuple.of(id))
                .coAwait()
        return if (rowSet.rowCount() > 0) rowSet.first().toWorker() else null
    }

    override suspend fun deleteWorker(id: String) {
        pool
            .activeClient()
            .preparedQuery($$"DELETE FROM workers WHERE id = $1")
            .execute(Tuple.of(id))
            .coAwait()
    }

    override suspend fun deleteWorkers(ids: List<String>) {
        if (ids.isEmpty()) return
        val placeholders = ids.indices.joinToString(",") { $$"$$${it + 1}" }
        val tuple = Tuple.tuple()
        ids.forEach { tuple.addString(it) }
        pool
            .activeClient()
            .preparedQuery("DELETE FROM workers WHERE id IN ($placeholders)")
            .execute(tuple)
            .coAwait()
    }

    override suspend fun findAndLockDeadWorkers(now: Instant): List<Worker> {
        val rowSet =
            pool
                .activeClient(requireTransaction = true)
                .preparedQuery(
                    $$"SELECT * FROM workers WHERE ($1 - last_heartbeat_at) > session_timeout FOR UPDATE SKIP LOCKED"
                )
                .execute(Tuple.of(now.toEpochMilli()))
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
        )
}
