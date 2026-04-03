package dev.logplay.server.job.adapters

import dev.logplay.server.core.worker.Worker
import dev.logplay.server.core.worker.WorkerAlreadyRegisteredException
import dev.logplay.server.core.worker.WorkerGateway
import java.sql.ResultSet
import java.sql.SQLException
import java.time.Instant
import javax.sql.DataSource
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

class H2WorkerGateway(private val dataSource: DataSource) : WorkerGateway {

    companion object {
        private const val CONSTRAINT_WORKERS_PK = "PRIMARY KEY ON PUBLIC.WORKERS"
    }

    override suspend fun insertWorker(worker: Worker): Worker =
        withContext(Dispatchers.IO) {
            try {
                dataSource.connection.use { conn ->
                    conn
                        .prepareStatement(
                            "INSERT INTO workers (id, heartbeat_timeout, session_timeout, last_heartbeat_at, registered_at) VALUES (?, ?, ?, ?, ?)"
                        )
                        .use { stmt ->
                            stmt.setString(1, worker.id)
                            stmt.setLong(2, worker.heartbeatTimeout)
                            stmt.setLong(3, worker.sessionTimeout)
                            stmt.setLong(4, worker.lastHeartbeatAt.toEpochMilli())
                            stmt.setLong(5, worker.registeredAt.toEpochMilli())
                            stmt.executeUpdate()
                        }
                }
            } catch (e: SQLException) {
                if (e.message?.contains(CONSTRAINT_WORKERS_PK, ignoreCase = true) == true)
                    throw WorkerAlreadyRegisteredException(worker.id)
                throw e
            }
            worker
        }

    override suspend fun updateWorkerHeartbeat(workerId: String, heartbeatAt: Instant): Worker? =
        withContext(Dispatchers.IO) {
            dataSource.connection.use { conn ->
                val rowsUpdated =
                    conn
                        .prepareStatement(
                            "UPDATE workers SET last_heartbeat_at = ? WHERE id = ? AND condemned = FALSE"
                        )
                        .use { stmt ->
                            stmt.setLong(1, heartbeatAt.toEpochMilli())
                            stmt.setString(2, workerId)
                            stmt.executeUpdate()
                        }
                if (rowsUpdated == 0) {
                    null
                } else {
                    conn.prepareStatement("SELECT * FROM workers WHERE id = ?").use { stmt ->
                        stmt.setString(1, workerId)
                        stmt.executeQuery().use { rs -> if (rs.next()) rs.toWorker() else null }
                    }
                }
            }
        }

    override suspend fun findWorkerById(id: String): Worker? =
        withContext(Dispatchers.IO) {
            dataSource.connection.use { conn ->
                conn.prepareStatement("SELECT * FROM workers WHERE id = ?").use { stmt ->
                    stmt.setString(1, id)
                    stmt.executeQuery().use { rs -> if (rs.next()) rs.toWorker() else null }
                }
            }
        }

    override suspend fun deleteWorker(id: String) =
        withContext(Dispatchers.IO) {
            dataSource.connection.use { conn ->
                conn.prepareStatement("DELETE FROM workers WHERE id = ?").use { stmt ->
                    stmt.setString(1, id)
                    stmt.executeUpdate()
                }
            }
            Unit
        }

    override suspend fun condemnWorker(id: String) =
        withContext(Dispatchers.IO) {
            dataSource.connection.use { conn ->
                conn.prepareStatement("UPDATE workers SET condemned = TRUE WHERE id = ?").use { stmt
                    ->
                    stmt.setString(1, id)
                    stmt.executeUpdate()
                }
            }
            Unit
        }

    override suspend fun condemnDeadWorkers(now: Instant): Int =
        withContext(Dispatchers.IO) {
            dataSource.connection.use { conn ->
                conn
                    .prepareStatement(
                        "UPDATE workers SET condemned = TRUE WHERE condemned = FALSE AND (? - last_heartbeat_at) > session_timeout"
                    )
                    .use { stmt ->
                        stmt.setLong(1, now.toEpochMilli())
                        stmt.executeUpdate()
                    }
            }
        }

    override suspend fun deleteWorkers(ids: List<String>) =
        withContext(Dispatchers.IO) {
            if (ids.isEmpty()) return@withContext
            dataSource.connection.use { conn ->
                val placeholders = ids.joinToString(",") { "?" }
                conn.prepareStatement("DELETE FROM workers WHERE id IN ($placeholders)").use { stmt
                    ->
                    ids.forEachIndexed { i, id -> stmt.setString(i + 1, id) }
                    stmt.executeUpdate()
                }
            }
            Unit
        }

    override suspend fun findCondemnedWorkers(now: Instant, condemnPeriodMs: Long): List<Worker> =
        withContext(Dispatchers.IO) {
            dataSource.connection.use { conn ->
                conn
                    .prepareStatement(
                        "SELECT * FROM workers WHERE condemned = TRUE AND (? - last_heartbeat_at) > (session_timeout + ?)"
                    )
                    .use { stmt ->
                        stmt.setLong(1, now.toEpochMilli())
                        stmt.setLong(2, condemnPeriodMs)
                        stmt.executeQuery().use { rs ->
                            val workers = mutableListOf<Worker>()
                            while (rs.next()) workers.add(rs.toWorker())
                            workers
                        }
                    }
            }
        }

    private fun ResultSet.toWorker(): Worker =
        Worker(
            id = getString("id"),
            heartbeatTimeout = getLong("heartbeat_timeout"),
            sessionTimeout = getLong("session_timeout"),
            lastHeartbeatAt = Instant.ofEpochMilli(getLong("last_heartbeat_at")),
            registeredAt = Instant.ofEpochMilli(getLong("registered_at")),
            condemned = getBoolean("condemned"),
        )
}
