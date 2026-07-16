package org.zeplinko.logplay.server.job.adapters

import java.sql.ResultSet
import java.sql.SQLException
import java.time.Instant
import javax.sql.DataSource
import org.zeplinko.logplay.server.core.worker.Worker
import org.zeplinko.logplay.server.core.worker.WorkerAlreadyRegisteredException
import org.zeplinko.logplay.server.core.worker.WorkerGateway

class H2WorkerGateway(private val dataSource: DataSource) : WorkerGateway {

    companion object {
        // H2 auto-numbers the PK constraint name (e.g. PRIMARY_KEY_8); we match on the stable
        // column reference instead, mirroring how H2JobGateway identifies its PK violations.
        private const val CONSTRAINT_WORKERS_PK = "PUBLIC.WORKERS(ID)"
    }

    override suspend fun insertWorker(worker: Worker): Worker =
        dataSource.withConn { conn ->
            try {
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
            } catch (e: SQLException) {
                if (e.message?.contains(CONSTRAINT_WORKERS_PK, ignoreCase = true) == true)
                    throw WorkerAlreadyRegisteredException(worker.id)
                throw e
            }
            worker
        }

    override suspend fun updateWorkerHeartbeat(workerId: String, heartbeatAt: Instant): Worker? =
        dataSource.withConn { conn ->
            val rowsUpdated =
                conn
                    .prepareStatement(
                        "UPDATE workers SET last_heartbeat_at = ? WHERE id = ? AND (? - last_heartbeat_at) <= session_timeout"
                    )
                    .use { stmt ->
                        stmt.setLong(1, heartbeatAt.toEpochMilli())
                        stmt.setString(2, workerId)
                        stmt.setLong(3, heartbeatAt.toEpochMilli())
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

    override suspend fun findAndLockWorkerById(id: String): Worker? =
        dataSource.withConn(requireTransaction = true) { conn ->
            conn.prepareStatement("SELECT * FROM workers WHERE id = ? FOR UPDATE").use { stmt ->
                stmt.setString(1, id)
                stmt.executeQuery().use { rs -> if (rs.next()) rs.toWorker() else null }
            }
        }

    override suspend fun deleteWorker(id: String) {
        dataSource.withConn { conn ->
            conn.prepareStatement("DELETE FROM workers WHERE id = ?").use { stmt ->
                stmt.setString(1, id)
                stmt.executeUpdate()
            }
        }
    }

    override suspend fun deleteWorkers(ids: List<String>) {
        if (ids.isEmpty()) return
        dataSource.withConn { conn ->
            val placeholders = ids.joinToString(",") { "?" }
            conn.prepareStatement("DELETE FROM workers WHERE id IN ($placeholders)").use { stmt ->
                ids.forEachIndexed { i, id -> stmt.setString(i + 1, id) }
                stmt.executeUpdate()
            }
        }
    }

    override suspend fun findAndLockDeadWorkers(now: Instant): List<Worker> =
        dataSource.withConn(requireTransaction = true) { conn ->
            conn
                .prepareStatement(
                    "SELECT * FROM workers WHERE (? - last_heartbeat_at) > session_timeout FOR UPDATE SKIP LOCKED"
                )
                .use { stmt ->
                    stmt.setLong(1, now.toEpochMilli())
                    stmt.executeQuery().use { rs ->
                        val workers = mutableListOf<Worker>()
                        while (rs.next()) workers.add(rs.toWorker())
                        workers
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
        )
}
