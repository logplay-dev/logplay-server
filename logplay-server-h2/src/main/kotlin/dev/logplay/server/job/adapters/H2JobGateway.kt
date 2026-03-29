package dev.logplay.server.job.adapters

import dev.logplay.server.core.job.*
import java.sql.ResultSet
import java.sql.SQLException
import java.time.Instant
import javax.sql.DataSource
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

class H2JobGateway(private val dataSource: DataSource) : JobGateway {

    init {
        dataSource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(
                    """
                    CREATE TABLE IF NOT EXISTS jobs (
                        id VARCHAR(36) PRIMARY KEY,
                        name VARCHAR(255) NOT NULL,
                        type VARCHAR(255) NOT NULL,
                        status VARCHAR(50) NOT NULL,
                        retries INT NOT NULL DEFAULT 0,
                        created_at BIGINT NOT NULL,
                        updated_at BIGINT NOT NULL,
                        version BIGINT NOT NULL DEFAULT 0
                    )
                    """
                        .trimIndent()
                )
            }
            conn.createStatement().use { stmt ->
                stmt.execute(
                    """
                    CREATE TABLE IF NOT EXISTS checkpoints (
                        id VARCHAR(36) PRIMARY KEY,
                        job_id VARCHAR(36) NOT NULL,
                        class_type VARCHAR(512) NOT NULL,
                        description VARCHAR(1024),
                        created_at BIGINT NOT NULL,
                        data BINARY VARYING NOT NULL
                    )
                    """
                        .trimIndent()
                )
            }
        }
    }

    override suspend fun insertJob(job: Job): Job =
        withContext(Dispatchers.IO) {
            try {
                dataSource.connection.use { conn ->
                    conn
                        .prepareStatement(
                            "INSERT INTO jobs (id, name, type, status, retries, created_at, updated_at, version) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
                        )
                        .use { stmt ->
                            stmt.setString(1, job.id)
                            stmt.setString(2, job.name)
                            stmt.setString(3, job.type)
                            stmt.setString(4, job.status.name)
                            stmt.setInt(5, job.retries)
                            stmt.setLong(6, job.createdAt.toEpochMilli())
                            stmt.setLong(7, job.updatedAt.toEpochMilli())
                            stmt.setLong(8, job.version)
                            stmt.executeUpdate()
                        }
                }
            } catch (e: SQLException) {
                if (e.sqlState?.startsWith("23") == true) throw JobAlreadyExistsException(e)
                throw e
            }
            job
        }

    override suspend fun updateJob(job: Job): Job =
        withContext(Dispatchers.IO) {
            dataSource.connection.use { conn ->
                conn
                    .prepareStatement(
                        "UPDATE jobs SET name = ?, type = ?, status = ?, retries = ?, updated_at = ?, version = ? WHERE id = ?"
                    )
                    .use { stmt ->
                        stmt.setString(1, job.name)
                        stmt.setString(2, job.type)
                        stmt.setString(3, job.status.name)
                        stmt.setInt(4, job.retries)
                        stmt.setLong(5, job.updatedAt.toEpochMilli())
                        stmt.setLong(6, job.version)
                        stmt.setString(7, job.id)
                        stmt.executeUpdate()
                    }
            }
            job
        }

    override suspend fun getPendingJobs(limit: Int): List<Job> =
        withContext(Dispatchers.IO) {
            dataSource.connection.use { conn ->
                conn
                    .prepareStatement(
                        "SELECT * FROM jobs WHERE status = ? ORDER BY created_at ASC LIMIT ?"
                    )
                    .use { stmt ->
                        stmt.setString(1, JobStatus.PENDING.name)
                        stmt.setInt(2, limit)
                        stmt.executeQuery().use { rs -> rs.mapToJobs() }
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

    override suspend fun saveCheckpoint(checkpoint: Checkpoint): Checkpoint =
        withContext(Dispatchers.IO) {
            dataSource.connection.use { conn ->
                conn
                    .prepareStatement(
                        "INSERT INTO checkpoints (id, job_id, class_type, description, created_at, data) VALUES (?, ?, ?, ?, ?, ?)"
                    )
                    .use { stmt ->
                        stmt.setString(1, checkpoint.id)
                        stmt.setString(2, checkpoint.jobId)
                        stmt.setString(3, checkpoint.classType)
                        stmt.setString(4, checkpoint.description)
                        stmt.setLong(5, checkpoint.createdAt.toEpochMilli())
                        stmt.setBytes(6, checkpoint.data)
                        stmt.executeUpdate()
                    }
            }
            checkpoint
        }

    private fun ResultSet.mapToJobs(): List<Job> {
        val jobs = mutableListOf<Job>()
        while (next()) jobs.add(toJob())
        return jobs
    }

    private fun ResultSet.toJob(): Job =
        Job(
            id = getString("id"),
            name = getString("name"),
            type = getString("type"),
            status = JobStatus.valueOf(getString("status")),
            retries = getInt("retries"),
            createdAt = Instant.ofEpochMilli(getLong("created_at")),
            updatedAt = Instant.ofEpochMilli(getLong("updated_at")),
            version = getLong("version"),
        )
}
