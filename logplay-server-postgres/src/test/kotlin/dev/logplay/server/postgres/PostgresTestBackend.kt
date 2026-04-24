package dev.logplay.server.postgres

import dev.logplay.server.core.job.JobGateway
import dev.logplay.server.core.worker.WorkerGateway
import dev.logplay.server.job.adapters.PostgresJobGateway
import dev.logplay.server.job.adapters.PostgresWorkerGateway
import dev.logplay.server.test.IntegrationTestBackend
import io.vertx.core.Vertx
import io.vertx.pgclient.PgBuilder
import io.vertx.pgclient.PgConnectOptions
import io.vertx.sqlclient.Pool
import io.vertx.sqlclient.PoolOptions
import java.sql.Connection
import java.sql.DriverManager
import org.flywaydb.core.Flyway
import org.testcontainers.containers.PostgreSQLContainer

class PostgresTestBackend(private val postgres: PostgreSQLContainer<*>) : IntegrationTestBackend {

    private lateinit var pool: Pool

    override fun initDatabase() {
        Flyway.configure()
            .dataSource(postgres.jdbcUrl, postgres.username, postgres.password)
            .locations("classpath:db/migration")
            .load()
            .migrate()
    }

    override fun createGateways(vertx: Vertx): Pair<JobGateway, WorkerGateway> {
        val connectOptions =
            PgConnectOptions()
                .setHost(postgres.host)
                .setPort(postgres.firstMappedPort)
                .setDatabase(postgres.databaseName)
                .setUser(postgres.username)
                .setPassword(postgres.password)
        pool =
            PgBuilder.pool()
                .with(PoolOptions().setMaxSize(5))
                .connectingTo(connectOptions)
                .using(vertx)
                .build()
        return PostgresJobGateway(pool) to PostgresWorkerGateway(pool)
    }

    override fun getJdbcConnection(): Connection =
        DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password)

    override fun shutdown() {
        pool.close()
    }
}
