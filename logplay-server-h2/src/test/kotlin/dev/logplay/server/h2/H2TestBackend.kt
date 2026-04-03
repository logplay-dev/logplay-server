package dev.logplay.server.h2

import dev.logplay.server.core.job.JobGateway
import dev.logplay.server.core.worker.WorkerGateway
import dev.logplay.server.job.adapters.H2JobGateway
import dev.logplay.server.job.adapters.H2WorkerGateway
import dev.logplay.server.test.IntegrationTestBackend
import io.vertx.core.Vertx
import java.sql.Connection
import java.util.UUID
import javax.sql.DataSource
import org.flywaydb.core.Flyway
import org.h2.jdbcx.JdbcConnectionPool

class H2TestBackend : IntegrationTestBackend {

    private lateinit var dataSource: DataSource

    override fun initDatabase() {
        val dbName = "test_${UUID.randomUUID().toString().replace("-", "").take(8)}"
        dataSource = JdbcConnectionPool.create("jdbc:h2:mem:$dbName;DB_CLOSE_DELAY=-1", "sa", "")
        Flyway.configure()
            .dataSource(dataSource)
            .locations("classpath:db/migration")
            .load()
            .migrate()
    }

    override fun createGateways(vertx: Vertx): Pair<JobGateway, WorkerGateway> {
        return H2JobGateway(dataSource) to H2WorkerGateway(dataSource)
    }

    override fun getJdbcConnection(): Connection = dataSource.connection

    override fun shutdown() {
        (dataSource as? JdbcConnectionPool)?.dispose()
    }
}
