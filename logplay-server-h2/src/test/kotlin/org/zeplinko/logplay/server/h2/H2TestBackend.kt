package org.zeplinko.logplay.server.h2

import io.vertx.core.Vertx
import java.sql.Connection
import java.util.UUID
import javax.sql.DataSource
import org.flywaydb.core.Flyway
import org.h2.jdbcx.JdbcConnectionPool
import org.zeplinko.logplay.server.core.job.JobGateway
import org.zeplinko.logplay.server.core.worker.WorkerGateway
import org.zeplinko.logplay.server.job.adapters.H2JobGateway
import org.zeplinko.logplay.server.job.adapters.H2WorkerGateway
import org.zeplinko.logplay.server.test.IntegrationTestBackend

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
