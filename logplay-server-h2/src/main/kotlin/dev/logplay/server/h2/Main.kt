package dev.logplay.server.h2

import dev.logplay.server.MainVerticle
import dev.logplay.server.job.adapters.H2JobGateway
import dev.logplay.server.job.adapters.H2WorkerGateway
import io.vertx.core.Vertx
import java.lang.invoke.MethodHandles
import org.flywaydb.core.Flyway
import org.h2.jdbcx.JdbcConnectionPool
import org.slf4j.LoggerFactory

private val logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

fun main() {
    val dataSource =
        JdbcConnectionPool.create("jdbc:h2:file:./logplay-data;AUTO_SERVER=TRUE", "sa", "")

    Flyway.configure().dataSource(dataSource).locations("classpath:db/migration").load().migrate()

    val vertx = Vertx.vertx()
    vertx
        .deployVerticle(MainVerticle(H2JobGateway(dataSource), H2WorkerGateway(dataSource)))
        .onFailure { error ->
            logger.error("Failed to start server", error)
            vertx.close()
        }
}
