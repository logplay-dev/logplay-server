package org.zeplinko.logplay.server.h2

import io.vertx.core.Vertx
import java.lang.invoke.MethodHandles
import org.flywaydb.core.Flyway
import org.h2.jdbcx.JdbcConnectionPool
import org.slf4j.LoggerFactory
import org.zeplinko.logplay.server.MainVerticle
import org.zeplinko.logplay.server.job.adapters.H2JobGateway
import org.zeplinko.logplay.server.job.adapters.H2WorkerGateway

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
