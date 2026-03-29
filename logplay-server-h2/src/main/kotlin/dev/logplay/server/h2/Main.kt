package dev.logplay.server.h2

import dev.logplay.server.MainVerticle
import dev.logplay.server.job.adapters.H2JobGateway
import io.vertx.core.Vertx
import org.h2.jdbcx.JdbcConnectionPool

fun main() {
    val dataSource =
        JdbcConnectionPool.create("jdbc:h2:file:./logplay-data;AUTO_SERVER=TRUE", "sa", "")
    val vertx = Vertx.vertx()
    vertx.deployVerticle(MainVerticle(H2JobGateway(dataSource))).onFailure { error ->
        System.err.println("Failed to start server: ${error.message}")
        vertx.close()
    }
}
