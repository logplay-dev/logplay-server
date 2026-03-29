package dev.logplay.server.postgres

import dev.logplay.server.MainVerticle
import dev.logplay.server.job.adapters.PostgresJobGateway
import io.vertx.core.Vertx
import io.vertx.pgclient.PgBuilder
import io.vertx.pgclient.PgConnectOptions
import io.vertx.sqlclient.PoolOptions

fun main() {
    val vertx = Vertx.vertx()
    val connectOptions =
        PgConnectOptions()
            .setHost(System.getenv("DB_HOST") ?: "localhost")
            .setPort(System.getenv("DB_PORT")?.toInt() ?: 5432)
            .setDatabase(System.getenv("DB_NAME") ?: "logplay")
            .setUser(System.getenv("DB_USER") ?: "logplay")
            .setPassword(System.getenv("DB_PASSWORD") ?: "")
    val pool =
        PgBuilder.pool()
            .with(PoolOptions().setMaxSize(10))
            .connectingTo(connectOptions)
            .using(vertx)
            .build()
    vertx.deployVerticle(MainVerticle(PostgresJobGateway(pool))).onFailure { error ->
        System.err.println("Failed to start server: ${error.message}")
        vertx.close()
    }
}
