package dev.logplay.server.postgres

import dev.logplay.server.MainVerticle
import dev.logplay.server.job.adapters.PostgresJobGateway
import dev.logplay.server.job.adapters.PostgresWorkerGateway
import io.vertx.core.Vertx
import io.vertx.pgclient.PgBuilder
import io.vertx.pgclient.PgConnectOptions
import io.vertx.sqlclient.PoolOptions
import java.lang.invoke.MethodHandles
import org.flywaydb.core.Flyway
import org.slf4j.LoggerFactory

private val logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

fun main() {
    val dbHost = System.getenv("DB_HOST") ?: "localhost"
    val dbPort = System.getenv("DB_PORT")?.toInt() ?: 5432
    val dbName = System.getenv("DB_NAME") ?: "logplay"
    val dbUser = System.getenv("DB_USER") ?: "logplay"
    val dbPassword = System.getenv("DB_PASSWORD") ?: ""

    Flyway.configure()
        .dataSource("jdbc:postgresql://$dbHost:$dbPort/$dbName", dbUser, dbPassword)
        .locations("classpath:db/migration")
        .load()
        .migrate()

    val vertx = Vertx.vertx()
    val connectOptions =
        PgConnectOptions()
            .setHost(dbHost)
            .setPort(dbPort)
            .setDatabase(dbName)
            .setUser(dbUser)
            .setPassword(dbPassword)
    val pool =
        PgBuilder.pool()
            .with(PoolOptions().setMaxSize(10))
            .connectingTo(connectOptions)
            .using(vertx)
            .build()
    vertx
        .deployVerticle(MainVerticle(PostgresJobGateway(pool), PostgresWorkerGateway(pool)))
        .onFailure { error ->
            logger.error("Failed to start server", error)
            vertx.close()
        }
}
