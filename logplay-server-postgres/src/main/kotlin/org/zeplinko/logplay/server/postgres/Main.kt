package org.zeplinko.logplay.server.postgres

import io.vertx.core.Vertx
import io.vertx.pgclient.PgBuilder
import io.vertx.pgclient.PgConnectOptions
import io.vertx.sqlclient.PoolOptions
import java.lang.invoke.MethodHandles
import org.flywaydb.core.Flyway
import org.slf4j.LoggerFactory
import org.zeplinko.logplay.server.MainVerticle
import org.zeplinko.logplay.server.job.adapters.PostgresJobGateway
import org.zeplinko.logplay.server.job.adapters.PostgresUnitOfWork
import org.zeplinko.logplay.server.job.adapters.PostgresWorkerGateway

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
        .defaultSchema("public")
        .load()
        .migrate()

    val vertx = Vertx.vertx()
    // Pool/driver tuning is hardcoded for now. When server configuration is standardised
    // (env vars / config file), these knobs should be exposed there with the values below
    // as defaults.
    val connectOptions =
        PgConnectOptions()
            .setHost(dbHost)
            .setPort(dbPort)
            .setDatabase(dbName)
            .setUser(dbUser)
            .setPassword(dbPassword)
            .setCachePreparedStatements(true)
            .setPreparedStatementCacheMaxSize(256)
            .setPipeliningLimit(256)
    val pool =
        PgBuilder.pool()
            .with(PoolOptions().setMaxSize(32))
            .connectingTo(connectOptions)
            .using(vertx)
            .build()
    vertx
        .deployVerticle(
            MainVerticle(
                PostgresJobGateway(pool),
                PostgresWorkerGateway(pool),
                PostgresUnitOfWork(pool),
            )
        )
        .onFailure { error ->
            logger.error("Failed to start server", error)
            vertx.close()
        }
}
