package org.zeplinko.logplay.server.postgres

import org.zeplinko.logplay.server.config.Env

/** Postgres connection + pool/driver tuning. */
data class PostgresDbConfig(
    val host: String = "localhost",
    val port: Int = 5432,
    val database: String = "logplay",
    val user: String = "logplay",
    val password: String = "",
    val poolMaxSize: Int = 32,
    val cachePreparedStatements: Boolean = true,
    val preparedStatementCacheMaxSize: Int = 256,
    val pipeliningLimit: Int = 256,
) {
    init {
        require(poolMaxSize > 0) { "poolMaxSize must be > 0, was $poolMaxSize" }
    }

    val jdbcUrl: String
        get() = "jdbc:postgresql://$host:$port/$database"

    companion object {
        fun fromEnv(env: Env = Env()): PostgresDbConfig =
            PostgresDbConfig(
                host = env.string("DB_HOST", "localhost"),
                port = env.int("DB_PORT", 5432),
                database = env.string("DB_NAME", "logplay"),
                user = env.string("DB_USER", "logplay"),
                password = env.string("DB_PASSWORD", ""),
                poolMaxSize = env.int("DB_POOL_MAX_SIZE", 32),
                cachePreparedStatements = env.boolean("DB_CACHE_PREPARED_STATEMENTS", true),
                preparedStatementCacheMaxSize =
                    env.int("DB_PREPARED_STATEMENT_CACHE_MAX_SIZE", 256),
                pipeliningLimit = env.int("DB_PIPELINING_LIMIT", 256),
            )
    }
}
