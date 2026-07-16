package org.zeplinko.logplay.server.h2

import org.zeplinko.logplay.server.config.Env
import org.zeplinko.logplay.server.job.adapters.H2UnitOfWork

/** H2 connection + pool settings. */
data class H2DbConfig(
    val jdbcUrl: String = "jdbc:h2:file:./logplay-data;AUTO_SERVER=TRUE",
    val user: String = "sa",
    val password: String = "",
    val poolMaxSize: Int = H2UnitOfWork.RECOMMENDED_JDBC_POOL_SIZE,
) {
    init {
        require(poolMaxSize > 0) { "poolMaxSize must be > 0, was $poolMaxSize" }
    }

    companion object {
        fun fromEnv(env: Env = Env()): H2DbConfig =
            H2DbConfig(
                jdbcUrl = env.string("H2_URL", "jdbc:h2:file:./logplay-data;AUTO_SERVER=TRUE"),
                user = env.string("H2_USER", "sa"),
                password = env.string("H2_PASSWORD", ""),
                poolMaxSize = env.int("H2_POOL_MAX_SIZE", H2UnitOfWork.RECOMMENDED_JDBC_POOL_SIZE),
            )
    }
}
