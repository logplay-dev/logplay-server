package org.zeplinko.logplay.server.postgres

import org.zeplinko.logplay.server.config.AppConfig
import org.zeplinko.logplay.server.config.Env

/**
 * Full configuration for the Postgres backend: shared [app] settings + Postgres [db] connection.
 */
data class Config(val app: AppConfig, val db: PostgresDbConfig) {
    companion object {
        fun load(env: Env = Env()): Config =
            Config(AppConfig.fromEnv(env), PostgresDbConfig.fromEnv(env))
    }
}
