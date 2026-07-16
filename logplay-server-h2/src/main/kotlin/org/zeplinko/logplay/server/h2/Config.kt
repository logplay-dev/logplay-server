package org.zeplinko.logplay.server.h2

import org.zeplinko.logplay.server.config.AppConfig
import org.zeplinko.logplay.server.config.Env

/** Full configuration for the H2 backend: shared [app] settings + H2 [db] connection. */
data class Config(val app: AppConfig, val db: H2DbConfig) {
    companion object {
        fun load(env: Env = Env()): Config = Config(AppConfig.fromEnv(env), H2DbConfig.fromEnv(env))
    }
}
