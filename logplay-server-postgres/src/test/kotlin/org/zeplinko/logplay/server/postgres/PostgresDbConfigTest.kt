package org.zeplinko.logplay.server.postgres

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.zeplinko.logplay.server.config.Env

class PostgresDbConfigTest {

    private fun env(vararg pairs: Pair<String, String>): Env {
        val map = pairs.toMap()
        return Env { map[it] }
    }

    @Test
    fun `defaults and derived jdbcUrl`() {
        val db = PostgresDbConfig.fromEnv(env())

        assertThat(db.poolMaxSize).isEqualTo(32)
        assertThat(db.jdbcUrl).isEqualTo("jdbc:postgresql://localhost:5432/logplay")
    }

    @Test
    fun `reads env including the now-configurable pool size`() {
        val db =
            PostgresDbConfig.fromEnv(
                env(
                    "DB_HOST" to "pg",
                    "DB_PORT" to "6000",
                    "DB_NAME" to "d",
                    "DB_POOL_MAX_SIZE" to "128",
                    "DB_PIPELINING_LIMIT" to "512",
                )
            )

        assertThat(db.jdbcUrl).isEqualTo("jdbc:postgresql://pg:6000/d")
        assertThat(db.poolMaxSize).isEqualTo(128)
        assertThat(db.pipeliningLimit).isEqualTo(512)
    }

    @Test
    fun `non-positive pool size is rejected`() {
        assertThatThrownBy { PostgresDbConfig.fromEnv(env("DB_POOL_MAX_SIZE" to "0")) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("poolMaxSize")
    }
}
