package org.zeplinko.logplay.server.h2

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.zeplinko.logplay.server.config.Env
import org.zeplinko.logplay.server.job.adapters.H2UnitOfWork

class H2DbConfigTest {

    private fun env(vararg pairs: Pair<String, String>): Env {
        val map = pairs.toMap()
        return Env { map[it] }
    }

    @Test
    fun `defaults`() {
        val db = H2DbConfig.fromEnv(env())

        assertThat(db.user).isEqualTo("sa")
        assertThat(db.poolMaxSize).isEqualTo(H2UnitOfWork.RECOMMENDED_JDBC_POOL_SIZE)
    }

    @Test
    fun `reads env`() {
        val db = H2DbConfig.fromEnv(env("H2_URL" to "jdbc:h2:mem:t", "H2_POOL_MAX_SIZE" to "10"))

        assertThat(db.jdbcUrl).isEqualTo("jdbc:h2:mem:t")
        assertThat(db.poolMaxSize).isEqualTo(10)
    }
}
