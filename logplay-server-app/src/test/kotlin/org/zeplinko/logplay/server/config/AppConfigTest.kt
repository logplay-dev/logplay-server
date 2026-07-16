package org.zeplinko.logplay.server.config

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

class AppConfigTest {

    private fun env(vararg pairs: Pair<String, String>): Env {
        val map = pairs.toMap()
        return Env { map[it] }
    }

    @Test
    fun `defaults when nothing is set`() {
        val cfg = AppConfig.fromEnv(env())

        assertThat(cfg).isEqualTo(AppConfig())
        assertThat(cfg.httpHost).isEqualTo("0.0.0.0")
        assertThat(cfg.httpPort).isEqualTo(8080)
        assertThat(cfg.maxBodyBytes).isEqualTo(-1)
        assertThat(cfg.metrics.enabled).isFalse()
    }

    @Test
    fun `reads and parses env values`() {
        val cfg =
            AppConfig.fromEnv(
                env(
                    "HTTP_HOST" to "127.0.0.1",
                    "HTTP_PORT" to "9090",
                    "MAX_BODY_BYTES" to "1048576",
                    "CLEANUP_INTERVAL_MS" to "5000",
                    "METRICS_ENABLED" to "true",
                    "METRICS_SERVICE_NAME" to "svc",
                    "METRICS_OTLP_ENDPOINT" to "http://collector:4317",
                )
            )

        assertThat(cfg.httpHost).isEqualTo("127.0.0.1")
        assertThat(cfg.httpPort).isEqualTo(9090)
        assertThat(cfg.maxBodyBytes).isEqualTo(1_048_576)
        assertThat(cfg.cleanupIntervalMs).isEqualTo(5000)
        assertThat(cfg.metrics)
            .isEqualTo(AppConfig.MetricsConfig(true, "svc", "http://collector:4317"))
    }

    @Test
    fun `blank env values are treated as unset and boolean is case-insensitive`() {
        val cfg =
            AppConfig.fromEnv(env("METRICS_SERVICE_NAME" to "  ", "METRICS_ENABLED" to "TRUE"))

        assertThat(cfg.metrics.serviceName).isEqualTo("logplay-server")
        assertThat(cfg.metrics.enabled).isTrue()
    }

    @Test
    fun `malformed integer fails fast with the variable name`() {
        assertThatThrownBy { AppConfig.fromEnv(env("HTTP_PORT" to "abc")) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("HTTP_PORT")
    }

    @Test
    fun `out-of-range port is rejected`() {
        assertThatThrownBy { AppConfig.fromEnv(env("HTTP_PORT" to "70000")) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("httpPort")
    }
}
