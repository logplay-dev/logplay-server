package org.zeplinko.logplay.server.config

/**
 * All configuration the app layer needs, as plain framework-free data. Backend modules build it
 * (usually via [fromEnv]) and pass it to `MainVerticle`'s constructor — nothing in the app layer
 * reads configuration from anywhere else.
 */
data class AppConfig(
    val httpHost: String = "0.0.0.0",
    val httpPort: Int = 8080,
    /** Maximum request body size in bytes; `-1` means unlimited (Vert.x `BodyHandler` default). */
    val maxBodyBytes: Long = -1,
    val cleanupIntervalMs: Long = 30_000,
    val metrics: MetricsConfig = MetricsConfig(),
) {
    init {
        require(httpPort in 0..65535) { "httpPort must be in 0..65535, was $httpPort" }
        require(cleanupIntervalMs > 0) { "cleanupIntervalMs must be > 0, was $cleanupIntervalMs" }
    }

    data class MetricsConfig(
        val enabled: Boolean = false,
        val serviceName: String = "logplay-server",
        /**
         * OTLP endpoint the server exports metrics to; `null` uses the OpenTelemetry SDK default.
         */
        val otlpEndpoint: String? = null,
    )

    companion object {
        fun fromEnv(env: Env = Env()): AppConfig =
            AppConfig(
                httpHost = env.string("HTTP_HOST", "0.0.0.0"),
                httpPort = env.int("HTTP_PORT", 8080),
                maxBodyBytes = env.long("MAX_BODY_BYTES", -1),
                cleanupIntervalMs = env.long("CLEANUP_INTERVAL_MS", 30_000),
                metrics =
                    MetricsConfig(
                        enabled = env.boolean("METRICS_ENABLED", false),
                        serviceName = env.string("METRICS_SERVICE_NAME", "logplay-server"),
                        otlpEndpoint = env.stringOrNull("METRICS_OTLP_ENDPOINT"),
                    ),
            )
    }
}
