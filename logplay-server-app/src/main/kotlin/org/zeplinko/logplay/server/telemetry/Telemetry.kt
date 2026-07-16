package org.zeplinko.logplay.server.telemetry

import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.metrics.Meter
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk

/**
 * Process-global OpenTelemetry bootstrap. Idempotent: [install] builds the SDK once (subsequent
 * calls return the same instance), so multiple verticle deployments share one `MeterProvider`.
 *
 * Configuration follows the standard OpenTelemetry autoconfigure contract — operators set `OTEL_*`
 * env vars / `otel.*` system properties, which override the programmatic defaults set here (metrics
 * via OTLP, traces and logs off). The whole thing stays dormant unless `metrics.enabled=true` in
 * the app config, so nothing exports by default.
 */
object Telemetry {
    const val INSTRUMENTATION_SCOPE = "org.zeplinko.logplay.server"

    @Volatile private var openTelemetry: OpenTelemetry = OpenTelemetry.noop()
    @Volatile private var installed = false

    @Synchronized
    fun install(serviceName: String, otlpEndpoint: String?): OpenTelemetry {
        if (installed) return openTelemetry
        val defaults = buildMap {
            put("otel.service.name", serviceName)
            put("otel.metrics.exporter", "otlp")
            put("otel.traces.exporter", "none")
            put("otel.logs.exporter", "none")
            if (!otlpEndpoint.isNullOrBlank()) put("otel.exporter.otlp.endpoint", otlpEndpoint)
        }
        val sdk =
            AutoConfiguredOpenTelemetrySdk.builder()
                .addPropertiesSupplier { defaults }
                .build()
                .openTelemetrySdk
        Runtime.getRuntime().addShutdownHook(Thread { sdk.close() })
        openTelemetry = sdk
        installed = true
        return openTelemetry
    }

    fun meter(): Meter = openTelemetry.getMeter(INSTRUMENTATION_SCOPE)
}
