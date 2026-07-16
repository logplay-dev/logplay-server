package org.zeplinko.logplay.server.telemetry

import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.metrics.LongCounter
import io.opentelemetry.api.metrics.LongHistogram
import io.opentelemetry.api.metrics.Meter
import io.vertx.ext.web.RoutingContext

/**
 * Per-request HTTP metrics (duration + count) tagged with method, matched route pattern, and status
 * code. Installed as the first handler on the root router: it stamps a start time and registers an
 * end-handler that fires on response completion (success or failure). Using the matched route
 * pattern (e.g. `/api/v1/jobs/:jobId/checkpoints`) keeps `http.route` cardinality bounded.
 */
class HttpMetrics(meter: Meter) {

    private val duration: LongHistogram =
        meter
            .histogramBuilder("logplay.http.server.duration")
            .ofLongs()
            .setDescription("HTTP server request duration")
            .setUnit("ms")
            .build()
    private val requests: LongCounter =
        meter.counterBuilder("logplay.http.server.requests").setDescription("HTTP requests").build()

    fun handle(rc: RoutingContext) {
        val startNanos = System.nanoTime()
        rc.addEndHandler {
            val elapsedMs = (System.nanoTime() - startNanos) / 1_000_000L
            val attributes =
                Attributes.builder()
                    .put(METHOD, rc.request().method().name())
                    .put(ROUTE, routeLabel(rc))
                    .put(STATUS, rc.response().statusCode.toLong())
                    .build()
            duration.record(elapsedMs, attributes)
            requests.add(1L, attributes)
        }
        rc.next()
    }

    private fun routeLabel(rc: RoutingContext): String =
        rc.currentRoute()?.path ?: rc.normalizedPath()

    private companion object {
        val METHOD: AttributeKey<String> = AttributeKey.stringKey("http.request.method")
        val ROUTE: AttributeKey<String> = AttributeKey.stringKey("http.route")
        val STATUS: AttributeKey<Long> = AttributeKey.longKey("http.response.status_code")
    }
}
