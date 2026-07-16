package org.zeplinko.logplay.server.telemetry

import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.metrics.LongCounter
import io.opentelemetry.api.metrics.LongHistogram
import io.opentelemetry.api.metrics.Meter
import org.zeplinko.logplay.server.core.Metrics

/**
 * OpenTelemetry-backed adapter for the domain [Metrics] port. Maps each semantic event to a counter
 * or histogram; attribute cardinality is deliberately bounded (fixed enum-like values only).
 *
 * See `docs/METRICS.md` for the metric catalog (names, units, attributes).
 */
class OtelMetrics(meter: Meter) : Metrics {

    private val jobsCreated = meter.counter("logplay.jobs.created", "Jobs created")
    private val jobsDuplicate =
        meter.counter(
            "logplay.jobs.duplicate_rejected",
            "Duplicate-idempotency-key creates rejected",
        )
    private val acquireCalls =
        meter.counter("logplay.acquire.calls", "Acquire calls, tagged by outcome (hit|empty)")
    private val jobsAcquired = meter.counter("logplay.jobs.acquired", "Jobs claimed by acquire")
    private val checkpointsSaved = meter.counter("logplay.checkpoints.saved", "Checkpoints saved")
    private val jobsCompleted = meter.counter("logplay.jobs.completed", "Jobs finished")
    private val jobsReleased =
        meter.counter("logplay.jobs.released", "Jobs released, tagged scheduled (durable sleep)")
    private val errorsReported =
        meter.counter("logplay.errors.reported", "Execution errors, tagged retry")
    private val jobsFailed = meter.counter("logplay.jobs.failed", "Jobs terminal-failed")
    private val jobsAborted = meter.counter("logplay.jobs.aborted", "Jobs aborted")
    private val checkpointBytes: LongHistogram =
        meter
            .histogramBuilder("logplay.checkpoints.data_bytes")
            .ofLongs()
            .setDescription("Checkpoint payload size")
            .setUnit("By")
            .build()

    override fun onJobCreated() {
        jobsCreated.add(1L)
    }

    override fun onJobDuplicateRejected() {
        jobsDuplicate.add(1L)
    }

    override fun onJobsAcquired(requested: Int, acquired: Int) {
        acquireCalls.add(1L, Attributes.of(OUTCOME, if (acquired > 0) "hit" else "empty"))
        if (acquired > 0) jobsAcquired.add(acquired.toLong())
    }

    override fun onCheckpointSaved(dataBytes: Int) {
        checkpointsSaved.add(1L)
        checkpointBytes.record(dataBytes.toLong())
    }

    override fun onJobCompleted() {
        jobsCompleted.add(1L)
    }

    override fun onJobReleased(scheduled: Boolean) {
        jobsReleased.add(1L, Attributes.of(SCHEDULED, scheduled))
    }

    override fun onErrorReported(willRetry: Boolean) {
        errorsReported.add(1L, Attributes.of(RETRY, willRetry))
    }

    override fun onJobFailed() {
        jobsFailed.add(1L)
    }

    override fun onJobAborted() {
        jobsAborted.add(1L)
    }

    private companion object {
        val OUTCOME: AttributeKey<String> = AttributeKey.stringKey("outcome")
        val SCHEDULED: AttributeKey<Boolean> = AttributeKey.booleanKey("scheduled")
        val RETRY: AttributeKey<Boolean> = AttributeKey.booleanKey("retry")

        fun Meter.counter(name: String, description: String): LongCounter =
            counterBuilder(name).setDescription(description).build()
    }
}
