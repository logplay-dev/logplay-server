package org.zeplinko.logplay.server.core

/**
 * Outbound telemetry port. Framework-free by design: no OpenTelemetry (or any infrastructure) type
 * crosses this boundary, so the domain module stays dependency-clean — exactly like [UnitOfWork].
 * The app layer supplies an OpenTelemetry-backed adapter; [NoopMetrics] is the default so use cases
 * and unit tests need no wiring.
 *
 * These are **semantic** events (a job was created, a checkpoint was saved), not transport details.
 * Mapping to concrete counters/histograms/attributes is the adapter's job.
 */
interface Metrics {
    /** A job was created and committed. */
    fun onJobCreated()

    /** A create was rejected because its `(groupId, idempotencyKey)` already exists. */
    fun onJobDuplicateRejected()

    /**
     * One acquire call completed: [requested] is the limit asked for, [acquired] the number
     * claimed.
     */
    fun onJobsAcquired(requested: Int, acquired: Int)

    /**
     * A checkpoint was saved carrying [dataBytes] bytes of payload (0 for a data-less checkpoint).
     */
    fun onCheckpointSaved(dataBytes: Int)

    /** A job transitioned to FINISHED. */
    fun onJobCompleted()

    /**
     * A job returned to the queue; [scheduled] = a future `availableAt` (durable sleep) vs
     * immediate.
     */
    fun onJobReleased(scheduled: Boolean)

    /**
     * A worker reported an execution error; [willRetry] = re-queued, else it went terminal FAILED.
     */
    fun onErrorReported(willRetry: Boolean)

    /** A job reached terminal FAILED (retries exhausted). */
    fun onJobFailed()

    /** A job was aborted. */
    fun onJobAborted()
}

/** No-op [Metrics] — the default when telemetry is disabled or in tests. */
object NoopMetrics : Metrics {
    override fun onJobCreated() {}

    override fun onJobDuplicateRejected() {}

    override fun onJobsAcquired(requested: Int, acquired: Int) {}

    override fun onCheckpointSaved(dataBytes: Int) {}

    override fun onJobCompleted() {}

    override fun onJobReleased(scheduled: Boolean) {}

    override fun onErrorReported(willRetry: Boolean) {}

    override fun onJobFailed() {}

    override fun onJobAborted() {}
}
