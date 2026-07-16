package org.zeplinko.logplay.server.core.job.impl

import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.zeplinko.logplay.server.core.Metrics
import org.zeplinko.logplay.server.core.fakes.InMemoryUnitOfWork
import org.zeplinko.logplay.server.core.job.CreateJobCommand
import org.zeplinko.logplay.server.core.job.fakes.InMemoryJobGateway

class MetricsInstrumentationTest {

    private class RecordingMetrics : Metrics {
        var created = 0
        var duplicates = 0

        override fun onJobCreated() {
            created++
        }

        override fun onJobDuplicateRejected() {
            duplicates++
        }

        override fun onJobsAcquired(requested: Int, acquired: Int) {}

        override fun onCheckpointSaved(dataBytes: Int) {}

        override fun onJobCompleted() {}

        override fun onJobReleased(scheduled: Boolean) {}

        override fun onErrorReported(willRetry: Boolean) {}

        override fun onJobFailed() {}

        override fun onJobAborted() {}
    }

    private fun createCommand(idempotencyKey: String) =
        CreateJobCommand(groupId = "g", name = "", type = "t", idempotencyKey = idempotencyKey)

    @Test
    fun `create records created on success and duplicate on conflict`() = runTest {
        val metrics = RecordingMetrics()
        val gateway = InMemoryJobGateway()
        val useCase = CreateJobUseCaseImpl(gateway, InMemoryUnitOfWork(), metrics)

        useCase.execute(createCommand("k"))
        runCatching { useCase.execute(createCommand("k")) }

        assertThat(metrics.created).isEqualTo(1)
        assertThat(metrics.duplicates).isEqualTo(1)
    }

    @Test
    fun `use cases default to NoopMetrics when none is supplied`() = runTest {
        // Constructing without a Metrics argument must still work (the NoopMetrics default).
        val useCase = CreateJobUseCaseImpl(InMemoryJobGateway(), InMemoryUnitOfWork())

        val job = useCase.execute(createCommand("k"))

        assertThat(job.id).isNotBlank()
    }
}
