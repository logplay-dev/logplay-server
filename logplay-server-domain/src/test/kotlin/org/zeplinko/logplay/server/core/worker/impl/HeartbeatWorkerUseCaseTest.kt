package org.zeplinko.logplay.server.core.worker.impl

import java.time.Instant
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.zeplinko.logplay.server.core.worker.*
import org.zeplinko.logplay.server.core.worker.fakes.InMemoryWorkerGateway

class HeartbeatWorkerUseCaseTest {

    private lateinit var workerGateway: InMemoryWorkerGateway
    private lateinit var useCase: HeartbeatWorkerUseCaseImpl

    @BeforeEach
    fun setUp() {
        workerGateway = InMemoryWorkerGateway()
        useCase = HeartbeatWorkerUseCaseImpl(workerGateway)
    }

    // --- Happy path ---

    @Test
    fun `execute should update lastHeartbeatAt`() = runTest {
        // Recent (alive) so the liveness gate admits the heartbeat; still old enough to assert the
        // update moved it forward.
        val originalTime = Instant.now().minusMillis(1000)
        val worker = aWorker("worker-1", lastHeartbeatAt = originalTime)
        workerGateway.save(worker)

        val result = useCase.execute(HeartbeatWorkerCommand("worker-1"))

        assertThat(result.lastHeartbeatAt).isAfter(originalTime)
    }

    @Test
    fun `execute should return the updated worker`() = runTest {
        val worker = aWorker("worker-1")
        workerGateway.save(worker)

        val result = useCase.execute(HeartbeatWorkerCommand("worker-1"))

        assertThat(result.id).isEqualTo("worker-1")
        assertThat(result.heartbeatTimeout).isEqualTo(worker.heartbeatTimeout)
        assertThat(result.sessionTimeout).isEqualTo(worker.sessionTimeout)
    }

    // --- Validation ---

    @Test
    fun `execute should throw BlankWorkerIdException when workerId is blank`() = runTest {
        val exception =
            runCatching { useCase.execute(HeartbeatWorkerCommand("  ")) }.exceptionOrNull()

        assertThat(exception).isInstanceOf(BlankWorkerIdException::class.java)
    }

    @Test
    fun `execute should throw WorkerNotFoundException when worker does not exist`() = runTest {
        val exception =
            runCatching { useCase.execute(HeartbeatWorkerCommand("non-existent")) }
                .exceptionOrNull()

        assertThat(exception).isInstanceOf(WorkerNotFoundException::class.java)
    }

    @Test
    fun `execute should throw WorkerNotFoundException when worker has timed out`() = runTest {
        // Last heartbeat far enough in the past to exceed sessionTimeout — a timed-out worker
        // cannot heartbeat back to life and must re-register.
        val worker = aWorker("dead-worker", lastHeartbeatAt = Instant.now().minusSeconds(3600))
        workerGateway.save(worker)

        val exception =
            runCatching { useCase.execute(HeartbeatWorkerCommand("dead-worker")) }.exceptionOrNull()

        assertThat(exception).isInstanceOf(WorkerNotFoundException::class.java)
    }

    // --- Helpers ---

    private fun aWorker(id: String, lastHeartbeatAt: Instant = Instant.now()) =
        Worker(
            id = id,
            heartbeatTimeout = 5000,
            sessionTimeout = 15000,
            lastHeartbeatAt = lastHeartbeatAt,
            registeredAt = Instant.now(),
        )
}
