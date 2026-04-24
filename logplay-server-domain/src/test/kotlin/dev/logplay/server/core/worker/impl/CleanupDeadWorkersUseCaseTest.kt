package dev.logplay.server.core.worker.impl

import dev.logplay.server.core.job.*
import dev.logplay.server.core.job.fakes.InMemoryJobGateway
import dev.logplay.server.core.worker.*
import dev.logplay.server.core.worker.fakes.InMemoryWorkerGateway
import java.time.Instant
import java.util.UUID
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class CleanupDeadWorkersUseCaseTest {

    private lateinit var workerGateway: InMemoryWorkerGateway
    private lateinit var jobGateway: InMemoryJobGateway
    private lateinit var useCase: CleanupDeadWorkersUseCaseImpl

    @BeforeEach
    fun setUp() {
        workerGateway = InMemoryWorkerGateway()
        jobGateway = InMemoryJobGateway()
        useCase = CleanupDeadWorkersUseCaseImpl(workerGateway, jobGateway, condemnPeriodMs = 0)
    }

    // --- Phase 1: Condemn dead workers ---

    @Test
    fun `execute should condemn a dead worker without deleting it`() = runTest {
        val worker = aWorker("worker-1", lastHeartbeatAt = Instant.now().minusMillis(20000))
        workerGateway.save(worker)
        val job = aJob(acquiredByWorkerId = "worker-1")
        jobGateway.save(job)

        useCase.execute()

        val updated = workerGateway.findWorkerById("worker-1")
        assertThat(updated).isNotNull
        assertThat(updated!!.condemned).isTrue()
        assertThat(jobGateway.findJobById(job.id)!!.status).isEqualTo(JobStatus.ACQUIRED)
        assertThat(jobGateway.findJobById(job.id)!!.acquiredByWorkerId).isEqualTo("worker-1")
    }

    @Test
    fun `execute should not condemn alive workers`() = runTest {
        val worker = aWorker("worker-1", lastHeartbeatAt = Instant.now())
        workerGateway.save(worker)

        val count = useCase.execute()

        assertThat(count).isEqualTo(0)
        assertThat(workerGateway.findWorkerById("worker-1")!!.condemned).isFalse()
    }

    @Test
    fun `execute should not re-condemn already condemned workers`() = runTest {
        val worker =
            aWorker("worker-1", lastHeartbeatAt = Instant.now().minusMillis(20000))
                .copy(condemned = true)
        workerGateway.save(worker)

        val count = useCase.execute()

        // Only the condemned worker is processed in phase 2, no new dead workers found
        assertThat(count).isEqualTo(1)
    }

    // --- Phase 2: Evict condemned workers ---

    @Test
    fun `execute should delete condemned worker and release its jobs on second run`() = runTest {
        val worker = aWorker("worker-1", lastHeartbeatAt = Instant.now().minusMillis(20000))
        workerGateway.save(worker)
        val job = aJob(acquiredByWorkerId = "worker-1")
        jobGateway.save(job)

        // Phase 1: condemn
        useCase.execute()
        assertThat(workerGateway.findWorkerById("worker-1")).isNotNull
        assertThat(jobGateway.findJobById(job.id)!!.status).isEqualTo(JobStatus.ACQUIRED)

        // Phase 2: evict
        useCase.execute()
        assertThat(workerGateway.findWorkerById("worker-1")).isNull()
        assertThat(jobGateway.findJobById(job.id)!!.status).isEqualTo(JobStatus.PENDING)
        assertThat(jobGateway.findJobById(job.id)!!.acquiredByWorkerId).isNull()
    }

    @Test
    fun `execute should handle multiple dead workers across two phases`() = runTest {
        val deadTime = Instant.now().minusMillis(20000)
        workerGateway.save(aWorker("worker-1", lastHeartbeatAt = deadTime))
        workerGateway.save(aWorker("worker-2", lastHeartbeatAt = deadTime))
        jobGateway.save(aJob(acquiredByWorkerId = "worker-1"))
        jobGateway.save(aJob(acquiredByWorkerId = "worker-2"))

        // Phase 1: both condemned
        useCase.execute()
        assertThat(workerGateway.count()).isEqualTo(2)
        assertThat(workerGateway.findWorkerById("worker-1")!!.condemned).isTrue()
        assertThat(workerGateway.findWorkerById("worker-2")!!.condemned).isTrue()

        // Phase 2: both evicted
        useCase.execute()
        assertThat(workerGateway.count()).isEqualTo(0)
    }

    @Test
    fun `execute should delete condemned worker that has no jobs`() = runTest {
        val worker = aWorker("worker-1", lastHeartbeatAt = Instant.now().minusMillis(20000))
        workerGateway.save(worker)

        useCase.execute() // condemn
        useCase.execute() // evict

        assertThat(workerGateway.findWorkerById("worker-1")).isNull()
    }

    @Test
    fun `execute should not affect alive workers while evicting condemned ones`() = runTest {
        val deadWorker = aWorker("dead", lastHeartbeatAt = Instant.now().minusMillis(20000))
        val aliveWorker = aWorker("alive", lastHeartbeatAt = Instant.now())
        workerGateway.save(deadWorker)
        workerGateway.save(aliveWorker)
        val aliveJob = aJob(acquiredByWorkerId = "alive")
        jobGateway.save(aliveJob)

        useCase.execute() // condemn dead
        useCase.execute() // evict dead

        assertThat(workerGateway.findWorkerById("alive")).isNotNull
        assertThat(workerGateway.findWorkerById("alive")!!.condemned).isFalse()
        assertThat(jobGateway.findJobById(aliveJob.id)!!.status).isEqualTo(JobStatus.ACQUIRED)
        assertThat(jobGateway.findJobById(aliveJob.id)!!.acquiredByWorkerId).isEqualTo("alive")
        assertThat(workerGateway.findWorkerById("dead")).isNull()
    }

    // --- Condemn period ---

    @Test
    fun `execute should not evict condemned worker before condemn period elapses`() = runTest {
        val useCaseWithPeriod =
            CleanupDeadWorkersUseCaseImpl(workerGateway, jobGateway, condemnPeriodMs = 30000)
        // Worker died 20s ago, sessionTimeout=15s, condemnPeriod=30s
        // Threshold = 15000 + 30000 = 45000. Elapsed = 20000. Not evicted.
        val worker =
            aWorker("worker-1", lastHeartbeatAt = Instant.now().minusMillis(20000))
                .copy(condemned = true)
        workerGateway.save(worker)
        val job = aJob(acquiredByWorkerId = "worker-1")
        jobGateway.save(job)

        val count = useCaseWithPeriod.execute()

        assertThat(count).isEqualTo(0)
        assertThat(workerGateway.findWorkerById("worker-1")).isNotNull
        assertThat(jobGateway.findJobById(job.id)!!.status).isEqualTo(JobStatus.ACQUIRED)
    }

    @Test
    fun `execute should evict condemned worker after condemn period elapses`() = runTest {
        val useCaseWithPeriod =
            CleanupDeadWorkersUseCaseImpl(workerGateway, jobGateway, condemnPeriodMs = 5000)
        // Worker died 25s ago, sessionTimeout=15s, condemnPeriod=5s
        // Threshold = 15000 + 5000 = 20000. Elapsed = 25000. Evicted.
        val worker =
            aWorker("worker-1", lastHeartbeatAt = Instant.now().minusMillis(25000))
                .copy(condemned = true)
        workerGateway.save(worker)
        val job = aJob(acquiredByWorkerId = "worker-1")
        jobGateway.save(job)

        val count = useCaseWithPeriod.execute()

        assertThat(count).isEqualTo(1)
        assertThat(workerGateway.findWorkerById("worker-1")).isNull()
        assertThat(jobGateway.findJobById(job.id)!!.status).isEqualTo(JobStatus.PENDING)
    }

    // --- Helpers ---

    private fun aWorker(
        id: String,
        lastHeartbeatAt: Instant = Instant.now(),
        sessionTimeout: Long = 15000,
    ) =
        Worker(
            id = id,
            heartbeatTimeout = 5000,
            sessionTimeout = sessionTimeout,
            lastHeartbeatAt = lastHeartbeatAt,
            registeredAt = Instant.now(),
        )

    private fun aJob(acquiredByWorkerId: String) =
        Job(
            id = UUID.randomUUID().toString(),
            groupId = "test-group",
            name = "test-job",
            type = "test-type",
            status = JobStatus.ACQUIRED,
            retries = 0,
            idempotencyKey = UUID.randomUUID().toString(),
            createdAt = Instant.now(),
            updatedAt = Instant.now(),
            acquiredByWorkerId = acquiredByWorkerId,
            version = 1,
        )
}
