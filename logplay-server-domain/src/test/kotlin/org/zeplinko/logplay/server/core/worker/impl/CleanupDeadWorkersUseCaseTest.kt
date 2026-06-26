package org.zeplinko.logplay.server.core.worker.impl

import java.time.Instant
import java.util.UUID
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.zeplinko.logplay.server.core.fakes.InMemoryUnitOfWork
import org.zeplinko.logplay.server.core.job.*
import org.zeplinko.logplay.server.core.job.fakes.InMemoryJobGateway
import org.zeplinko.logplay.server.core.worker.*
import org.zeplinko.logplay.server.core.worker.fakes.InMemoryWorkerGateway

class CleanupDeadWorkersUseCaseTest {

    private lateinit var workerGateway: InMemoryWorkerGateway
    private lateinit var jobGateway: InMemoryJobGateway
    private lateinit var useCase: CleanupDeadWorkersUseCaseImpl

    @BeforeEach
    fun setUp() {
        workerGateway = InMemoryWorkerGateway()
        jobGateway = InMemoryJobGateway()
        useCase = CleanupDeadWorkersUseCaseImpl(workerGateway, jobGateway, InMemoryUnitOfWork())
    }

    @Test
    fun `execute reclaims a dead worker - releases its jobs and deletes it - in one pass`() =
        runTest {
            workerGateway.save(
                aWorker("worker-1", lastHeartbeatAt = Instant.now().minusMillis(20000))
            )
            val job = aJob(acquiredByWorkerId = "worker-1")
            jobGateway.save(job)

            val count = useCase.execute()

            assertThat(count).isEqualTo(1)
            assertThat(workerGateway.findWorkerById("worker-1")).isNull()
            assertThat(jobGateway.findJobById(job.id)!!.status).isEqualTo(JobStatus.PENDING)
            assertThat(jobGateway.findJobById(job.id)!!.acquiredByWorkerId).isNull()
            val released =
                jobGateway.eventsForJob(job.id).single { it.eventType == JobEventType.RELEASED }
            assertThat(released.actorType).isEqualTo(ActorType.SYSTEM)
            assertThat(released.actorId).isNull()
            assertThat(released.eventMessage).isEqualTo("released by dead-worker cleanup")
        }

    @Test
    fun `execute does not touch alive workers`() = runTest {
        workerGateway.save(aWorker("worker-1", lastHeartbeatAt = Instant.now()))
        val job = aJob(acquiredByWorkerId = "worker-1")
        jobGateway.save(job)

        val count = useCase.execute()

        assertThat(count).isEqualTo(0)
        assertThat(workerGateway.findWorkerById("worker-1")).isNotNull
        assertThat(jobGateway.findJobById(job.id)!!.status).isEqualTo(JobStatus.ACQUIRED)
    }

    @Test
    fun `execute returns 0 when there are no dead workers`() = runTest {
        workerGateway.save(aWorker("alive", lastHeartbeatAt = Instant.now()))
        assertThat(useCase.execute()).isEqualTo(0)
    }

    @Test
    fun `execute reclaims multiple dead workers in one pass`() = runTest {
        val deadTime = Instant.now().minusMillis(20000)
        workerGateway.save(aWorker("worker-1", lastHeartbeatAt = deadTime))
        workerGateway.save(aWorker("worker-2", lastHeartbeatAt = deadTime))
        jobGateway.save(aJob(acquiredByWorkerId = "worker-1"))
        jobGateway.save(aJob(acquiredByWorkerId = "worker-2"))

        val count = useCase.execute()

        assertThat(count).isEqualTo(2)
        assertThat(workerGateway.count()).isEqualTo(0)
    }

    @Test
    fun `execute deletes a dead worker that has no jobs`() = runTest {
        workerGateway.save(aWorker("worker-1", lastHeartbeatAt = Instant.now().minusMillis(20000)))

        val count = useCase.execute()

        assertThat(count).isEqualTo(1)
        assertThat(workerGateway.findWorkerById("worker-1")).isNull()
    }

    @Test
    fun `execute reclaims dead workers while leaving alive ones untouched`() = runTest {
        workerGateway.save(aWorker("dead", lastHeartbeatAt = Instant.now().minusMillis(20000)))
        workerGateway.save(aWorker("alive", lastHeartbeatAt = Instant.now()))
        val aliveJob = aJob(acquiredByWorkerId = "alive")
        jobGateway.save(aliveJob)

        val count = useCase.execute()

        assertThat(count).isEqualTo(1)
        assertThat(workerGateway.findWorkerById("dead")).isNull()
        assertThat(workerGateway.findWorkerById("alive")).isNotNull
        assertThat(jobGateway.findJobById(aliveJob.id)!!.status).isEqualTo(JobStatus.ACQUIRED)
        assertThat(jobGateway.findJobById(aliveJob.id)!!.acquiredByWorkerId).isEqualTo("alive")
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
            createdAt = Instant.now(),
            updatedAt = Instant.now(),
            acquiredByWorkerId = acquiredByWorkerId,
            lastAcquiredAt = Instant.now(),
        )
}
