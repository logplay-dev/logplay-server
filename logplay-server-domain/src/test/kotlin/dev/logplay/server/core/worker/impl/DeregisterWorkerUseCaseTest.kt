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

class DeregisterWorkerUseCaseTest {

    private lateinit var workerGateway: InMemoryWorkerGateway
    private lateinit var jobGateway: InMemoryJobGateway
    private lateinit var useCase: DeregisterWorkerUseCaseImpl

    @BeforeEach
    fun setUp() {
        workerGateway = InMemoryWorkerGateway()
        jobGateway = InMemoryJobGateway()
        useCase = DeregisterWorkerUseCaseImpl(workerGateway, jobGateway)
    }

    // --- Happy path ---

    @Test
    fun `execute should delete the worker`() = runTest {
        val worker = aWorker("worker-1")
        workerGateway.save(worker)

        useCase.execute(DeregisterWorkerCommand("worker-1"))

        assertThat(workerGateway.findWorkerById("worker-1")).isNull()
    }

    @Test
    fun `execute should release all acquired jobs for the worker`() = runTest {
        val worker = aWorker("worker-1")
        workerGateway.save(worker)
        val job1 = aJob(acquiredByWorkerId = "worker-1")
        val job2 = aJob(acquiredByWorkerId = "worker-1")
        jobGateway.save(job1)
        jobGateway.save(job2)

        useCase.execute(DeregisterWorkerCommand("worker-1"))

        assertThat(jobGateway.findJobById(job1.id)!!.status).isEqualTo(JobStatus.PENDING)
        assertThat(jobGateway.findJobById(job1.id)!!.acquiredByWorkerId).isNull()
        assertThat(jobGateway.findJobById(job2.id)!!.status).isEqualTo(JobStatus.PENDING)
        assertThat(jobGateway.findJobById(job2.id)!!.acquiredByWorkerId).isNull()
    }

    @Test
    fun `execute should not affect jobs owned by other workers`() = runTest {
        val worker = aWorker("worker-1")
        workerGateway.save(worker)
        val otherJob = aJob(acquiredByWorkerId = "worker-2")
        jobGateway.save(otherJob)

        useCase.execute(DeregisterWorkerCommand("worker-1"))

        assertThat(jobGateway.findJobById(otherJob.id)!!.status).isEqualTo(JobStatus.ACQUIRED)
        assertThat(jobGateway.findJobById(otherJob.id)!!.acquiredByWorkerId).isEqualTo("worker-2")
    }

    @Test
    fun `execute should work when worker has no jobs`() = runTest {
        val worker = aWorker("worker-1")
        workerGateway.save(worker)

        useCase.execute(DeregisterWorkerCommand("worker-1"))

        assertThat(workerGateway.findWorkerById("worker-1")).isNull()
    }

    @Test
    fun `execute should condemn worker before releasing jobs to prevent new acquisitions`() =
        runTest {
            val worker = aWorker("worker-1")
            workerGateway.save(worker)
            val job = aJob(acquiredByWorkerId = "worker-1")
            jobGateway.save(job)

            useCase.execute(DeregisterWorkerCommand("worker-1"))

            // Worker should be fully deleted
            assertThat(workerGateway.findWorkerById("worker-1")).isNull()
            // Job should be released
            assertThat(jobGateway.findJobById(job.id)!!.status).isEqualTo(JobStatus.PENDING)
            // A condemned worker cannot acquire new jobs (tested via the INNER JOIN guard)
        }

    // --- Validation ---

    @Test
    fun `execute should throw BlankWorkerIdException when workerId is blank`() = runTest {
        val exception =
            runCatching { useCase.execute(DeregisterWorkerCommand("  ")) }.exceptionOrNull()

        assertThat(exception).isInstanceOf(BlankWorkerIdException::class.java)
    }

    @Test
    fun `execute should throw WorkerNotFoundException when worker does not exist`() = runTest {
        val exception =
            runCatching { useCase.execute(DeregisterWorkerCommand("non-existent")) }
                .exceptionOrNull()

        assertThat(exception).isInstanceOf(WorkerNotFoundException::class.java)
    }

    // --- Helpers ---

    private fun aWorker(id: String) =
        Worker(
            id = id,
            heartbeatTimeout = 5000,
            sessionTimeout = 15000,
            lastHeartbeatAt = Instant.now(),
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
