package dev.logplay.server.core.job.impl

import dev.logplay.server.core.job.*
import dev.logplay.server.core.job.fakes.InMemoryJobGateway
import dev.logplay.server.core.worker.BlankWorkerIdException
import dev.logplay.server.core.worker.Worker
import dev.logplay.server.core.worker.WorkerCondemnedException
import dev.logplay.server.core.worker.WorkerNotFoundException
import dev.logplay.server.core.worker.fakes.InMemoryWorkerGateway
import java.time.Instant
import java.util.UUID
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class AcquirePendingJobsUseCaseTest {

    private lateinit var jobGateway: InMemoryJobGateway
    private lateinit var workerGateway: InMemoryWorkerGateway
    private lateinit var useCase: AcquirePendingJobsUseCaseImpl

    private val workerId = "worker-1"

    @BeforeEach
    fun setUp() {
        workerGateway = InMemoryWorkerGateway()
        jobGateway = InMemoryJobGateway(workerGateway)
        useCase = AcquirePendingJobsUseCaseImpl(jobGateway, workerGateway)
        workerGateway.save(aWorker(workerId))
    }

    // --- Happy path ---

    @Test
    fun `execute should acquire pending jobs and set acquiredByWorkerId`() = runTest {
        val job = aJob()
        jobGateway.save(job)

        val result = useCase.execute(AcquirePendingJobsCommand(workerId, 10))

        assertThat(result).hasSize(1)
        assertThat(result[0].status).isEqualTo(JobStatus.ACQUIRED)
        assertThat(result[0].acquiredByWorkerId).isEqualTo(workerId)
    }

    @Test
    fun `execute should return empty list when no pending jobs`() = runTest {
        val result = useCase.execute(AcquirePendingJobsCommand(workerId, 10))

        assertThat(result).isEmpty()
    }

    // --- Validation ---

    @Test
    fun `execute should throw BlankWorkerIdException when workerId is blank`() = runTest {
        val exception =
            runCatching { useCase.execute(AcquirePendingJobsCommand("  ", 10)) }.exceptionOrNull()

        assertThat(exception).isInstanceOf(BlankWorkerIdException::class.java)
    }

    @Test
    fun `execute should throw InvalidLimitException when limit is zero`() = runTest {
        val exception =
            runCatching { useCase.execute(AcquirePendingJobsCommand(workerId, 0)) }
                .exceptionOrNull()

        assertThat(exception).isInstanceOf(InvalidLimitException::class.java)
    }

    @Test
    fun `execute should throw InvalidLimitException when limit is negative`() = runTest {
        val exception =
            runCatching { useCase.execute(AcquirePendingJobsCommand(workerId, -1)) }
                .exceptionOrNull()

        assertThat(exception).isInstanceOf(InvalidLimitException::class.java)
    }

    @Test
    fun `execute should throw InvalidLimitException when limit exceeds max`() = runTest {
        val exception =
            runCatching { useCase.execute(AcquirePendingJobsCommand(workerId, 101)) }
                .exceptionOrNull()

        assertThat(exception).isInstanceOf(InvalidLimitException::class.java)
    }

    @Test
    fun `execute should throw WorkerNotFoundException when worker does not exist`() = runTest {
        val exception =
            runCatching { useCase.execute(AcquirePendingJobsCommand("non-existent", 10)) }
                .exceptionOrNull()

        assertThat(exception).isInstanceOf(WorkerNotFoundException::class.java)
    }

    @Test
    fun `execute should throw WorkerNotFoundException when worker does not exist even with pending jobs`() =
        runTest {
            val job = aJob()
            jobGateway.save(job)

            val exception =
                runCatching { useCase.execute(AcquirePendingJobsCommand("non-existent", 10)) }
                    .exceptionOrNull()

            assertThat(exception).isInstanceOf(WorkerNotFoundException::class.java)
            assertThat(jobGateway.findJobById(job.id)!!.status).isEqualTo(JobStatus.PENDING)
        }

    @Test
    fun `execute should throw WorkerCondemnedException when worker is condemned and jobs exist`() =
        runTest {
            workerGateway.save(aWorker("condemned-worker").copy(condemned = true))
            val job = aJob()
            jobGateway.save(job)

            val exception =
                runCatching { useCase.execute(AcquirePendingJobsCommand("condemned-worker", 10)) }
                    .exceptionOrNull()

            assertThat(exception).isInstanceOf(WorkerCondemnedException::class.java)
            assertThat(jobGateway.findJobById(job.id)!!.status).isEqualTo(JobStatus.PENDING)
        }

    @Test
    fun `execute should throw WorkerCondemnedException when worker is condemned and no jobs exist`() =
        runTest {
            workerGateway.save(aWorker("condemned-worker").copy(condemned = true))

            val exception =
                runCatching { useCase.execute(AcquirePendingJobsCommand("condemned-worker", 10)) }
                    .exceptionOrNull()

            assertThat(exception).isInstanceOf(WorkerCondemnedException::class.java)
        }

    @Test
    fun `execute should throw WorkerNotFoundException when worker does not exist and no jobs exist`() =
        runTest {
            val exception =
                runCatching { useCase.execute(AcquirePendingJobsCommand("non-existent", 10)) }
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

    private fun aJob() =
        Job(
            id = UUID.randomUUID().toString(),
            name = "test-job",
            type = "test-type",
            status = JobStatus.PENDING,
            retries = 0,
            idempotencyKey = UUID.randomUUID().toString(),
            createdAt = Instant.now(),
            updatedAt = Instant.now(),
            version = 0,
        )
}
