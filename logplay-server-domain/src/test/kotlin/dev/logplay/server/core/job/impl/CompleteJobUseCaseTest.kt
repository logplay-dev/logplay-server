package dev.logplay.server.core.job.impl

import dev.logplay.server.core.job.*
import dev.logplay.server.core.job.fakes.InMemoryJobGateway
import dev.logplay.server.core.worker.BlankWorkerIdException
import java.time.Instant
import java.util.UUID
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class CompleteJobUseCaseTest {

    private lateinit var gateway: InMemoryJobGateway
    private lateinit var useCase: CompleteJobUseCaseImpl

    private val workerId = "worker-1"

    @BeforeEach
    fun setUp() {
        gateway = InMemoryJobGateway()
        useCase = CompleteJobUseCaseImpl(gateway)
    }

    // --- Happy path ---

    @Test
    fun `execute should transition an ACQUIRED job to FINISHED`() = runTest {
        val job = aJob(status = JobStatus.ACQUIRED, acquiredByWorkerId = workerId)
        gateway.save(job)

        val result = useCase.execute(CompleteJobCommand(job.id, workerId))

        assertThat(result.id).isEqualTo(job.id)
        assertThat(result.status).isEqualTo(JobStatus.FINISHED)
    }

    @Test
    fun `execute should update the updatedAt timestamp`() = runTest {
        val job = aJob(status = JobStatus.ACQUIRED, acquiredByWorkerId = workerId)
        gateway.save(job)

        val result = useCase.execute(CompleteJobCommand(job.id, workerId))

        assertThat(result.updatedAt).isAfterOrEqualTo(job.updatedAt)
    }

    @Test
    fun `execute should preserve all other job fields`() = runTest {
        val job = aJob(status = JobStatus.ACQUIRED, acquiredByWorkerId = workerId)
        gateway.save(job)

        val result = useCase.execute(CompleteJobCommand(job.id, workerId))

        assertThat(result.name).isEqualTo(job.name)
        assertThat(result.type).isEqualTo(job.type)
        assertThat(result.retries).isEqualTo(job.retries)
        assertThat(result.createdAt).isEqualTo(job.createdAt)
        assertThat(result.version).isEqualTo(job.version + 1)
    }

    @Test
    fun `execute should clear acquiredByWorkerId when completing a job`() = runTest {
        val job = aJob(status = JobStatus.ACQUIRED, acquiredByWorkerId = workerId)
        gateway.save(job)

        val result = useCase.execute(CompleteJobCommand(job.id, workerId))

        assertThat(result.acquiredByWorkerId).isNull()
    }

    // --- Validation ---

    @Test
    fun `execute should throw BlankJobIdException when jobId is blank`() = runTest {
        val exception =
            runCatching { useCase.execute(CompleteJobCommand("  ", workerId)) }.exceptionOrNull()

        assertThat(exception).isInstanceOf(BlankJobIdException::class.java)
    }

    @Test
    fun `execute should throw BlankWorkerIdException when workerId is blank`() = runTest {
        val exception =
            runCatching { useCase.execute(CompleteJobCommand("job-1", "  ")) }.exceptionOrNull()

        assertThat(exception).isInstanceOf(BlankWorkerIdException::class.java)
    }

    // --- State ---

    @Test
    fun `execute should throw JobNotFoundException when job does not exist`() = runTest {
        val exception =
            runCatching { useCase.execute(CompleteJobCommand("non-existent", workerId)) }
                .exceptionOrNull()

        assertThat(exception).isInstanceOf(JobNotFoundException::class.java)
    }

    @Test
    fun `execute should throw JobNotAcquiredException for all non-ACQUIRED statuses`() = runTest {
        for (status in
            listOf(JobStatus.PENDING, JobStatus.FINISHED, JobStatus.FAILED, JobStatus.ABORTED)) {
            val localGateway = InMemoryJobGateway()
            val job = aJob(status = status)
            localGateway.save(job)

            val exception =
                runCatching {
                        CompleteJobUseCaseImpl(localGateway)
                            .execute(CompleteJobCommand(job.id, workerId))
                    }
                    .exceptionOrNull()

            assertThat(exception)
                .describedAs("expected JobNotAcquiredException for status $status")
                .isInstanceOf(JobNotAcquiredException::class.java)
        }
    }

    // --- Ownership ---

    @Test
    fun `execute should throw JobNotOwnedByWorkerException when workerId does not match`() =
        runTest {
            val job = aJob(status = JobStatus.ACQUIRED, acquiredByWorkerId = "other-worker")
            gateway.save(job)

            val exception =
                runCatching { useCase.execute(CompleteJobCommand(job.id, workerId)) }
                    .exceptionOrNull()

            assertThat(exception).isInstanceOf(JobNotOwnedByWorkerException::class.java)
        }

    // --- Helpers ---

    private fun aJob(status: JobStatus, acquiredByWorkerId: String? = null) =
        Job(
            id = UUID.randomUUID().toString(),
            name = "test-job",
            type = "test-type",
            status = status,
            retries = 0,
            idempotencyKey = UUID.randomUUID().toString(),
            createdAt = Instant.now(),
            updatedAt = Instant.now(),
            acquiredByWorkerId = acquiredByWorkerId,
            version = 1,
        )
}
