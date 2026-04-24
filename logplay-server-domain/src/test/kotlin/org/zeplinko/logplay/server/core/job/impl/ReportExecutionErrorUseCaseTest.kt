package org.zeplinko.logplay.server.core.job.impl

import java.time.Instant
import java.util.UUID
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.zeplinko.logplay.server.core.job.*
import org.zeplinko.logplay.server.core.job.fakes.InMemoryJobGateway
import org.zeplinko.logplay.server.core.worker.BlankWorkerIdException

class ReportExecutionErrorUseCaseTest {

    private lateinit var gateway: InMemoryJobGateway
    private lateinit var useCase: ReportExecutionErrorUseCaseImpl

    private val workerId = "worker-1"

    @BeforeEach
    fun setUp() {
        gateway = InMemoryJobGateway()
        useCase = ReportExecutionErrorUseCaseImpl(gateway)
    }

    // --- Happy path ---

    @Test
    fun `execute should transition acquired job to PENDING and increment retries when no maxRetries`() =
        runTest {
            val job =
                aJob(
                    status = JobStatus.ACQUIRED,
                    retries = 0,
                    maxRetries = null,
                    acquiredByWorkerId = workerId,
                )
            gateway.save(job)

            val result =
                useCase.execute(ReportExecutionErrorCommand(job.id, workerId, "some error"))

            assertThat(result.status).isEqualTo(JobStatus.PENDING)
            assertThat(result.retries).isEqualTo(1)
        }

    @Test
    fun `execute should transition acquired job to PENDING when retries below maxRetries`() =
        runTest {
            val job =
                aJob(
                    status = JobStatus.ACQUIRED,
                    retries = 1,
                    maxRetries = 3,
                    acquiredByWorkerId = workerId,
                )
            gateway.save(job)

            val result = useCase.execute(ReportExecutionErrorCommand(job.id, workerId, "error"))

            assertThat(result.status).isEqualTo(JobStatus.PENDING)
            assertThat(result.retries).isEqualTo(2)
        }

    @Test
    fun `execute should transition to FAILED when retries reach maxRetries`() = runTest {
        val job =
            aJob(
                status = JobStatus.ACQUIRED,
                retries = 2,
                maxRetries = 3,
                acquiredByWorkerId = workerId,
            )
        gateway.save(job)

        val result = useCase.execute(ReportExecutionErrorCommand(job.id, workerId, "error"))

        assertThat(result.status).isEqualTo(JobStatus.FAILED)
        assertThat(result.retries).isEqualTo(3)
    }

    @Test
    fun `execute should transition to FAILED when maxRetries is 1 and first error`() = runTest {
        val job =
            aJob(
                status = JobStatus.ACQUIRED,
                retries = 0,
                maxRetries = 1,
                acquiredByWorkerId = workerId,
            )
        gateway.save(job)

        val result = useCase.execute(ReportExecutionErrorCommand(job.id, workerId, "error"))

        assertThat(result.status).isEqualTo(JobStatus.FAILED)
        assertThat(result.retries).isEqualTo(1)
    }

    @Test
    fun `execute should accept null error message`() = runTest {
        val job =
            aJob(
                status = JobStatus.ACQUIRED,
                retries = 0,
                maxRetries = null,
                acquiredByWorkerId = workerId,
            )
        gateway.save(job)

        val result = useCase.execute(ReportExecutionErrorCommand(job.id, workerId, null))

        assertThat(result.status).isEqualTo(JobStatus.PENDING)
        assertThat(result.retries).isEqualTo(1)
    }

    @Test
    fun `execute should update updatedAt`() = runTest {
        val originalTime = Instant.ofEpochMilli(1000)
        val job =
            aJob(
                status = JobStatus.ACQUIRED,
                retries = 0,
                maxRetries = null,
                acquiredByWorkerId = workerId,
            )
        gateway.save(job.copy(updatedAt = originalTime))

        val result = useCase.execute(ReportExecutionErrorCommand(job.id, workerId, "error"))

        assertThat(result.updatedAt).isAfter(originalTime)
    }

    @Test
    fun `execute should preserve other job fields`() = runTest {
        val job =
            aJob(
                status = JobStatus.ACQUIRED,
                retries = 0,
                maxRetries = 5,
                acquiredByWorkerId = workerId,
            )
        gateway.save(job)

        val result = useCase.execute(ReportExecutionErrorCommand(job.id, workerId, "error"))

        assertThat(result.id).isEqualTo(job.id)
        assertThat(result.name).isEqualTo(job.name)
        assertThat(result.type).isEqualTo(job.type)
        assertThat(result.maxRetries).isEqualTo(5)
    }

    @Test
    fun `execute should clear acquiredByWorkerId when transitioning to PENDING`() = runTest {
        val job =
            aJob(
                status = JobStatus.ACQUIRED,
                retries = 0,
                maxRetries = null,
                acquiredByWorkerId = workerId,
            )
        gateway.save(job)

        val result = useCase.execute(ReportExecutionErrorCommand(job.id, workerId, "error"))

        assertThat(result.status).isEqualTo(JobStatus.PENDING)
        assertThat(result.acquiredByWorkerId).isNull()
    }

    @Test
    fun `execute should clear acquiredByWorkerId when transitioning to FAILED`() = runTest {
        val job =
            aJob(
                status = JobStatus.ACQUIRED,
                retries = 2,
                maxRetries = 3,
                acquiredByWorkerId = workerId,
            )
        gateway.save(job)

        val result = useCase.execute(ReportExecutionErrorCommand(job.id, workerId, "error"))

        assertThat(result.status).isEqualTo(JobStatus.FAILED)
        assertThat(result.acquiredByWorkerId).isNull()
    }

    // --- Validation ---

    @Test
    fun `execute should throw BlankJobIdException when jobId is blank`() = runTest {
        val exception =
            runCatching { useCase.execute(ReportExecutionErrorCommand("  ", workerId, "error")) }
                .exceptionOrNull()

        assertThat(exception).isInstanceOf(BlankJobIdException::class.java)
    }

    @Test
    fun `execute should throw BlankWorkerIdException when workerId is blank`() = runTest {
        val exception =
            runCatching { useCase.execute(ReportExecutionErrorCommand("job-1", "  ", "error")) }
                .exceptionOrNull()

        assertThat(exception).isInstanceOf(BlankWorkerIdException::class.java)
    }

    @Test
    fun `execute should throw JobNotFoundException when job does not exist`() = runTest {
        val exception =
            runCatching {
                    useCase.execute(ReportExecutionErrorCommand("non-existent", workerId, "error"))
                }
                .exceptionOrNull()

        assertThat(exception).isInstanceOf(JobNotFoundException::class.java)
    }

    @Test
    fun `execute should throw JobNotAcquiredException for all non-ACQUIRED statuses`() = runTest {
        for (status in
            listOf(JobStatus.PENDING, JobStatus.FINISHED, JobStatus.FAILED, JobStatus.ABORTED)) {
            val localGateway = InMemoryJobGateway()
            val job = aJob(status = status, retries = 0, maxRetries = null)
            localGateway.save(job)

            val exception =
                runCatching {
                        ReportExecutionErrorUseCaseImpl(localGateway)
                            .execute(ReportExecutionErrorCommand(job.id, workerId, "error"))
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
            val job =
                aJob(
                    status = JobStatus.ACQUIRED,
                    retries = 0,
                    maxRetries = null,
                    acquiredByWorkerId = "other-worker",
                )
            gateway.save(job)

            val exception =
                runCatching {
                        useCase.execute(ReportExecutionErrorCommand(job.id, workerId, "error"))
                    }
                    .exceptionOrNull()

            assertThat(exception).isInstanceOf(JobNotOwnedByWorkerException::class.java)
        }

    // --- Helpers ---

    private fun aJob(
        status: JobStatus,
        retries: Int = 0,
        maxRetries: Int? = null,
        acquiredByWorkerId: String? = null,
    ) =
        Job(
            id = UUID.randomUUID().toString(),
            groupId = "test-group",
            name = "test-job",
            type = "test-type",
            status = status,
            retries = retries,
            maxRetries = maxRetries,
            idempotencyKey = UUID.randomUUID().toString(),
            createdAt = Instant.now(),
            updatedAt = Instant.now(),
            acquiredByWorkerId = acquiredByWorkerId,
            version = 1,
        )
}
