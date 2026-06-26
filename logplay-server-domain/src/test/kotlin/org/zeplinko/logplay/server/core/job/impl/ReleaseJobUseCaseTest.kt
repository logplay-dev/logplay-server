package org.zeplinko.logplay.server.core.job.impl

import java.time.Instant
import java.util.UUID
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.zeplinko.logplay.server.core.fakes.InMemoryUnitOfWork
import org.zeplinko.logplay.server.core.job.*
import org.zeplinko.logplay.server.core.job.fakes.InMemoryJobGateway
import org.zeplinko.logplay.server.core.worker.BlankWorkerIdException

class ReleaseJobUseCaseTest {

    private lateinit var gateway: InMemoryJobGateway
    private lateinit var useCase: ReleaseJobUseCaseImpl

    private val workerId = "worker-1"

    @BeforeEach
    fun setUp() {
        gateway = InMemoryJobGateway()
        useCase = ReleaseJobUseCaseImpl(gateway, InMemoryUnitOfWork())
    }

    // --- Happy path ---

    @Test
    fun `execute should transition an ACQUIRED job to PENDING`() = runTest {
        val job = aJob(status = JobStatus.ACQUIRED, acquiredByWorkerId = workerId)
        gateway.save(job)

        val result = useCase.execute(ReleaseJobCommand(job.id, workerId))

        assertThat(result.id).isEqualTo(job.id)
        assertThat(result.status).isEqualTo(JobStatus.PENDING)
    }

    @Test
    fun `execute should clear acquiredByWorkerId`() = runTest {
        val job = aJob(status = JobStatus.ACQUIRED, acquiredByWorkerId = workerId)
        gateway.save(job)

        val result = useCase.execute(ReleaseJobCommand(job.id, workerId))

        assertThat(result.acquiredByWorkerId).isNull()
    }

    @Test
    fun `execute should update the updatedAt timestamp`() = runTest {
        val job = aJob(status = JobStatus.ACQUIRED, acquiredByWorkerId = workerId)
        gateway.save(job)

        val result = useCase.execute(ReleaseJobCommand(job.id, workerId))

        assertThat(result.updatedAt).isAfterOrEqualTo(job.updatedAt)
    }

    @Test
    fun `execute should preserve all other job fields`() = runTest {
        val job = aJob(status = JobStatus.ACQUIRED, acquiredByWorkerId = workerId)
        gateway.save(job)

        val result = useCase.execute(ReleaseJobCommand(job.id, workerId))

        assertThat(result.name).isEqualTo(job.name)
        assertThat(result.type).isEqualTo(job.type)
        assertThat(result.retries).isEqualTo(job.retries)
        assertThat(result.createdAt).isEqualTo(job.createdAt)
    }

    // --- Validation ---

    @Test
    fun `execute should throw BlankJobIdException when jobId is blank`() = runTest {
        val exception =
            runCatching { useCase.execute(ReleaseJobCommand("  ", workerId)) }.exceptionOrNull()

        assertThat(exception).isInstanceOf(BlankJobIdException::class.java)
    }

    @Test
    fun `execute should throw BlankWorkerIdException when workerId is blank`() = runTest {
        val exception =
            runCatching { useCase.execute(ReleaseJobCommand("job-1", "  ")) }.exceptionOrNull()

        assertThat(exception).isInstanceOf(BlankWorkerIdException::class.java)
    }

    // --- State ---

    @Test
    fun `execute should throw JobNotFoundException when job does not exist`() = runTest {
        val exception =
            runCatching { useCase.execute(ReleaseJobCommand("non-existent", workerId)) }
                .exceptionOrNull()

        assertThat(exception).isInstanceOf(JobNotFoundException::class.java)
    }

    @Test
    fun `execute should throw JobNotAcquiredException carrying the actual status`() = runTest {
        for (status in
            listOf(JobStatus.PENDING, JobStatus.FINISHED, JobStatus.FAILED, JobStatus.ABORTED)) {
            val localGateway = InMemoryJobGateway()
            val job = aJob(status = status)
            localGateway.save(job)

            val exception =
                runCatching {
                        ReleaseJobUseCaseImpl(localGateway, InMemoryUnitOfWork())
                            .execute(ReleaseJobCommand(job.id, workerId))
                    }
                    .exceptionOrNull()

            assertThat(exception)
                .describedAs("expected JobNotAcquiredException for status $status")
                .isInstanceOf(JobNotAcquiredException::class.java)
            assertThat((exception as JobNotAcquiredException).status).isEqualTo(status)
        }
    }

    // --- Ownership ---

    @Test
    fun `execute should throw JobNotOwnedByWorkerException when workerId does not match`() =
        runTest {
            val job = aJob(status = JobStatus.ACQUIRED, acquiredByWorkerId = "other-worker")
            gateway.save(job)

            val exception =
                runCatching { useCase.execute(ReleaseJobCommand(job.id, workerId)) }
                    .exceptionOrNull()

            assertThat(exception).isInstanceOf(JobNotOwnedByWorkerException::class.java)
        }

    // --- Helpers ---

    private fun aJob(status: JobStatus, acquiredByWorkerId: String? = null): Job {
        // Truncate to ms so the fixture mirrors what a DB-backed Job carries; assertions on
        // updatedAt ordering would otherwise race the use case's own Instant.now() within the
        // same millisecond (sub-ms nanos make the derived ms value look "earlier").
        val now = Instant.ofEpochMilli(Instant.now().toEpochMilli())
        return Job(
            id = UUID.randomUUID().toString(),
            groupId = "test-group",
            name = "test-job",
            type = "test-type",
            status = status,
            retries = 0,
            createdAt = now,
            updatedAt = now,
            acquiredByWorkerId = acquiredByWorkerId,
            lastAcquiredAt = if (status == JobStatus.ACQUIRED) now else null,
        )
    }
}
