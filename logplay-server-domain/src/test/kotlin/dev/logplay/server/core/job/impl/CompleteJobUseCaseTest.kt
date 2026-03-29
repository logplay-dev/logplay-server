package dev.logplay.server.core.job.impl

import dev.logplay.server.core.job.*
import dev.logplay.server.core.job.fakes.InMemoryJobGateway
import java.time.Instant
import java.util.UUID
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class CompleteJobUseCaseTest {

    private lateinit var gateway: InMemoryJobGateway
    private lateinit var useCase: CompleteJobUseCaseImpl

    @BeforeEach
    fun setUp() {
        gateway = InMemoryJobGateway()
        useCase = CompleteJobUseCaseImpl(gateway)
    }

    // --- Happy path ---

    @Test
    fun `execute should transition a PENDING job to FINISHED`() = runTest {
        val job = aJob(status = JobStatus.PENDING)
        gateway.save(job)

        val result = useCase.execute(CompleteJobCommand(job.id))

        assertThat(result.id).isEqualTo(job.id)
        assertThat(result.status).isEqualTo(JobStatus.FINISHED)
    }

    @Test
    fun `execute should update the updatedAt timestamp`() = runTest {
        val job = aJob(status = JobStatus.PENDING)
        gateway.save(job)

        val result = useCase.execute(CompleteJobCommand(job.id))

        assertThat(result.updatedAt).isAfterOrEqualTo(job.updatedAt)
    }

    @Test
    fun `execute should preserve all other job fields`() = runTest {
        val job = aJob(status = JobStatus.PENDING)
        gateway.save(job)

        val result = useCase.execute(CompleteJobCommand(job.id))

        assertThat(result.name).isEqualTo(job.name)
        assertThat(result.type).isEqualTo(job.type)
        assertThat(result.retries).isEqualTo(job.retries)
        assertThat(result.createdAt).isEqualTo(job.createdAt)
        assertThat(result.version).isEqualTo(job.version)
    }

    // --- Validation ---

    @Test
    fun `execute should throw BlankJobIdException when jobId is blank`() = runTest {
        val exception = runCatching { useCase.execute(CompleteJobCommand("  ")) }.exceptionOrNull()

        assertThat(exception).isInstanceOf(BlankJobIdException::class.java)
    }

    // --- State ---

    @Test
    fun `execute should throw JobNotFoundException when job does not exist`() = runTest {
        val exception =
            runCatching { useCase.execute(CompleteJobCommand("non-existent")) }.exceptionOrNull()

        assertThat(exception).isInstanceOf(JobNotFoundException::class.java)
    }

    @Test
    fun `execute should throw JobNotPendingException for all non-PENDING statuses`() = runTest {
        for (status in listOf(JobStatus.FINISHED, JobStatus.FAILED, JobStatus.ABORTED)) {
            val localGateway = InMemoryJobGateway()
            val job = aJob(status = status)
            localGateway.save(job)

            val exception =
                runCatching {
                        CompleteJobUseCaseImpl(localGateway).execute(CompleteJobCommand(job.id))
                    }
                    .exceptionOrNull()

            assertThat(exception)
                .describedAs("expected JobNotPendingException for status $status")
                .isInstanceOf(JobNotPendingException::class.java)
        }
    }

    // --- Helpers ---

    private fun aJob(status: JobStatus) =
        Job(
            id = UUID.randomUUID().toString(),
            name = "test-job",
            type = "test-type",
            status = status,
            retries = 0,
            createdAt = Instant.now(),
            updatedAt = Instant.now(),
            version = 1,
        )
}
