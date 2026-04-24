package dev.logplay.server.core.job.impl

import dev.logplay.server.core.job.*
import dev.logplay.server.core.job.fakes.InMemoryJobGateway
import java.time.Instant
import java.util.UUID
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class AbortJobUseCaseTest {

    private lateinit var gateway: InMemoryJobGateway
    private lateinit var useCase: AbortJobUseCaseImpl

    @BeforeEach
    fun setUp() {
        gateway = InMemoryJobGateway()
        useCase = AbortJobUseCaseImpl(gateway)
    }

    // --- Happy path ---

    @Test
    fun `execute should abort a PENDING job`() = runTest {
        val job = aJob(JobStatus.PENDING)
        gateway.save(job)

        val result = useCase.execute(AbortJobCommand(job.id))

        assertThat(result.status).isEqualTo(JobStatus.ABORTED)
    }

    @Test
    fun `execute should abort an ACQUIRED job and clear acquiredByWorkerId`() = runTest {
        val job = aJob(JobStatus.ACQUIRED).copy(acquiredByWorkerId = "worker-1")
        gateway.save(job)

        val result = useCase.execute(AbortJobCommand(job.id))

        assertThat(result.status).isEqualTo(JobStatus.ABORTED)
        assertThat(result.acquiredByWorkerId).isNull()
    }

    @Test
    fun `execute should update updatedAt`() = runTest {
        val originalTime = Instant.ofEpochMilli(1000)
        val job = aJob(JobStatus.PENDING).copy(updatedAt = originalTime)
        gateway.save(job)

        val result = useCase.execute(AbortJobCommand(job.id))

        assertThat(result.updatedAt).isAfter(originalTime)
    }

    @Test
    fun `execute should preserve other job fields`() = runTest {
        val job = aJob(JobStatus.ACQUIRED)
        gateway.save(job)

        val result = useCase.execute(AbortJobCommand(job.id))

        assertThat(result.id).isEqualTo(job.id)
        assertThat(result.name).isEqualTo(job.name)
        assertThat(result.type).isEqualTo(job.type)
        assertThat(result.retries).isEqualTo(job.retries)
    }

    // --- Validation ---

    @Test
    fun `execute should throw BlankJobIdException when jobId is blank`() = runTest {
        val exception = runCatching { useCase.execute(AbortJobCommand("  ")) }.exceptionOrNull()

        assertThat(exception).isInstanceOf(BlankJobIdException::class.java)
    }

    @Test
    fun `execute should throw JobNotFoundException when job does not exist`() = runTest {
        val exception =
            runCatching { useCase.execute(AbortJobCommand("non-existent")) }.exceptionOrNull()

        assertThat(exception).isInstanceOf(JobNotFoundException::class.java)
    }

    @Test
    fun `execute should throw JobNotAbortableException for terminal statuses`() = runTest {
        for (status in listOf(JobStatus.FINISHED, JobStatus.FAILED, JobStatus.ABORTED)) {
            val localGateway = InMemoryJobGateway()
            val job = aJob(status)
            localGateway.save(job)

            val exception =
                runCatching { AbortJobUseCaseImpl(localGateway).execute(AbortJobCommand(job.id)) }
                    .exceptionOrNull()

            assertThat(exception)
                .describedAs("expected JobNotAbortableException for status $status")
                .isInstanceOf(JobNotAbortableException::class.java)
        }
    }

    // --- Helpers ---

    private fun aJob(status: JobStatus) =
        Job(
            id = UUID.randomUUID().toString(),
            groupId = "test-group",
            name = "test-job",
            type = "test-type",
            status = status,
            retries = 0,
            idempotencyKey = UUID.randomUUID().toString(),
            createdAt = Instant.now(),
            updatedAt = Instant.now(),
            version = 1,
        )
}
