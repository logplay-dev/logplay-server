package dev.logplay.server.core.job.impl

import dev.logplay.server.core.job.*
import dev.logplay.server.core.job.fakes.InMemoryJobGateway
import java.time.Instant
import java.util.UUID
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class GetCheckpointsUseCaseTest {

    private lateinit var gateway: InMemoryJobGateway
    private lateinit var useCase: GetCheckpointsUseCaseImpl

    @BeforeEach
    fun setUp() {
        gateway = InMemoryJobGateway()
        useCase = GetCheckpointsUseCaseImpl(gateway)
    }

    // --- Happy path ---

    @Test
    fun `execute should return empty page when no checkpoints exist`() = runTest {
        val job = aJob(JobStatus.ACQUIRED)
        gateway.save(job)

        val page = useCase.execute(GetCheckpointsCommand(job.id, after = null, limit = 20))

        assertThat(page.checkpoints).isEmpty()
        assertThat(page.hasMore).isFalse()
    }

    @Test
    fun `execute should return all checkpoints when fewer than limit`() = runTest {
        val job = aJob(JobStatus.ACQUIRED)
        gateway.save(job)
        val c1 = aCheckpoint(job.id, createdAt = Instant.ofEpochMilli(1000), orderKey = 1)
        val c2 = aCheckpoint(job.id, createdAt = Instant.ofEpochMilli(2000), orderKey = 2)
        gateway.saveTestCheckpoint(c1)
        gateway.saveTestCheckpoint(c2)

        val page = useCase.execute(GetCheckpointsCommand(job.id, after = null, limit = 20))

        assertThat(page.checkpoints).hasSize(2)
        assertThat(page.checkpoints[0].id).isEqualTo(c1.id)
        assertThat(page.checkpoints[1].id).isEqualTo(c2.id)
        assertThat(page.hasMore).isFalse()
    }

    @Test
    fun `execute should return hasMore true when more checkpoints exist`() = runTest {
        val job = aJob(JobStatus.ACQUIRED)
        gateway.save(job)
        val c1 = aCheckpoint(job.id, createdAt = Instant.ofEpochMilli(1000), orderKey = 1)
        val c2 = aCheckpoint(job.id, createdAt = Instant.ofEpochMilli(2000), orderKey = 2)
        val c3 = aCheckpoint(job.id, createdAt = Instant.ofEpochMilli(3000), orderKey = 3)
        gateway.saveTestCheckpoint(c1)
        gateway.saveTestCheckpoint(c2)
        gateway.saveTestCheckpoint(c3)

        val page = useCase.execute(GetCheckpointsCommand(job.id, after = null, limit = 2))

        assertThat(page.checkpoints).hasSize(2)
        assertThat(page.hasMore).isTrue()
    }

    @Test
    fun `execute should return checkpoints after cursor`() = runTest {
        val job = aJob(JobStatus.ACQUIRED)
        gateway.save(job)
        val c1 = aCheckpoint(job.id, createdAt = Instant.ofEpochMilli(1000), orderKey = 1)
        val c2 = aCheckpoint(job.id, createdAt = Instant.ofEpochMilli(2000), orderKey = 2)
        val c3 = aCheckpoint(job.id, createdAt = Instant.ofEpochMilli(3000), orderKey = 3)
        gateway.saveTestCheckpoint(c1)
        gateway.saveTestCheckpoint(c2)
        gateway.saveTestCheckpoint(c3)

        val page = useCase.execute(GetCheckpointsCommand(job.id, after = c1.id, limit = 20))

        assertThat(page.checkpoints).hasSize(2)
        assertThat(page.checkpoints[0].id).isEqualTo(c2.id)
        assertThat(page.checkpoints[1].id).isEqualTo(c3.id)
        assertThat(page.hasMore).isFalse()
    }

    @Test
    fun `execute should paginate with cursor and limit`() = runTest {
        val job = aJob(JobStatus.ACQUIRED)
        gateway.save(job)
        val c1 = aCheckpoint(job.id, createdAt = Instant.ofEpochMilli(1000), orderKey = 1)
        val c2 = aCheckpoint(job.id, createdAt = Instant.ofEpochMilli(2000), orderKey = 2)
        val c3 = aCheckpoint(job.id, createdAt = Instant.ofEpochMilli(3000), orderKey = 3)
        val c4 = aCheckpoint(job.id, createdAt = Instant.ofEpochMilli(4000), orderKey = 4)
        gateway.saveTestCheckpoint(c1)
        gateway.saveTestCheckpoint(c2)
        gateway.saveTestCheckpoint(c3)
        gateway.saveTestCheckpoint(c4)

        val page = useCase.execute(GetCheckpointsCommand(job.id, after = c1.id, limit = 2))

        assertThat(page.checkpoints).hasSize(2)
        assertThat(page.checkpoints[0].id).isEqualTo(c2.id)
        assertThat(page.checkpoints[1].id).isEqualTo(c3.id)
        assertThat(page.hasMore).isTrue()
    }

    @Test
    fun `execute should use default limit when limit is null`() = runTest {
        val job = aJob(JobStatus.ACQUIRED)
        gateway.save(job)
        val checkpoints =
            (1..25).map {
                aCheckpoint(
                    job.id,
                    createdAt = Instant.ofEpochMilli(it.toLong() * 1000),
                    orderKey = it.toLong(),
                )
            }
        checkpoints.forEach { gateway.saveTestCheckpoint(it) }

        val page = useCase.execute(GetCheckpointsCommand(job.id, after = null, limit = null))

        assertThat(page.checkpoints).hasSize(20)
        assertThat(page.hasMore).isTrue()
    }

    @Test
    fun `execute should work for any job status`() = runTest {
        val job = aJob(JobStatus.FINISHED)
        gateway.save(job)
        val c1 = aCheckpoint(job.id, createdAt = Instant.ofEpochMilli(1000), orderKey = 1)
        gateway.saveTestCheckpoint(c1)

        val page = useCase.execute(GetCheckpointsCommand(job.id, after = null, limit = 20))

        assertThat(page.checkpoints).hasSize(1)
    }

    // --- Validation ---

    @Test
    fun `execute should throw BlankJobIdException when jobId is blank`() = runTest {
        val exception =
            runCatching { useCase.execute(GetCheckpointsCommand("  ", after = null, limit = 20)) }
                .exceptionOrNull()

        assertThat(exception).isInstanceOf(BlankJobIdException::class.java)
    }

    @Test
    fun `execute should throw JobNotFoundException when job does not exist`() = runTest {
        val exception =
            runCatching {
                    useCase.execute(GetCheckpointsCommand("non-existent", after = null, limit = 20))
                }
                .exceptionOrNull()

        assertThat(exception).isInstanceOf(JobNotFoundException::class.java)
    }

    @Test
    fun `execute should throw InvalidLimitException when limit is zero`() = runTest {
        val job = aJob(JobStatus.ACQUIRED)
        gateway.save(job)

        val exception =
            runCatching { useCase.execute(GetCheckpointsCommand(job.id, after = null, limit = 0)) }
                .exceptionOrNull()

        assertThat(exception).isInstanceOf(InvalidLimitException::class.java)
    }

    @Test
    fun `execute should throw InvalidLimitException when limit is negative`() = runTest {
        val job = aJob(JobStatus.ACQUIRED)
        gateway.save(job)

        val exception =
            runCatching { useCase.execute(GetCheckpointsCommand(job.id, after = null, limit = -1)) }
                .exceptionOrNull()

        assertThat(exception).isInstanceOf(InvalidLimitException::class.java)
    }

    @Test
    fun `execute should throw InvalidLimitException when limit exceeds max`() = runTest {
        val job = aJob(JobStatus.ACQUIRED)
        gateway.save(job)

        val exception =
            runCatching {
                    useCase.execute(GetCheckpointsCommand(job.id, after = null, limit = 101))
                }
                .exceptionOrNull()

        assertThat(exception).isInstanceOf(InvalidLimitException::class.java)
    }

    @Test
    fun `execute should throw CheckpointNotFoundException when cursor does not exist`() = runTest {
        val job = aJob(JobStatus.ACQUIRED)
        gateway.save(job)

        val exception =
            runCatching {
                    useCase.execute(
                        GetCheckpointsCommand(job.id, after = "non-existent", limit = 20)
                    )
                }
                .exceptionOrNull()

        assertThat(exception).isInstanceOf(CheckpointNotFoundException::class.java)
    }

    @Test
    fun `execute should throw CheckpointNotFoundException when cursor belongs to different job`() =
        runTest {
            val job1 = aJob(JobStatus.ACQUIRED)
            val job2 = aJob(JobStatus.ACQUIRED)
            gateway.save(job1)
            gateway.save(job2)
            val c1 = aCheckpoint(job2.id, createdAt = Instant.ofEpochMilli(1000), orderKey = 1)
            gateway.saveTestCheckpoint(c1)

            val exception =
                runCatching {
                        useCase.execute(GetCheckpointsCommand(job1.id, after = c1.id, limit = 20))
                    }
                    .exceptionOrNull()

            assertThat(exception).isInstanceOf(CheckpointNotFoundException::class.java)
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

    private fun aCheckpoint(jobId: String, createdAt: Instant, orderKey: Long) =
        Checkpoint(
            id = UUID.randomUUID().toString(),
            jobId = jobId,
            previousCheckpointId = null,
            name = null,
            createdAt = createdAt,
            orderKey = orderKey,
            data = "data".toByteArray(),
        )
}
