package dev.logplay.server.core.job.impl

import dev.logplay.server.core.job.*
import dev.logplay.server.core.job.fakes.InMemoryJobGateway
import dev.logplay.server.core.worker.BlankWorkerIdException
import java.time.Instant
import java.util.*
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class SaveJobCheckpointUseCaseTest {

    private lateinit var gateway: InMemoryJobGateway
    private lateinit var useCase: SaveJobCheckpointUseCaseImpl

    private val workerId = "worker-1"

    @BeforeEach
    fun setUp() {
        gateway = InMemoryJobGateway()
        useCase = SaveJobCheckpointUseCaseImpl(gateway)
    }

    // --- Happy path ---

    @Test
    fun `execute should return checkpoint with all fields mapped from command`() = runTest {
        val job = aJob(status = JobStatus.ACQUIRED, acquiredByWorkerId = workerId)
        gateway.save(job)

        val checkpoint =
            useCase.execute(
                SaveJobCheckpointCommand(
                    jobId = job.id,
                    workerId = workerId,
                    previousCheckpointId = null,
                    name = "payment processed",
                    data = "payload".toByteArray(),
                )
            )

        assertThat(checkpoint.jobId).isEqualTo(job.id)
        assertThat(checkpoint.previousCheckpointId).isNull()
        assertThat(checkpoint.name).isEqualTo("payment processed")
        assertThat(checkpoint.data).isEqualTo("payload".toByteArray())
        assertThat(checkpoint.createdAt).isNotNull()
        assertThat(checkpoint.orderKey).isEqualTo(1)
    }

    @Test
    fun `execute should accept a null name`() = runTest {
        val job = aJob(status = JobStatus.ACQUIRED, acquiredByWorkerId = workerId)
        gateway.save(job)

        val checkpoint =
            useCase.execute(SaveJobCheckpointCommand(job.id, workerId, null, null, byteArrayOf()))

        assertThat(checkpoint.name).isNull()
    }

    @Test
    fun `execute should generate deterministic ids from chain position`() = runTest {
        val job = aJob(status = JobStatus.ACQUIRED, acquiredByWorkerId = workerId)
        gateway.save(job)

        val first =
            useCase.execute(
                SaveJobCheckpointCommand(job.id, workerId, null, "step-one", byteArrayOf())
            )
        val second =
            useCase.execute(
                SaveJobCheckpointCommand(job.id, workerId, first.id, "step-two", byteArrayOf())
            )

        assertThat(first.id).isEqualTo(CheckpointIdGenerator.fromChainPosition(job.id, null))
        assertThat(second.id).isEqualTo(CheckpointIdGenerator.fromChainPosition(job.id, first.id))
        assertThat(first.id).isNotEqualTo(second.id)
        assertThat(second.previousCheckpointId).isEqualTo(first.id)
        assertThat(first.orderKey).isEqualTo(1)
        assertThat(second.orderKey).isEqualTo(2)
    }

    @Test
    fun `execute should persist the checkpoint`() = runTest {
        val job = aJob(status = JobStatus.ACQUIRED, acquiredByWorkerId = workerId)
        gateway.save(job)

        useCase.execute(SaveJobCheckpointCommand(job.id, workerId, null, null, byteArrayOf()))

        assertThat(gateway.checkpointCount()).isEqualTo(1)
        assertThat(gateway.checkpointsForJob(job.id)).hasSize(1)
    }

    // --- Validation ---

    @Test
    fun `execute should throw BlankJobIdException when jobId is blank`() = runTest {
        val exception =
            runCatching {
                    useCase.execute(
                        SaveJobCheckpointCommand("  ", workerId, null, null, byteArrayOf())
                    )
                }
                .exceptionOrNull()

        assertThat(exception).isInstanceOf(BlankJobIdException::class.java)
        assertThat(gateway.checkpointCount()).isEqualTo(0)
    }

    @Test
    fun `execute should throw BlankWorkerIdException when workerId is blank`() = runTest {
        val exception =
            runCatching {
                    useCase.execute(
                        SaveJobCheckpointCommand("job-id", "  ", null, null, byteArrayOf())
                    )
                }
                .exceptionOrNull()

        assertThat(exception).isInstanceOf(BlankWorkerIdException::class.java)
        assertThat(gateway.checkpointCount()).isEqualTo(0)
    }

    @Test
    fun `execute should throw InvalidCheckpointNameException when name is blank`() = runTest {
        val exception =
            runCatching {
                    useCase.execute(
                        SaveJobCheckpointCommand("job-id", workerId, null, "  ", byteArrayOf())
                    )
                }
                .exceptionOrNull()

        assertThat(exception).isInstanceOf(InvalidCheckpointNameException::class.java)
        assertThat(gateway.checkpointCount()).isEqualTo(0)
    }

    @Test
    fun `execute should throw InvalidCheckpointNameException when name exceeds 256 characters`() =
        runTest {
            val exception =
                runCatching {
                        useCase.execute(
                            SaveJobCheckpointCommand(
                                "job-id",
                                workerId,
                                null,
                                "a".repeat(257),
                                byteArrayOf(),
                            )
                        )
                    }
                    .exceptionOrNull()

            assertThat(exception).isInstanceOf(InvalidCheckpointNameException::class.java)
            assertThat(gateway.checkpointCount()).isEqualTo(0)
        }

    // --- Ordering ---

    @Test
    fun `execute should throw InvalidCheckpointOrderException when previousCheckpointId is non-null but no checkpoints exist`() =
        runTest {
            val job = aJob(status = JobStatus.ACQUIRED, acquiredByWorkerId = workerId)
            gateway.save(job)

            val exception =
                runCatching {
                        useCase.execute(
                            SaveJobCheckpointCommand(
                                job.id,
                                workerId,
                                "non-existent",
                                null,
                                byteArrayOf(),
                            )
                        )
                    }
                    .exceptionOrNull()

            assertThat(exception).isInstanceOf(InvalidCheckpointOrderException::class.java)
            assertThat(gateway.checkpointCount()).isEqualTo(0)
        }

    @Test
    fun `execute should throw InvalidCheckpointOrderException when previousCheckpointId is null but checkpoints exist`() =
        runTest {
            val job = aJob(status = JobStatus.ACQUIRED, acquiredByWorkerId = workerId)
            gateway.save(job)

            useCase.execute(
                SaveJobCheckpointCommand(job.id, workerId, null, "step-one", byteArrayOf())
            )

            val exception =
                runCatching {
                        useCase.execute(
                            SaveJobCheckpointCommand(
                                job.id,
                                workerId,
                                null,
                                "step-two",
                                byteArrayOf(),
                            )
                        )
                    }
                    .exceptionOrNull()

            assertThat(exception).isInstanceOf(InvalidCheckpointOrderException::class.java)
            assertThat(gateway.checkpointCount()).isEqualTo(1)
        }

    @Test
    fun `execute should throw InvalidCheckpointOrderException when previousCheckpointId does not match last checkpoint`() =
        runTest {
            val job = aJob(status = JobStatus.ACQUIRED, acquiredByWorkerId = workerId)
            gateway.save(job)

            useCase.execute(
                SaveJobCheckpointCommand(job.id, workerId, null, "step-one", byteArrayOf())
            )

            val exception =
                runCatching {
                        useCase.execute(
                            SaveJobCheckpointCommand(
                                job.id,
                                workerId,
                                "wrong-id",
                                "step-two",
                                byteArrayOf(),
                            )
                        )
                    }
                    .exceptionOrNull()

            assertThat(exception).isInstanceOf(InvalidCheckpointOrderException::class.java)
            assertThat(gateway.checkpointCount()).isEqualTo(1)
        }

    // --- Retry reset ---

    @Test
    fun `execute should reset retries to zero and increment version when job has retries greater than zero`() =
        runTest {
            val job =
                aJob(status = JobStatus.ACQUIRED, acquiredByWorkerId = workerId).copy(retries = 3)
            gateway.save(job)

            useCase.execute(SaveJobCheckpointCommand(job.id, workerId, null, null, byteArrayOf()))

            val updated = gateway.findJobById(job.id)!!
            assertThat(updated.retries).isEqualTo(0)
            assertThat(updated.version).isEqualTo(job.version + 1)
        }

    @Test
    fun `execute should keep retries at zero and increment version when retries are already zero`() =
        runTest {
            val job = aJob(status = JobStatus.ACQUIRED, acquiredByWorkerId = workerId)
            gateway.save(job)

            useCase.execute(SaveJobCheckpointCommand(job.id, workerId, null, null, byteArrayOf()))

            val updated = gateway.findJobById(job.id)!!
            assertThat(updated.retries).isEqualTo(0)
            assertThat(updated.version).isEqualTo(job.version + 1)
        }

    // --- State ---

    @Test
    fun `execute should throw JobNotFoundException when job does not exist`() = runTest {
        val exception =
            runCatching {
                    useCase.execute(
                        SaveJobCheckpointCommand(
                            "non-existent",
                            workerId,
                            null,
                            null,
                            byteArrayOf(),
                        )
                    )
                }
                .exceptionOrNull()

        assertThat(exception).isInstanceOf(JobNotFoundException::class.java)
        assertThat(gateway.checkpointCount()).isEqualTo(0)
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
                        SaveJobCheckpointUseCaseImpl(localGateway)
                            .execute(
                                SaveJobCheckpointCommand(
                                    job.id,
                                    workerId,
                                    null,
                                    null,
                                    byteArrayOf(),
                                )
                            )
                    }
                    .exceptionOrNull()

            assertThat(exception)
                .describedAs("expected JobNotAcquiredException for status $status")
                .isInstanceOf(JobNotAcquiredException::class.java)
            assertThat(localGateway.checkpointCount()).isEqualTo(0)
        }
    }

    // --- Ownership ---

    @Test
    fun `execute should throw JobNotOwnedByWorkerException when workerId does not match`() =
        runTest {
            val job = aJob(status = JobStatus.ACQUIRED, acquiredByWorkerId = "other-worker")
            gateway.save(job)

            val exception =
                runCatching {
                        useCase.execute(
                            SaveJobCheckpointCommand(job.id, workerId, null, null, byteArrayOf())
                        )
                    }
                    .exceptionOrNull()

            assertThat(exception).isInstanceOf(JobNotOwnedByWorkerException::class.java)
            assertThat(gateway.checkpointCount()).isEqualTo(0)
        }

    // --- Helpers ---

    private fun aJob(status: JobStatus, acquiredByWorkerId: String? = null) =
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
            acquiredByWorkerId = acquiredByWorkerId,
            version = 1,
        )
}
