package org.zeplinko.logplay.server.core.job.impl

import java.time.Instant
import java.util.*
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.zeplinko.logplay.server.core.job.*
import org.zeplinko.logplay.server.core.job.fakes.InMemoryJobGateway
import org.zeplinko.logplay.server.core.worker.BlankWorkerIdException

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
    fun `execute should accept null data and persist it as null`() = runTest {
        val job = aJob(status = JobStatus.ACQUIRED, acquiredByWorkerId = workerId)
        gateway.save(job)

        val checkpoint =
            useCase.execute(SaveJobCheckpointCommand(job.id, workerId, null, "marker", null))

        assertThat(checkpoint.data).isNull()
        assertThat(checkpoint.name).isEqualTo("marker")
        assertThat(gateway.checkpointsForJob(job.id))
            .singleElement()
            .satisfies({ assertThat(it.data).isNull() })
    }

    @Test
    fun `execute should preserve empty byte array distinctly from null`() = runTest {
        val job = aJob(status = JobStatus.ACQUIRED, acquiredByWorkerId = workerId)
        gateway.save(job)

        val checkpoint =
            useCase.execute(SaveJobCheckpointCommand(job.id, workerId, null, null, byteArrayOf()))

        assertThat(checkpoint.data).isNotNull()
        assertThat(checkpoint.data).isEmpty()
    }

    @Test
    fun `execute should support a mixed chain of null and non-null data checkpoints`() = runTest {
        val job = aJob(status = JobStatus.ACQUIRED, acquiredByWorkerId = workerId)
        gateway.save(job)

        val first = useCase.execute(SaveJobCheckpointCommand(job.id, workerId, null, "a", null))
        val second =
            useCase.execute(
                SaveJobCheckpointCommand(job.id, workerId, first.id, "b", "payload".toByteArray())
            )
        val third =
            useCase.execute(SaveJobCheckpointCommand(job.id, workerId, second.id, "c", null))

        assertThat(first.data).isNull()
        assertThat(second.data).isEqualTo("payload".toByteArray())
        assertThat(third.data).isNull()
        assertThat(first.orderKey).isEqualTo(1)
        assertThat(second.orderKey).isEqualTo(2)
        assertThat(third.orderKey).isEqualTo(3)
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
    fun `execute should reset retries to zero when job has retries greater than zero`() = runTest {
        val job = aJob(status = JobStatus.ACQUIRED, acquiredByWorkerId = workerId).copy(retries = 3)
        gateway.save(job)

        useCase.execute(SaveJobCheckpointCommand(job.id, workerId, null, null, byteArrayOf()))

        val updated = gateway.findJobById(job.id)!!
        assertThat(updated.retries).isEqualTo(0)
    }

    @Test
    fun `execute should keep retries at zero when retries are already zero`() = runTest {
        val job = aJob(status = JobStatus.ACQUIRED, acquiredByWorkerId = workerId)
        gateway.save(job)

        useCase.execute(SaveJobCheckpointCommand(job.id, workerId, null, null, byteArrayOf()))

        val updated = gateway.findJobById(job.id)!!
        assertThat(updated.retries).isEqualTo(0)
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
    fun `execute should throw JobNotAcquiredException carrying the actual status`() = runTest {
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
            assertThat((exception as JobNotAcquiredException).status).isEqualTo(status)
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

    @Test
    fun `execute should throw and not persist when lease is revoked between compute and write`() =
        runTest {
            val job = aJob(status = JobStatus.ACQUIRED, acquiredByWorkerId = workerId)
            gateway.save(job)
            // Simulate a concurrent dead-worker cleanup landing in the race window: after the
            // gateway has computed the new checkpoint but before it commits the retries-reset.
            gateway.onAfterCheckpointCompute = {
                kotlinx.coroutines.runBlocking {
                    gateway.releaseJobsByWorkerId(workerId, Instant.now()) { jobId ->
                        JobEvent(
                            id = UUID.randomUUID().toString(),
                            jobId = jobId,
                            eventType = JobEventType.RELEASED,
                            actorType = ActorType.SYSTEM,
                            actorId = null,
                            createdAt = Instant.now(),
                            eventMessage = "test concurrent release",
                            eventDetail = null,
                        )
                    }
                }
            }

            val exception =
                runCatching {
                        useCase.execute(
                            SaveJobCheckpointCommand(job.id, workerId, null, "step", byteArrayOf())
                        )
                    }
                    .exceptionOrNull()

            // Job is now PENDING after the release; the gateway re-classifies the post-compute
            // state and surfaces it as JobNotAcquiredException with the actual PENDING status.
            assertThat(exception).isInstanceOf(JobNotAcquiredException::class.java)
            assertThat((exception as JobNotAcquiredException).status).isEqualTo(JobStatus.PENDING)
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
            createdAt = Instant.now(),
            updatedAt = Instant.now(),
            acquiredByWorkerId = acquiredByWorkerId,
            lastAcquiredAt = if (status == JobStatus.ACQUIRED) Instant.now() else null,
        )
}
