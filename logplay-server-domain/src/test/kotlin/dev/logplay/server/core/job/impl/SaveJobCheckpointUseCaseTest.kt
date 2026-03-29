package dev.logplay.server.core.job.impl

import dev.logplay.server.core.job.*
import dev.logplay.server.core.job.fakes.InMemoryJobGateway
import java.time.Instant
import java.util.UUID
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class SaveJobCheckpointUseCaseTest {

    private lateinit var gateway: InMemoryJobGateway
    private lateinit var useCase: SaveJobCheckpointUseCaseImpl

    @BeforeEach
    fun setUp() {
        gateway = InMemoryJobGateway()
        useCase = SaveJobCheckpointUseCaseImpl(gateway)
    }

    // --- Happy path ---

    @Test
    fun `execute should return checkpoint with all fields mapped from command`() = runTest {
        val job = aJob(status = JobStatus.PENDING)
        gateway.save(job)

        val checkpoint =
            useCase.execute(
                SaveJobCheckpointCommand(
                    jobId = job.id,
                    classType = "com.example.PaymentResult",
                    description = "payment processed",
                    data = "payload".toByteArray(),
                )
            )

        assertThat(checkpoint.jobId).isEqualTo(job.id)
        assertThat(checkpoint.classType).isEqualTo("com.example.PaymentResult")
        assertThat(checkpoint.description).isEqualTo("payment processed")
        assertThat(checkpoint.data).isEqualTo("payload".toByteArray())
        assertThat(checkpoint.createdAt).isNotNull()
    }

    @Test
    fun `execute should accept a null description`() = runTest {
        val job = aJob(status = JobStatus.PENDING)
        gateway.save(job)

        val checkpoint =
            useCase.execute(
                SaveJobCheckpointCommand(job.id, "com.example.Result", null, byteArrayOf())
            )

        assertThat(checkpoint.description).isNull()
    }

    @Test
    fun `execute should generate a unique id for each checkpoint`() = runTest {
        val job = aJob(status = JobStatus.PENDING)
        gateway.save(job)

        val first =
            useCase.execute(
                SaveJobCheckpointCommand(job.id, "com.example.StepOne", null, byteArrayOf())
            )
        val second =
            useCase.execute(
                SaveJobCheckpointCommand(job.id, "com.example.StepTwo", null, byteArrayOf())
            )

        assertThat(first.id).isNotBlank()
        assertThat(first.id).isNotEqualTo(second.id)
    }

    @Test
    fun `execute should persist the checkpoint`() = runTest {
        val job = aJob(status = JobStatus.PENDING)
        gateway.save(job)

        useCase.execute(SaveJobCheckpointCommand(job.id, "com.example.Result", null, byteArrayOf()))

        assertThat(gateway.checkpointCount()).isEqualTo(1)
        assertThat(gateway.checkpointsForJob(job.id)).hasSize(1)
    }

    // --- Validation ---

    @Test
    fun `execute should throw BlankJobIdException when jobId is blank`() = runTest {
        val exception =
            runCatching {
                    useCase.execute(
                        SaveJobCheckpointCommand("  ", "com.example.Result", null, byteArrayOf())
                    )
                }
                .exceptionOrNull()

        assertThat(exception).isInstanceOf(BlankJobIdException::class.java)
        assertThat(gateway.checkpointCount()).isEqualTo(0)
    }

    @Test
    fun `execute should throw BlankCheckpointClassTypeException when classType is blank`() =
        runTest {
            val exception =
                runCatching {
                        useCase.execute(
                            SaveJobCheckpointCommand("job-id", "  ", null, byteArrayOf())
                        )
                    }
                    .exceptionOrNull()

            assertThat(exception).isInstanceOf(BlankCheckpointClassTypeException::class.java)
            assertThat(gateway.checkpointCount()).isEqualTo(0)
        }

    @Test
    fun `execute should throw BlankCheckpointDescriptionException when description is a blank string`() =
        runTest {
            val exception =
                runCatching {
                        useCase.execute(
                            SaveJobCheckpointCommand(
                                "job-id",
                                "com.example.Result",
                                "  ",
                                byteArrayOf(),
                            )
                        )
                    }
                    .exceptionOrNull()

            assertThat(exception).isInstanceOf(BlankCheckpointDescriptionException::class.java)
            assertThat(gateway.checkpointCount()).isEqualTo(0)
        }

    // --- State ---

    @Test
    fun `execute should throw JobNotFoundException when job does not exist`() = runTest {
        val exception =
            runCatching {
                    useCase.execute(
                        SaveJobCheckpointCommand(
                            "non-existent",
                            "com.example.Result",
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
    fun `execute should throw JobNotPendingException for all non-PENDING statuses`() = runTest {
        for (status in listOf(JobStatus.FINISHED, JobStatus.FAILED, JobStatus.ABORTED)) {
            val localGateway = InMemoryJobGateway()
            val job = aJob(status = status)
            localGateway.save(job)

            val exception =
                runCatching {
                        SaveJobCheckpointUseCaseImpl(localGateway)
                            .execute(
                                SaveJobCheckpointCommand(
                                    job.id,
                                    "com.example.Result",
                                    null,
                                    byteArrayOf(),
                                )
                            )
                    }
                    .exceptionOrNull()

            assertThat(exception)
                .describedAs("expected JobNotPendingException for status $status")
                .isInstanceOf(JobNotPendingException::class.java)
            assertThat(localGateway.checkpointCount()).isEqualTo(0)
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
