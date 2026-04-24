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
import org.zeplinko.logplay.server.core.worker.Worker
import org.zeplinko.logplay.server.core.worker.WorkerCondemnedException
import org.zeplinko.logplay.server.core.worker.WorkerNotFoundException
import org.zeplinko.logplay.server.core.worker.fakes.InMemoryWorkerGateway

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

        val result =
            useCase.execute(AcquirePendingJobsCommand("test-group", "test-type", workerId, 10))

        assertThat(result).hasSize(1)
        assertThat(result[0].status).isEqualTo(JobStatus.ACQUIRED)
        assertThat(result[0].acquiredByWorkerId).isEqualTo(workerId)
    }

    @Test
    fun `execute should return empty list when no pending jobs`() = runTest {
        val result =
            useCase.execute(AcquirePendingJobsCommand("test-group", "test-type", workerId, 10))

        assertThat(result).isEmpty()
    }

    // --- Validation ---

    @Test
    fun `execute should throw BlankWorkerIdException when workerId is blank`() = runTest {
        val exception =
            runCatching {
                    useCase.execute(AcquirePendingJobsCommand("test-group", "test-type", "  ", 10))
                }
                .exceptionOrNull()

        assertThat(exception).isInstanceOf(BlankWorkerIdException::class.java)
    }

    @Test
    fun `execute should throw InvalidLimitException when limit is zero`() = runTest {
        val exception =
            runCatching {
                    useCase.execute(
                        AcquirePendingJobsCommand("test-group", "test-type", workerId, 0)
                    )
                }
                .exceptionOrNull()

        assertThat(exception).isInstanceOf(InvalidLimitException::class.java)
    }

    @Test
    fun `execute should throw InvalidLimitException when limit is negative`() = runTest {
        val exception =
            runCatching {
                    useCase.execute(
                        AcquirePendingJobsCommand("test-group", "test-type", workerId, -1)
                    )
                }
                .exceptionOrNull()

        assertThat(exception).isInstanceOf(InvalidLimitException::class.java)
    }

    @Test
    fun `execute should throw InvalidLimitException when limit exceeds max`() = runTest {
        val exception =
            runCatching {
                    useCase.execute(
                        AcquirePendingJobsCommand("test-group", "test-type", workerId, 101)
                    )
                }
                .exceptionOrNull()

        assertThat(exception).isInstanceOf(InvalidLimitException::class.java)
    }

    @Test
    fun `execute should throw WorkerNotFoundException when worker does not exist`() = runTest {
        val exception =
            runCatching {
                    useCase.execute(
                        AcquirePendingJobsCommand("test-group", "test-type", "non-existent", 10)
                    )
                }
                .exceptionOrNull()

        assertThat(exception).isInstanceOf(WorkerNotFoundException::class.java)
    }

    @Test
    fun `execute should throw WorkerNotFoundException when worker does not exist even with pending jobs`() =
        runTest {
            val job = aJob()
            jobGateway.save(job)

            val exception =
                runCatching {
                        useCase.execute(
                            AcquirePendingJobsCommand("test-group", "test-type", "non-existent", 10)
                        )
                    }
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
                runCatching {
                        useCase.execute(
                            AcquirePendingJobsCommand(
                                "test-group",
                                "test-type",
                                "condemned-worker",
                                10,
                            )
                        )
                    }
                    .exceptionOrNull()

            assertThat(exception).isInstanceOf(WorkerCondemnedException::class.java)
            assertThat(jobGateway.findJobById(job.id)!!.status).isEqualTo(JobStatus.PENDING)
        }

    @Test
    fun `execute should throw WorkerCondemnedException when worker is condemned and no jobs exist`() =
        runTest {
            workerGateway.save(aWorker("condemned-worker").copy(condemned = true))

            val exception =
                runCatching {
                        useCase.execute(
                            AcquirePendingJobsCommand(
                                "test-group",
                                "test-type",
                                "condemned-worker",
                                10,
                            )
                        )
                    }
                    .exceptionOrNull()

            assertThat(exception).isInstanceOf(WorkerCondemnedException::class.java)
        }

    @Test
    fun `execute should throw WorkerNotFoundException when worker does not exist and no jobs exist`() =
        runTest {
            val exception =
                runCatching {
                        useCase.execute(
                            AcquirePendingJobsCommand("test-group", "test-type", "non-existent", 10)
                        )
                    }
                    .exceptionOrNull()

            assertThat(exception).isInstanceOf(WorkerNotFoundException::class.java)
        }

    // --- Type validation ---

    @Test
    fun `execute should throw BlankJobTypeException when type is blank`() = runTest {
        val exception =
            runCatching {
                    useCase.execute(AcquirePendingJobsCommand("test-group", "  ", workerId, 10))
                }
                .exceptionOrNull()

        assertThat(exception).isInstanceOf(BlankJobTypeException::class.java)
    }

    @Test
    fun `execute should only acquire jobs matching the requested type`() = runTest {
        val renderJob1 = aJob(type = "render")
        val renderJob2 = aJob(type = "render")
        val exportJob = aJob(type = "export")
        jobGateway.save(renderJob1)
        jobGateway.save(renderJob2)
        jobGateway.save(exportJob)

        val result =
            useCase.execute(AcquirePendingJobsCommand("test-group", "render", workerId, 10))

        assertThat(result).hasSize(2)
        assertThat(result.map { it.id }).containsExactlyInAnyOrder(renderJob1.id, renderJob2.id)
        assertThat(result).allSatisfy { assertThat(it.status).isEqualTo(JobStatus.ACQUIRED) }
        assertThat(jobGateway.findJobById(exportJob.id)!!.status).isEqualTo(JobStatus.PENDING)
    }

    @Test
    fun `execute should only acquire jobs matching both groupId and type`() = runTest {
        val matchJob = aJob(groupId = "A", type = "render")
        val wrongGroup = aJob(groupId = "B", type = "render")
        val wrongType = aJob(groupId = "A", type = "export")
        jobGateway.save(matchJob)
        jobGateway.save(wrongGroup)
        jobGateway.save(wrongType)

        val result = useCase.execute(AcquirePendingJobsCommand("A", "render", workerId, 10))

        assertThat(result).hasSize(1)
        assertThat(result[0].id).isEqualTo(matchJob.id)
        assertThat(jobGateway.findJobById(wrongGroup.id)!!.status).isEqualTo(JobStatus.PENDING)
        assertThat(jobGateway.findJobById(wrongType.id)!!.status).isEqualTo(JobStatus.PENDING)
    }

    // --- Group ID validation ---

    @Test
    fun `execute should throw BlankGroupIdException when groupId is blank`() = runTest {
        val exception =
            runCatching {
                    useCase.execute(AcquirePendingJobsCommand("  ", "test-type", workerId, 10))
                }
                .exceptionOrNull()

        assertThat(exception).isInstanceOf(BlankGroupIdException::class.java)
    }

    @Test
    fun `execute should only acquire jobs matching the requested groupId`() = runTest {
        val jobA1 = aJob(groupId = "A")
        val jobA2 = aJob(groupId = "A")
        val jobB1 = aJob(groupId = "B")
        jobGateway.save(jobA1)
        jobGateway.save(jobA2)
        jobGateway.save(jobB1)

        val result = useCase.execute(AcquirePendingJobsCommand("A", "test-type", workerId, 10))

        assertThat(result).hasSize(2)
        assertThat(result.map { it.id }).containsExactlyInAnyOrder(jobA1.id, jobA2.id)
        assertThat(result).allSatisfy { assertThat(it.status).isEqualTo(JobStatus.ACQUIRED) }
        assertThat(jobGateway.findJobById(jobB1.id)!!.status).isEqualTo(JobStatus.PENDING)
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

    private fun aJob(groupId: String = "test-group", type: String = "test-type") =
        Job(
            id = UUID.randomUUID().toString(),
            groupId = groupId,
            name = "test-job",
            type = type,
            status = JobStatus.PENDING,
            retries = 0,
            idempotencyKey = UUID.randomUUID().toString(),
            createdAt = Instant.now(),
            updatedAt = Instant.now(),
            version = 0,
        )
}
