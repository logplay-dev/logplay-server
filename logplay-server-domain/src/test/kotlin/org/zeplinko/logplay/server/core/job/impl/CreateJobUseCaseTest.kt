package org.zeplinko.logplay.server.core.job.impl

import java.util.UUID
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.zeplinko.logplay.server.core.fakes.InMemoryUnitOfWork
import org.zeplinko.logplay.server.core.job.*
import org.zeplinko.logplay.server.core.job.fakes.InMemoryJobGateway

class CreateJobUseCaseTest {

    private lateinit var gateway: InMemoryJobGateway
    private lateinit var useCase: CreateJobUseCaseImpl

    @BeforeEach
    fun setUp() {
        gateway = InMemoryJobGateway()
        useCase = CreateJobUseCaseImpl(gateway, InMemoryUnitOfWork())
    }

    // --- Happy path ---

    @Test
    fun `execute should derive the same id when given the same idempotencyKey`() = runTest {
        val job = useCase.execute(aCommand(groupId = "g", idempotencyKey = "my-key"))

        assertThat(job.id).isEqualTo(JobIdGenerator.fromIdempotencyKey("g", "my-key"))
    }

    @Test
    fun `execute should create job with maxRetries`() = runTest {
        val job = useCase.execute(aCommand(maxRetries = 5))

        assertThat(job.maxRetries).isEqualTo(5)
    }

    @Test
    fun `execute should create job with null maxRetries by default`() = runTest {
        val job = useCase.execute(aCommand())

        assertThat(job.maxRetries).isNull()
    }

    // --- Idempotency ---

    @Test
    fun `execute should throw DuplicateIdempotencyKeyException when key already exists`() =
        runTest {
            useCase.execute(aCommand(idempotencyKey = "dup-key"))

            val exception =
                runCatching { useCase.execute(aCommand(idempotencyKey = "dup-key")) }
                    .exceptionOrNull()

            assertThat(exception).isInstanceOf(DuplicateIdempotencyKeyException::class.java)
        }

    @Test
    fun `execute should allow different idempotency keys`() = runTest {
        val job1 = useCase.execute(aCommand(idempotencyKey = "key-1"))
        val job2 = useCase.execute(aCommand(idempotencyKey = "key-2"))

        assertThat(job2.id).isNotEqualTo(job1.id)
    }

    @Test
    fun `execute should allow same idempotency key in different groups`() = runTest {
        val job1 = useCase.execute(aCommand(groupId = "A", idempotencyKey = "K"))
        val job2 = useCase.execute(aCommand(groupId = "B", idempotencyKey = "K"))

        assertThat(job2.groupId).isEqualTo("B")
        // Derived ids must differ when groupId differs even for the same key.
        assertThat(job1.id).isNotEqualTo(job2.id)
    }

    @Test
    fun `execute should derive job id deterministically from groupId and idempotencyKey`() =
        runTest {
            val expected = JobIdGenerator.fromIdempotencyKey("grp", "det-key")

            val job = useCase.execute(aCommand(groupId = "grp", idempotencyKey = "det-key"))

            assertThat(job.id).isEqualTo(expected)
        }

    @Test
    fun `execute should throw BlankIdempotencyKeyException when key is blank`() = runTest {
        val exception =
            runCatching { useCase.execute(aCommand(idempotencyKey = "  ")) }.exceptionOrNull()

        assertThat(exception).isInstanceOf(BlankIdempotencyKeyException::class.java)
    }

    @Test
    fun `execute should throw InvalidIdempotencyKeyException when key exceeds 64 characters`() =
        runTest {
            val exception =
                runCatching { useCase.execute(aCommand(idempotencyKey = "a".repeat(65))) }
                    .exceptionOrNull()

            assertThat(exception).isInstanceOf(InvalidIdempotencyKeyException::class.java)
        }

    @Test
    fun `execute should accept idempotency key with exactly 64 characters`() = runTest {
        val exactKey = "a".repeat(64)
        val job = useCase.execute(aCommand(groupId = "g", idempotencyKey = exactKey))

        assertThat(job.id).isEqualTo(JobIdGenerator.fromIdempotencyKey("g", exactKey))
    }

    // --- Group ID validation ---

    @Test
    fun `execute should throw BlankGroupIdException when groupId is blank`() = runTest {
        val exception1 = runCatching { useCase.execute(aCommand(groupId = "")) }.exceptionOrNull()
        val exception2 = runCatching { useCase.execute(aCommand(groupId = "  ")) }.exceptionOrNull()

        assertThat(exception1).isInstanceOf(BlankGroupIdException::class.java)
        assertThat(exception2).isInstanceOf(BlankGroupIdException::class.java)
    }

    @Test
    fun `execute should throw InvalidGroupIdException when groupId exceeds 64 characters`() =
        runTest {
            val exception =
                runCatching { useCase.execute(aCommand(groupId = "a".repeat(65))) }
                    .exceptionOrNull()

            assertThat(exception).isInstanceOf(InvalidGroupIdException::class.java)
        }

    // --- Validation ---

    @Test
    fun `execute should throw InvalidJobTypeException when type exceeds 512 characters`() =
        runTest {
            val exception =
                runCatching { useCase.execute(aCommand(type = "a".repeat(513))) }.exceptionOrNull()

            assertThat(exception).isInstanceOf(InvalidJobTypeException::class.java)
        }

    @Test
    fun `execute should accept type with exactly 512 characters`() = runTest {
        val exactType = "a".repeat(512)
        val job = useCase.execute(aCommand(type = exactType))

        assertThat(job.type).isEqualTo(exactType)
    }

    @Test
    fun `execute should throw InvalidJobNameException when name exceeds 256 characters`() =
        runTest {
            val exception =
                runCatching { useCase.execute(aCommand(name = "a".repeat(257))) }.exceptionOrNull()

            assertThat(exception).isInstanceOf(InvalidJobNameException::class.java)
        }

    @Test
    fun `execute should accept name with exactly 256 characters`() = runTest {
        val exactName = "a".repeat(256)
        val job = useCase.execute(aCommand(name = exactName))

        assertThat(job.name).isEqualTo(exactName)
    }

    @Test
    fun `execute should throw InvalidMaxRetriesException when maxRetries is zero`() = runTest {
        val exception = runCatching { useCase.execute(aCommand(maxRetries = 0)) }.exceptionOrNull()

        assertThat(exception).isInstanceOf(InvalidMaxRetriesException::class.java)
    }

    @Test
    fun `execute should throw InvalidMaxRetriesException when maxRetries is negative`() = runTest {
        val exception = runCatching { useCase.execute(aCommand(maxRetries = -1)) }.exceptionOrNull()

        assertThat(exception).isInstanceOf(InvalidMaxRetriesException::class.java)
    }

    // --- Helpers ---

    private fun aCommand(
        groupId: String = "test-group",
        name: String = "job",
        type: String = "render",
        maxRetries: Int? = null,
        idempotencyKey: String = UUID.randomUUID().toString(),
    ) =
        CreateJobCommand(
            groupId = groupId,
            name = name,
            type = type,
            maxRetries = maxRetries,
            idempotencyKey = idempotencyKey,
        )
}
