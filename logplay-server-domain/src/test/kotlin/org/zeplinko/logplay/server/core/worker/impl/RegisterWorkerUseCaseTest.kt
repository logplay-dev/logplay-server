package org.zeplinko.logplay.server.core.worker.impl

import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.zeplinko.logplay.server.core.worker.*
import org.zeplinko.logplay.server.core.worker.fakes.InMemoryWorkerGateway

class RegisterWorkerUseCaseTest {

    private lateinit var workerGateway: InMemoryWorkerGateway
    private lateinit var useCase: RegisterWorkerUseCaseImpl

    @BeforeEach
    fun setUp() {
        workerGateway = InMemoryWorkerGateway()
        useCase = RegisterWorkerUseCaseImpl(workerGateway)
    }

    // --- Happy path ---

    @Test
    fun `execute should register a worker and return it`() = runTest {
        val command =
            RegisterWorkerCommand("worker-1", heartbeatTimeout = 5000, sessionTimeout = 15000)

        val result = useCase.execute(command)

        assertThat(result.id).isEqualTo("worker-1")
        assertThat(result.heartbeatTimeout).isEqualTo(5000)
        assertThat(result.sessionTimeout).isEqualTo(15000)
        assertThat(result.lastHeartbeatAt).isNotNull()
        assertThat(result.registeredAt).isNotNull()
    }

    @Test
    fun `execute should persist the worker`() = runTest {
        val command =
            RegisterWorkerCommand("worker-1", heartbeatTimeout = 5000, sessionTimeout = 15000)

        useCase.execute(command)

        assertThat(workerGateway.count()).isEqualTo(1)
        assertThat(workerGateway.findWorkerById("worker-1")).isNotNull()
    }

    // --- Validation ---

    @Test
    fun `execute should throw BlankWorkerIdException when workerId is blank`() = runTest {
        val exception =
            runCatching {
                    useCase.execute(
                        RegisterWorkerCommand("  ", heartbeatTimeout = 5000, sessionTimeout = 15000)
                    )
                }
                .exceptionOrNull()

        assertThat(exception).isInstanceOf(BlankWorkerIdException::class.java)
        assertThat(workerGateway.count()).isEqualTo(0)
    }

    @Test
    fun `execute should throw InvalidWorkerIdException when workerId exceeds 64 characters`() =
        runTest {
            val longId = "a".repeat(65)
            val exception =
                runCatching {
                        useCase.execute(
                            RegisterWorkerCommand(
                                longId,
                                heartbeatTimeout = 5000,
                                sessionTimeout = 15000,
                            )
                        )
                    }
                    .exceptionOrNull()

            assertThat(exception).isInstanceOf(InvalidWorkerIdException::class.java)
            assertThat(workerGateway.count()).isEqualTo(0)
        }

    @Test
    fun `execute should accept workerId with exactly 64 characters`() = runTest {
        val exactId = "a".repeat(64)
        val result =
            useCase.execute(
                RegisterWorkerCommand(exactId, heartbeatTimeout = 5000, sessionTimeout = 15000)
            )

        assertThat(result.id).isEqualTo(exactId)
    }

    @Test
    fun `execute should throw InvalidWorkerTimeoutException when heartbeatTimeout is zero`() =
        runTest {
            val exception =
                runCatching {
                        useCase.execute(
                            RegisterWorkerCommand(
                                "worker-1",
                                heartbeatTimeout = 0,
                                sessionTimeout = 15000,
                            )
                        )
                    }
                    .exceptionOrNull()

            assertThat(exception).isInstanceOf(InvalidWorkerTimeoutException::class.java)
            assertThat(workerGateway.count()).isEqualTo(0)
        }

    @Test
    fun `execute should throw InvalidWorkerTimeoutException when heartbeatTimeout is negative`() =
        runTest {
            val exception =
                runCatching {
                        useCase.execute(
                            RegisterWorkerCommand(
                                "worker-1",
                                heartbeatTimeout = -1,
                                sessionTimeout = 15000,
                            )
                        )
                    }
                    .exceptionOrNull()

            assertThat(exception).isInstanceOf(InvalidWorkerTimeoutException::class.java)
        }

    @Test
    fun `execute should throw InvalidWorkerTimeoutException when sessionTimeout is zero`() =
        runTest {
            val exception =
                runCatching {
                        useCase.execute(
                            RegisterWorkerCommand(
                                "worker-1",
                                heartbeatTimeout = 5000,
                                sessionTimeout = 0,
                            )
                        )
                    }
                    .exceptionOrNull()

            assertThat(exception).isInstanceOf(InvalidWorkerTimeoutException::class.java)
        }

    @Test
    fun `execute should throw InvalidWorkerTimeoutException when sessionTimeout is not greater than heartbeatTimeout`() =
        runTest {
            val exception =
                runCatching {
                        useCase.execute(
                            RegisterWorkerCommand(
                                "worker-1",
                                heartbeatTimeout = 5000,
                                sessionTimeout = 5000,
                            )
                        )
                    }
                    .exceptionOrNull()

            assertThat(exception).isInstanceOf(InvalidWorkerTimeoutException::class.java)
        }

    @Test
    fun `execute should throw WorkerAlreadyRegisteredException when worker already exists`() =
        runTest {
            useCase.execute(
                RegisterWorkerCommand("worker-1", heartbeatTimeout = 5000, sessionTimeout = 15000)
            )

            val exception =
                runCatching {
                        useCase.execute(
                            RegisterWorkerCommand(
                                "worker-1",
                                heartbeatTimeout = 5000,
                                sessionTimeout = 15000,
                            )
                        )
                    }
                    .exceptionOrNull()

            assertThat(exception).isInstanceOf(WorkerAlreadyRegisteredException::class.java)
        }
}
