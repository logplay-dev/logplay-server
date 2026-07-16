package org.zeplinko.logplay.server.test

import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.client.WebClient
import io.vertx.junit5.Timeout
import io.vertx.junit5.VertxExtension
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.coroutines.coAwait
import io.vertx.kotlin.coroutines.dispatcher
import java.sql.Connection
import java.time.Instant
import java.util.Base64
import java.util.UUID
import java.util.concurrent.TimeUnit
import kotlin.time.Duration.Companion.milliseconds
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.*
import org.junit.jupiter.api.extension.ExtendWith
import org.zeplinko.logplay.server.MainVerticle
import org.zeplinko.logplay.server.config.AppConfig
import org.zeplinko.logplay.server.core.UnitOfWork
import org.zeplinko.logplay.server.core.job.JobGateway
import org.zeplinko.logplay.server.core.job.JobIdGenerator
import org.zeplinko.logplay.server.core.worker.WorkerGateway

@ExtendWith(VertxExtension::class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class AbstractIntegrationTest {

    protected abstract fun createBackend(): IntegrationTestBackend

    private lateinit var backend: IntegrationTestBackend
    private lateinit var client: WebClient
    private lateinit var verticle: MainVerticle
    private var port: Int = -1

    // Exposed for gateway-level tests (e.g. the lock-method transaction guard) that need to call a
    // gateway or open a transaction directly, bypassing the HTTP layer.
    private lateinit var jobGateway: JobGateway
    private lateinit var workerGateway: WorkerGateway
    private lateinit var unitOfWork: UnitOfWork

    // Iterations for the concurrency stress guards below. Timing-dependent races reproduce only
    // rarely single-shot, so the looped variants make them reliable. Tunable via the
    // `logplay.stress.iterations` system property (CI can crank it up for deeper soak runs).
    private val stressRaceIterations =
        System.getProperty("logplay.stress.iterations")?.toIntOrNull() ?: 250

    // One reused, autocommit JDBC connection for invariant/consistency assertions. Opening a fresh
    // connection per check (especially on Postgres) dominates the runtime of the looped stress
    // tests; these checks are SELECT-only and are called sequentially from the test coroutine.
    private var assertionConnection: Connection? = null

    private fun assertionConn(): Connection {
        var c = assertionConnection
        if (c == null || c.isClosed) {
            c = backend.getJdbcConnection().also { it.autoCommit = true }
            assertionConnection = c
        }
        return c
    }

    @BeforeAll
    fun setUp(vertx: Vertx, testContext: VertxTestContext) {
        backend = createBackend()
        backend.initDatabase()

        val (jg, wg) = backend.createGateways(vertx)
        jobGateway = jg
        workerGateway = wg
        unitOfWork = backend.createUnitOfWork()
        verticle =
            MainVerticle(
                jobGateway,
                workerGateway,
                unitOfWork,
                AppConfig(httpPort = 0, cleanupIntervalMs = 600_000),
            )
        vertx
            .deployVerticle(verticle)
            .onComplete(
                testContext.succeeding {
                    port = verticle.actualPort
                    client = WebClient.create(vertx)
                    testContext.completeNow()
                }
            )
    }

    @BeforeEach
    fun cleanDb() {
        backend.getJdbcConnection().use { conn ->
            conn.createStatement().use { stmt ->
                // Delete in dependency order: events/checkpoints reference jobs; job_acquired
                // references workers; the queue/acquired tables must be empty before workers can
                // go (workers FK in job_acquired is not ON DELETE CASCADE).
                stmt.execute("DELETE FROM job_events")
                stmt.execute("DELETE FROM checkpoints")
                stmt.execute("DELETE FROM job_acquired")
                stmt.execute("DELETE FROM job_queue")
                stmt.execute("DELETE FROM jobs")
                stmt.execute("DELETE FROM workers")
            }
        }
    }

    @AfterAll
    fun tearDown(vertx: Vertx, testContext: VertxTestContext) {
        assertionConnection?.let { runCatching { it.close() } }
        backend.shutdown()
        vertx.close().onComplete(testContext.succeeding { testContext.completeNow() })
    }

    // --- Happy path ---

    @Test
    fun `should create a job`(vertx: Vertx, testContext: VertxTestContext) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val body = createJob("my-job", "render")
                assertThat(body.getString("id")).isNotBlank()
                assertThat(body.getString("name")).isEqualTo("my-job")
                assertThat(body.getString("type")).isEqualTo("render")
                assertThat(body.getString("status")).isEqualTo("PENDING")
                assertThat(body.getInteger("retries")).isEqualTo(0)
                assertThat(body.getString("createdAt")).isNotBlank()
                assertThat(body.getString("updatedAt")).isNotBlank()
                assertThat(body.getString("lastAcquiredAt")).isNull()
                assertThat(body.getString("acquiredByWorkerId")).isNull()
                assertThat(body.getLong("availableAt")).isEqualTo(0L)
                assertThat(body.getString("terminalAt")).isNull()
                assertThat(body.getString("groupId")).isEqualTo("test-group")
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    // Review #3: findAndLock* take a real row lock, which only lives as long as the enclosing
    // transaction. Called outside one, the lock would be acquired on a throwaway auto-commit
    // connection and released immediately — a silent no-op race. The gateways guard against this by
    // requiring an ambient transaction, so these calls must throw outside one and succeed inside.
    @Test
    fun `findAndLock methods require an open transaction`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val jobId = createJob("lock-guard", "render").getString("id")
                val workerId = registerWorker()

                // Outside a transaction: each lock read must fail loudly, not no-op the lock.
                assertThat(runCatching { jobGateway.findAndLockJobById(jobId) }.exceptionOrNull())
                    .isInstanceOf(IllegalStateException::class.java)
                assertThat(
                        runCatching { workerGateway.findAndLockWorkerById(workerId) }
                            .exceptionOrNull()
                    )
                    .isInstanceOf(IllegalStateException::class.java)
                assertThat(
                        runCatching { workerGateway.findAndLockDeadWorkers(Instant.now()) }
                            .exceptionOrNull()
                    )
                    .isInstanceOf(IllegalStateException::class.java)

                // Inside a transaction: the guard does not false-trip and the locks resolve.
                unitOfWork.transaction {
                    assertThat(jobGateway.findAndLockJobById(jobId)).isNotNull
                    assertThat(workerGateway.findAndLockWorkerById(workerId)).isNotNull
                    workerGateway.findAndLockDeadWorkers(Instant.now())
                }
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should create a job with blank name and get auto-generated name`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val body = createJob("", "render")
                assertThat(body.getString("name")).isNotBlank()
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should acquire pending jobs`(vertx: Vertx, testContext: VertxTestContext) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val workerId = registerWorker()
                createJob("job-1", "render")
                createJob("job-2", "render")
                createJob("job-3", "render")

                val acquired = acquireJobs(workerId, 2)
                assertThat(acquired).hasSize(2)
                for (job in acquired) {
                    assertThat(job.getString("status")).isEqualTo("ACQUIRED")
                    assertThat(job.getString("lastAcquiredAt")).isNotNull()
                    assertThat(job.getString("acquiredByWorkerId")).isEqualTo(workerId)
                }
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should acquire no jobs when none are pending`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val workerId = registerWorker()
                val acquired = acquireJobs(workerId, 5)
                assertThat(acquired).isEmpty()
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `acquire returns the batch ordered by enqueued_at ascending`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val workerId = registerWorker()
                // Create jobs in order; they enter job_queue with ascending enqueued_at matching
                // their (physical) creation order.
                val jobIds =
                    (0 until 5).map {
                        createJob("ord-job-$it", "ordered-acq", idempotencyKey = "ord-$it")
                            .getString("id")
                    }
                // Scramble: rewrite enqueued_at so FIFO order is the REVERSE of creation order
                // (the last-created job is now the oldest). This decouples enqueued_at order from
                // the rows' physical/scan order, exposing a backend that returns the acquired batch
                // in scan order instead of the documented enqueued_at ASC order.
                jobIds.forEachIndexed { i, id -> setEnqueuedAt(id, (jobIds.size - i).toLong()) }
                val expectedOrder = jobIds.reversed()

                val acquiredIds =
                    acquireJobs(workerId, limit = 10, type = "ordered-acq").map {
                        it.getString("id")
                    }

                assertThat(acquiredIds).containsExactlyElementsOf(expectedOrder)
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should save a checkpoint on an acquired job`(vertx: Vertx, testContext: VertxTestContext) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val (job, workerId) = createAndAcquireJob()
                val jobId = job.getString("id")
                val originalData = "hello world"
                val base64Data = Base64.getEncoder().encodeToString(originalData.toByteArray())

                val checkpoint =
                    saveCheckpoint(jobId, workerId, name = "step one", data = base64Data)

                assertThat(checkpoint.getString("id")).isNotBlank()
                assertThat(checkpoint.getString("jobId")).isEqualTo(jobId)
                assertThat(checkpoint.getString("previousCheckpointId")).isNull()
                assertThat(checkpoint.getString("name")).isEqualTo("step one")
                assertThat(checkpoint.getString("createdAt")).isNotBlank()

                val decodedData = String(Base64.getDecoder().decode(checkpoint.getString("data")))
                assertThat(decodedData).isEqualTo(originalData)
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should save a checkpoint with null name`(vertx: Vertx, testContext: VertxTestContext) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val (job, workerId) = createAndAcquireJob()
                val checkpoint = saveCheckpoint(job.getString("id"), workerId, name = null)
                assertThat(checkpoint.getString("name")).isNull()
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should save a checkpoint when data field is omitted from request body`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val (job, workerId) = createAndAcquireJob()
                val checkpoint = saveCheckpoint(job.getString("id"), workerId, includeData = false)
                assertThat(checkpoint.getString("data")).isNull()
                assertThat(checkpoint.getString("id")).isNotBlank()
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should save a checkpoint when data is explicitly null in request body`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val (job, workerId) = createAndAcquireJob()
                val checkpoint = saveCheckpoint(job.getString("id"), workerId, data = null)
                assertThat(checkpoint.getString("data")).isNull()
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should save a checkpoint with empty base64 data and round-trip as empty string`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val (job, workerId) = createAndAcquireJob()
                val checkpoint = saveCheckpoint(job.getString("id"), workerId, data = "")
                assertThat(checkpoint.getString("data")).isEqualTo("")
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should round-trip a mixed chain of null and non-null data through pagination`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val (job, workerId) = createAndAcquireJob()
                val jobId = job.getString("id")
                val payload = Base64.getEncoder().encodeToString("payload".toByteArray())

                val c1 = saveCheckpoint(jobId, workerId, previousCheckpointId = null, data = null)
                val c2 =
                    saveCheckpoint(
                        jobId,
                        workerId,
                        previousCheckpointId = c1.getString("id"),
                        data = payload,
                    )
                val c3 =
                    saveCheckpoint(
                        jobId,
                        workerId,
                        previousCheckpointId = c2.getString("id"),
                        includeData = false,
                    )

                val page = getCheckpoints(jobId)
                val checkpoints = page.getJsonArray("checkpoints")
                assertThat(checkpoints).hasSize(3)
                assertThat(checkpoints.getJsonObject(0).getString("id"))
                    .isEqualTo(c1.getString("id"))
                assertThat(checkpoints.getJsonObject(0).getString("data")).isNull()
                assertThat(checkpoints.getJsonObject(1).getString("id"))
                    .isEqualTo(c2.getString("id"))
                assertThat(checkpoints.getJsonObject(1).getString("data")).isEqualTo(payload)
                assertThat(checkpoints.getJsonObject(2).getString("id"))
                    .isEqualTo(c3.getString("id"))
                assertThat(checkpoints.getJsonObject(2).getString("data")).isNull()
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should reset retries when saving a checkpoint with null data`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val workerId = registerWorker()
                createJob(idempotencyKey = "retry-null-cp", maxRetries = 5)
                var job = acquireJobs(workerId, 1).first()
                val jobId = job.getString("id")

                reportError(jobId, workerId, "boom")
                job = acquireJobs(workerId, 1).first()
                assertThat(job.getInteger("retries")).isEqualTo(1)

                saveCheckpoint(jobId, workerId, includeData = false)

                reportError(jobId, workerId, "boom2")
                job = acquireJobs(workerId, 1).first()
                assertThat(job.getInteger("retries")).isEqualTo(1)
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should complete an acquired job`(vertx: Vertx, testContext: VertxTestContext) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val (job, workerId) = createAndAcquireJob()
                val completed = completeJob(job.getString("id"), workerId)
                assertThat(completed.getString("status")).isEqualTo("FINISHED")
                assertThat(completed.getString("terminalAt")).isNotBlank()
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should release an acquired job back to pending`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val (job, workerId) = createAndAcquireJob()
                val released = releaseJob(job.getString("id"), workerId)
                assertThat(released.getString("status")).isEqualTo("PENDING")
                assertThat(released.getString("acquiredByWorkerId")).isNull()
                assertThat(released.getLong("availableAt")).isEqualTo(0L)
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should complete full lifecycle - create, acquire, checkpoint, complete`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val workerId = registerWorker()
                val created = createJob("lifecycle-job", "render")
                assertThat(created.getString("status")).isEqualTo("PENDING")

                val acquired = acquireJobs(workerId, 1)
                assertThat(acquired).hasSize(1)
                assertThat(acquired[0].getString("status")).isEqualTo("ACQUIRED")

                val jobId = acquired[0].getString("id")
                saveCheckpoint(jobId, workerId)

                val finished = completeJob(jobId, workerId)
                assertThat(finished.getString("status")).isEqualTo("FINISHED")
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should release and re-acquire a job`(vertx: Vertx, testContext: VertxTestContext) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val (job, workerId) = createAndAcquireJob()
                val jobId = job.getString("id")

                releaseJob(jobId, workerId)

                val reacquired = acquireJobs(workerId, 1)
                assertThat(reacquired).hasSize(1)
                assertThat(reacquired[0].getString("id")).isEqualTo(jobId)
                assertThat(reacquired[0].getString("status")).isEqualTo("ACQUIRED")
                assertThat(reacquired[0].getString("acquiredByWorkerId")).isEqualTo(workerId)
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    // --- Checkpoint ordering ---

    @Test
    fun `should save sequential checkpoints with correct chain`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val (job, workerId) = createAndAcquireJob()
                val jobId = job.getString("id")

                val first = saveCheckpoint(jobId, workerId, previousCheckpointId = null)
                assertThat(first.getString("previousCheckpointId")).isNull()

                val second =
                    saveCheckpoint(jobId, workerId, previousCheckpointId = first.getString("id"))
                assertThat(second.getString("previousCheckpointId"))
                    .isEqualTo(first.getString("id"))

                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should return 409 when saving checkpoint with wrong previousCheckpointId`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val (job, workerId) = createAndAcquireJob()
                val jobId = job.getString("id")

                saveCheckpoint(jobId, workerId, previousCheckpointId = null)

                val response =
                    client
                        .post(port, "localhost", "/api/v1/jobs/$jobId/checkpoints")
                        .sendJsonObject(
                            JsonObject()
                                .put("workerId", workerId)
                                .put("previousCheckpointId", "wrong-id")
                                .put(
                                    "data",
                                    Base64.getEncoder().encodeToString("test".toByteArray()),
                                )
                        )
                        .coAwait()
                assertThat(response.statusCode()).isEqualTo(409)
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should return 409 when saving checkpoint with null previousCheckpointId but checkpoints exist`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val (job, workerId) = createAndAcquireJob()
                val jobId = job.getString("id")

                saveCheckpoint(jobId, workerId, previousCheckpointId = null)

                val response =
                    client
                        .post(port, "localhost", "/api/v1/jobs/$jobId/checkpoints")
                        .sendJsonObject(
                            JsonObject()
                                .put("workerId", workerId)
                                .put(
                                    "data",
                                    Base64.getEncoder().encodeToString("test".toByteArray()),
                                )
                        )
                        .coAwait()
                assertThat(response.statusCode()).isEqualTo(409)
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    // --- Get checkpoints ---

    @Test
    fun `should return empty page when job has no checkpoints`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val (job, _) = createAndAcquireJob()
                val page = getCheckpoints(job.getString("id"))

                assertThat(page.getJsonArray("checkpoints")).isEmpty()
                assertThat(page.getBoolean("hasMore")).isFalse()
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should return checkpoints in order`(vertx: Vertx, testContext: VertxTestContext) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val (job, workerId) = createAndAcquireJob()
                val jobId = job.getString("id")

                val c1 = saveCheckpoint(jobId, workerId, previousCheckpointId = null)
                val c2 = saveCheckpoint(jobId, workerId, previousCheckpointId = c1.getString("id"))
                val c3 = saveCheckpoint(jobId, workerId, previousCheckpointId = c2.getString("id"))

                val page = getCheckpoints(jobId)
                val checkpoints = page.getJsonArray("checkpoints")

                assertThat(checkpoints).hasSize(3)
                assertThat(checkpoints.getJsonObject(0).getString("id"))
                    .isEqualTo(c1.getString("id"))
                assertThat(checkpoints.getJsonObject(1).getString("id"))
                    .isEqualTo(c2.getString("id"))
                assertThat(checkpoints.getJsonObject(2).getString("id"))
                    .isEqualTo(c3.getString("id"))
                assertThat(page.getBoolean("hasMore")).isFalse()
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should paginate checkpoints with cursor`(vertx: Vertx, testContext: VertxTestContext) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val (job, workerId) = createAndAcquireJob()
                val jobId = job.getString("id")

                val c1 = saveCheckpoint(jobId, workerId, previousCheckpointId = null)
                val c2 = saveCheckpoint(jobId, workerId, previousCheckpointId = c1.getString("id"))
                val c3 = saveCheckpoint(jobId, workerId, previousCheckpointId = c2.getString("id"))

                val page1 = getCheckpoints(jobId, limit = 2)
                val checkpoints1 = page1.getJsonArray("checkpoints")
                assertThat(checkpoints1).hasSize(2)
                assertThat(page1.getBoolean("hasMore")).isTrue()

                val lastId = checkpoints1.getJsonObject(1).getString("id")
                val page2 = getCheckpoints(jobId, after = lastId, limit = 2)
                val checkpoints2 = page2.getJsonArray("checkpoints")
                assertThat(checkpoints2).hasSize(1)
                assertThat(checkpoints2.getJsonObject(0).getString("id"))
                    .isEqualTo(c3.getString("id"))
                assertThat(page2.getBoolean("hasMore")).isFalse()
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should paginate correctly when checkpoints share the same timestamp`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val (job, _) = createAndAcquireJob()
                val jobId = job.getString("id")
                val timestamp = 1000L

                insertCheckpointDirectly("checkpoint-a", jobId, null, timestamp, 1)
                insertCheckpointDirectly("checkpoint-b", jobId, "checkpoint-a", timestamp, 2)
                insertCheckpointDirectly("checkpoint-c", jobId, "checkpoint-b", timestamp, 3)
                insertCheckpointDirectly("checkpoint-d", jobId, "checkpoint-c", timestamp, 4)

                val page1 = getCheckpoints(jobId, limit = 2)
                val page1Ids =
                    page1.getJsonArray("checkpoints").map { (it as JsonObject).getString("id") }
                assertThat(page1Ids).hasSize(2)
                assertThat(page1.getBoolean("hasMore")).isTrue()

                val page2 = getCheckpoints(jobId, after = page1Ids.last(), limit = 2)
                val page2Ids =
                    page2.getJsonArray("checkpoints").map { (it as JsonObject).getString("id") }
                assertThat(page2Ids).hasSize(2)
                assertThat(page2.getBoolean("hasMore")).isFalse()

                val allIds = page1Ids + page2Ids
                assertThat(allIds).hasSize(4)
                assertThat(allIds)
                    .containsExactly("checkpoint-a", "checkpoint-b", "checkpoint-c", "checkpoint-d")
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should return 404 when getting checkpoints for non-existent job`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val response =
                    client
                        .get(port, "localhost", "/api/v1/jobs/non-existent-id/checkpoints")
                        .send()
                        .coAwait()
                assertThat(response.statusCode()).isEqualTo(404)
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should return 404 when cursor checkpoint does not exist`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val (job, _) = createAndAcquireJob()
                val response =
                    client
                        .get(
                            port,
                            "localhost",
                            "/api/v1/jobs/${job.getString("id")}/checkpoints?after=non-existent",
                        )
                        .send()
                        .coAwait()
                assertThat(response.statusCode()).isEqualTo(404)
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    // --- Error reporting ---

    @Test
    fun `should report error on acquired job and transition to PENDING`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val (job, workerId) = createAndAcquireJob()
                val result = reportError(job.getString("id"), workerId, "something went wrong")

                assertThat(result.getString("status")).isEqualTo("PENDING")
                assertThat(result.getInteger("retries")).isEqualTo(1)
                assertThat(result.getString("acquiredByWorkerId")).isNull()
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should report error and transition to FAILED when maxRetries exceeded`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val workerId = registerWorker()
                val created = createJob("test", "render", maxRetries = 1)
                assertThat(created.getInteger("maxRetries")).isEqualTo(1)

                val acquired = acquireJobs(workerId, 1)
                val result = reportError(acquired[0].getString("id"), workerId, "error")

                assertThat(result.getString("status")).isEqualTo("FAILED")
                // retries is null for terminal jobs; the count lives on the audit events.
                assertThat(result.getValue("retries")).isNull()
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should return 409 with PENDING status when reporting error on non-acquired job`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val workerId = registerWorker()
                val created = createJob("test", "render")
                val response =
                    client
                        .post(port, "localhost", "/api/v1/jobs/${created.getString("id")}/error")
                        .sendJsonObject(JsonObject().put("workerId", workerId).put("error", "fail"))
                        .coAwait()
                assertThat(response.statusCode()).isEqualTo(409)
                assertThat(response.bodyAsJsonObject().getString("error")).contains("PENDING")
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should return 404 when reporting error on non-existent job`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val workerId = registerWorker()
                val response =
                    client
                        .post(port, "localhost", "/api/v1/jobs/non-existent-id/error")
                        .sendJsonObject(JsonObject().put("workerId", workerId).put("error", "fail"))
                        .coAwait()
                assertThat(response.statusCode()).isEqualTo(404)
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should create job with maxRetries and verify in response`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val job = createJob("test", "render", maxRetries = 5)
                assertThat(job.getInteger("maxRetries")).isEqualTo(5)
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should create job without maxRetries and get null`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val job = createJob("test", "render")
                assertThat(job.getValue("maxRetries")).isNull()
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should return 400 when creating job with invalid maxRetries`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val response =
                    client
                        .post(port, "localhost", "/api/v1/jobs")
                        .sendJsonObject(
                            JsonObject()
                                .put("name", "test")
                                .put("type", "render")
                                .put("groupId", "test-group")
                                .put("maxRetries", 0)
                        )
                        .coAwait()
                assertThat(response.statusCode()).isEqualTo(400)
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    // --- Idempotency ---

    @Test
    fun `should derive deterministic job id from idempotencyKey and omit key from response`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val job = createJob("test", "render", idempotencyKey = "my-key-123")
                // The key is not echoed back; it is only the input used to derive `id`.
                assertThat(job.containsKey("idempotencyKey")).isFalse()
                assertThat(job.getString("id"))
                    .isEqualTo(JobIdGenerator.fromIdempotencyKey("test-group", "my-key-123"))
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should return 409 when creating job with duplicate idempotency key`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                createJob("job-1", "render", idempotencyKey = "dup-key")
                val response =
                    client
                        .post(port, "localhost", "/api/v1/jobs")
                        .sendJsonObject(
                            JsonObject()
                                .put("name", "job-2")
                                .put("type", "render")
                                .put("groupId", "test-group")
                                .put("idempotencyKey", "dup-key")
                        )
                        .coAwait()
                assertThat(response.statusCode()).isEqualTo(409)
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should return 400 when idempotency key is missing`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val response =
                    client
                        .post(port, "localhost", "/api/v1/jobs")
                        .sendJsonObject(
                            JsonObject()
                                .put("name", "job")
                                .put("type", "render")
                                .put("groupId", "test-group")
                        )
                        .coAwait()
                assertThat(response.statusCode()).isEqualTo(400)
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    // --- Group isolation ---

    @Test
    fun `should allow same idempotency key in different groups`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val responseA =
                    client
                        .post(port, "localhost", "/api/v1/jobs")
                        .sendJsonObject(
                            JsonObject()
                                .put("name", "job-a")
                                .put("type", "render")
                                .put("idempotencyKey", "K")
                                .put("groupId", "A")
                        )
                        .coAwait()
                assertThat(responseA.statusCode()).isEqualTo(201)

                val responseB =
                    client
                        .post(port, "localhost", "/api/v1/jobs")
                        .sendJsonObject(
                            JsonObject()
                                .put("name", "job-b")
                                .put("type", "render")
                                .put("idempotencyKey", "K")
                                .put("groupId", "B")
                        )
                        .coAwait()
                assertThat(responseB.statusCode()).isEqualTo(201)
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should return 409 for duplicate idempotency key within same group`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val first =
                    client
                        .post(port, "localhost", "/api/v1/jobs")
                        .sendJsonObject(
                            JsonObject()
                                .put("name", "job-1")
                                .put("type", "render")
                                .put("idempotencyKey", "K")
                                .put("groupId", "A")
                        )
                        .coAwait()
                assertThat(first.statusCode()).isEqualTo(201)

                val second =
                    client
                        .post(port, "localhost", "/api/v1/jobs")
                        .sendJsonObject(
                            JsonObject()
                                .put("name", "job-2")
                                .put("type", "render")
                                .put("idempotencyKey", "K")
                                .put("groupId", "A")
                        )
                        .coAwait()
                assertThat(second.statusCode()).isEqualTo(409)
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should only acquire jobs from the requested group`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val workerId = registerWorker()
                createJob("job-a1", "render", groupId = "A")
                createJob("job-a2", "render", groupId = "A")
                createJob("job-b1", "render", groupId = "B")

                val acquiredA = acquireJobs(workerId, 10, groupId = "A")
                assertThat(acquiredA).hasSize(2)
                for (job in acquiredA) {
                    assertThat(job.getString("groupId")).isEqualTo("A")
                }

                val acquiredB = acquireJobs(workerId, 10, groupId = "B")
                assertThat(acquiredB).hasSize(1)
                for (job in acquiredB) {
                    assertThat(job.getString("groupId")).isEqualTo("B")
                }
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    // --- Type filtering ---

    @Test
    fun `should only acquire jobs matching the requested type`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val workerId = registerWorker()
                createJob("render-1", "render")
                createJob("render-2", "render")
                createJob("export-1", "export")

                val acquired = acquireJobs(workerId, 10, type = "render")
                assertThat(acquired).hasSize(2)
                for (job in acquired) {
                    assertThat(job.getString("type")).isEqualTo("render")
                }

                val acquiredExport = acquireJobs(workerId, 10, type = "export")
                assertThat(acquiredExport).hasSize(1)
                for (job in acquiredExport) {
                    assertThat(job.getString("type")).isEqualTo("export")
                }
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should acquire jobs matching both groupId and type`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val workerId = registerWorker()
                createJob("match", "render", groupId = "A")
                createJob("wrong-group", "render", groupId = "B")
                createJob("wrong-type", "export", groupId = "A")

                val acquired = acquireJobs(workerId, 10, groupId = "A", type = "render")
                assertThat(acquired).hasSize(1)
                assertThat(acquired[0].getString("groupId")).isEqualTo("A")
                assertThat(acquired[0].getString("type")).isEqualTo("render")
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should return 400 when type is missing from acquire request`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val workerId = registerWorker()
                val response =
                    client
                        .post(port, "localhost", "/api/v1/jobs/acquire")
                        .sendJsonObject(
                            JsonObject().put("workerId", workerId).put("groupId", "test-group")
                        )
                        .coAwait()
                assertThat(response.statusCode()).isEqualTo(400)
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    // --- Retry lifecycle ---

    @Test
    fun `should complete full retry lifecycle - create, acquire, error, re-acquire, error until FAILED`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val workerId = registerWorker()
                val created = createJob("retry-job", "render", maxRetries = 2)
                val jobId = created.getString("id")

                // First attempt
                val acquired1 = acquireJobs(workerId, 1)
                assertThat(acquired1[0].getString("id")).isEqualTo(jobId)
                val afterError1 = reportError(jobId, workerId, "error 1")
                assertThat(afterError1.getString("status")).isEqualTo("PENDING")
                assertThat(afterError1.getInteger("retries")).isEqualTo(1)

                // Second attempt
                val acquired2 = acquireJobs(workerId, 1)
                assertThat(acquired2[0].getString("id")).isEqualTo(jobId)
                val afterError2 = reportError(jobId, workerId, "error 2")
                assertThat(afterError2.getString("status")).isEqualTo("FAILED")
                // retries is null for terminal jobs; the count lives on the audit events.
                assertThat(afterError2.getValue("retries")).isNull()

                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should reset retries on checkpoint save and allow more errors before FAILED`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val workerId = registerWorker()
                val created = createJob("retry-reset-job", "render", maxRetries = 2)
                val jobId = created.getString("id")

                // First attempt: acquire, error (retries=1)
                acquireJobs(workerId, 1)
                reportError(jobId, workerId, "error 1")
                assertThat(acquireJobs(workerId, 1)[0].getInteger("retries")).isEqualTo(1)

                // Second attempt: save checkpoint (resets retries to 0), then error
                saveCheckpoint(jobId, workerId, previousCheckpointId = null)

                // Verify retries are reset by reporting error again
                reportError(jobId, workerId, "error after checkpoint")
                val afterError = acquireJobs(workerId, 1)[0]
                assertThat(afterError.getInteger("retries")).isEqualTo(1)

                // One more error should trigger FAILED (retries goes to 2)
                reportError(jobId, workerId, "final error")

                // Job should now be FAILED — no more jobs to acquire
                val remaining = acquireJobs(workerId, 1)
                assertThat(remaining).isEmpty()

                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    // --- Abort ---

    @Test
    fun `should abort a pending job`(vertx: Vertx, testContext: VertxTestContext) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val created = createJob("test", "render")
                val aborted = abortJob(created.getString("id"))

                assertThat(aborted.getString("status")).isEqualTo("ABORTED")
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should abort an acquired job`(vertx: Vertx, testContext: VertxTestContext) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val (job, _) = createAndAcquireJob()
                val aborted = abortJob(job.getString("id"))

                assertThat(aborted.getString("status")).isEqualTo("ABORTED")
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should return 409 with FINISHED status when aborting a finished job`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val (job, workerId) = createAndAcquireJob()
                completeJob(job.getString("id"), workerId)

                val response =
                    client
                        .post(port, "localhost", "/api/v1/jobs/${job.getString("id")}/abort")
                        .send()
                        .coAwait()
                assertThat(response.statusCode()).isEqualTo(409)
                assertThat(response.bodyAsJsonObject().getString("error")).contains("FINISHED")
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should return 409 with ABORTED status when aborting an already aborted job`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val created = createJob("test", "render")
                abortJob(created.getString("id"))

                val response =
                    client
                        .post(port, "localhost", "/api/v1/jobs/${created.getString("id")}/abort")
                        .send()
                        .coAwait()
                assertThat(response.statusCode()).isEqualTo(409)
                assertThat(response.bodyAsJsonObject().getString("error")).contains("ABORTED")
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should return 404 when aborting a non-existent job`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val response =
                    client
                        .post(port, "localhost", "/api/v1/jobs/non-existent-id/abort")
                        .send()
                        .coAwait()
                assertThat(response.statusCode()).isEqualTo(404)
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    // --- Job events ---

    @Test
    fun `should return job events timeline for full lifecycle`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val workerId = registerWorker()
                val job = createJob("event-job", "render")
                val jobId = job.getString("id")

                // Acquire
                acquireJobs(workerId, 1)

                // Complete
                client
                    .post(port, "localhost", "/api/v1/jobs/$jobId/complete")
                    .sendJsonObject(JsonObject().put("workerId", workerId))
                    .coAwait()

                // Fetch events
                val response =
                    client.get(port, "localhost", "/api/v1/jobs/$jobId/events").send().coAwait()
                assertThat(response.statusCode()).isEqualTo(200)
                val events = response.bodyAsJsonArray()
                val eventTypes = events.map { (it as JsonObject).getString("eventType") }
                assertThat(eventTypes).containsExactly("CREATED", "ACQUIRED", "COMPLETED")
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should include error details in ERROR_REPORTED event`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val workerId = registerWorker()
                val job = createJob("error-job", "render", maxRetries = 2)
                val jobId = job.getString("id")

                acquireJobs(workerId, 1)
                reportError(jobId, workerId, "something went wrong")

                val response =
                    client.get(port, "localhost", "/api/v1/jobs/$jobId/events").send().coAwait()
                val events = response.bodyAsJsonArray()
                val errorEvent =
                    events
                        .map { it as JsonObject }
                        .first { it.getString("eventType") == "ERROR_REPORTED" }
                assertThat(errorEvent.getString("eventMessage")).isEqualTo("something went wrong")
                assertThat(errorEvent.getString("actorType")).isEqualTo("WORKER")
                assertThat(errorEvent.getString("actorId")).isEqualTo(workerId)
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should emit ACQUIRED event atomically with job acquisition`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val workerId = registerWorker()
                createJob("job-1", "render")
                createJob("job-2", "render")

                val acquired = acquireJobs(workerId, 10)
                assertThat(acquired).hasSize(2)

                // Events should exist for both acquired jobs
                for (job in acquired) {
                    val jobId = job.getString("id")
                    val response =
                        client.get(port, "localhost", "/api/v1/jobs/$jobId/events").send().coAwait()
                    assertThat(response.statusCode()).isEqualTo(200)
                    val eventTypes =
                        response.bodyAsJsonArray().map { (it as JsonObject).getString("eventType") }
                    assertThat(eventTypes).contains("CREATED", "ACQUIRED")
                }
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should return 409 on duplicate idempotency key and allow retrieval of existing job`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val original = createJob("job-1", "render", idempotencyKey = "unique-key-123")
                val originalId = original.getString("id")

                // Retry with same key should fail
                val response =
                    client
                        .post(port, "localhost", "/api/v1/jobs")
                        .sendJsonObject(
                            JsonObject()
                                .put("name", "job-2")
                                .put("type", "render")
                                .put("groupId", "test-group")
                                .put("idempotencyKey", "unique-key-123")
                        )
                        .coAwait()
                assertThat(response.statusCode()).isEqualTo(409)

                // Original job should still be accessible
                val getResponse =
                    client
                        .get(port, "localhost", "/api/v1/jobs/$originalId/events")
                        .send()
                        .coAwait()
                assertThat(getResponse.statusCode()).isEqualTo(200)
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    // --- Input/Output data ---

    @Test
    fun `should store and return input data on job creation`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val inputBase64 = Base64.getEncoder().encodeToString("hello world".toByteArray())
                val job = createJob("io-job", "render", inputData = inputBase64)

                assertThat(job.getString("inputData")).isEqualTo(inputBase64)
                assertThat(job.getValue("outputData")).isNull()
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should store and return output data on job completion`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val inputBase64 = Base64.getEncoder().encodeToString("input".toByteArray())
                val outputBase64 = Base64.getEncoder().encodeToString("result".toByteArray())
                val (job, workerId) = createAndAcquireJob(inputData = inputBase64)
                val jobId = job.getString("id")

                val completed = completeJob(jobId, workerId, outputData = outputBase64)
                assertThat(completed.getString("inputData")).isEqualTo(inputBase64)
                assertThat(completed.getString("outputData")).isEqualTo(outputBase64)
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should create job without input or output data`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val job = createJob("no-data-job", "render")
                assertThat(job.getValue("inputData")).isNull()
                assertThat(job.getValue("outputData")).isNull()
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    // --- Event lifecycle coverage ---

    @Test
    fun `should emit RELEASED event when job is released`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val (job, workerId) = createAndAcquireJob()
                val jobId = job.getString("id")
                releaseJob(jobId, workerId)

                val response =
                    client.get(port, "localhost", "/api/v1/jobs/$jobId/events").send().coAwait()
                val eventTypes =
                    response.bodyAsJsonArray().map { (it as JsonObject).getString("eventType") }
                assertThat(eventTypes).containsExactly("CREATED", "ACQUIRED", "RELEASED")
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should emit ABORTED event when job is aborted`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val job = createJob("abort-job", "render")
                val jobId = job.getString("id")
                abortJob(jobId)

                val response =
                    client.get(port, "localhost", "/api/v1/jobs/$jobId/events").send().coAwait()
                val eventTypes =
                    response.bodyAsJsonArray().map { (it as JsonObject).getString("eventType") }
                assertThat(eventTypes).containsExactly("CREATED", "ABORTED")
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should emit ERROR_REPORTED and FAILED events when max retries exceeded`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val workerId = registerWorker()
                val job = createJob("fail-job", "render", maxRetries = 1)
                val jobId = job.getString("id")

                acquireJobs(workerId, 1)
                reportError(jobId, workerId, "fatal error")

                val response =
                    client.get(port, "localhost", "/api/v1/jobs/$jobId/events").send().coAwait()
                val eventTypes =
                    response.bodyAsJsonArray().map { (it as JsonObject).getString("eventType") }
                assertThat(eventTypes)
                    .containsExactly("CREATED", "ACQUIRED", "ERROR_REPORTED", "FAILED")
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    // --- Worker lifecycle ---

    @Test
    fun `should register a worker`(vertx: Vertx, testContext: VertxTestContext) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val response =
                    client
                        .post(port, "localhost", "/api/v1/workers")
                        .sendJsonObject(
                            JsonObject()
                                .put("workerId", "w-1")
                                .put("heartbeatTimeout", 5000)
                                .put("sessionTimeout", 15000)
                        )
                        .coAwait()
                assertThat(response.statusCode()).isEqualTo(201)
                val body = response.bodyAsJsonObject()
                assertThat(body.getString("id")).isEqualTo("w-1")
                assertThat(body.getLong("heartbeatTimeout")).isEqualTo(5000)
                assertThat(body.getLong("sessionTimeout")).isEqualTo(15000)
                assertThat(body.getString("lastHeartbeatAt")).isNotBlank()
                assertThat(body.getString("registeredAt")).isNotBlank()
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should heartbeat a registered worker`(vertx: Vertx, testContext: VertxTestContext) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val workerId = registerWorker()
                val response =
                    client
                        .post(port, "localhost", "/api/v1/workers/$workerId/heartbeat")
                        .send()
                        .coAwait()
                assertThat(response.statusCode()).isEqualTo(200)
                assertThat(response.bodyAsJsonObject().getString("id")).isEqualTo(workerId)
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should deregister a worker and free its jobs`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val (job, workerId) = createAndAcquireJob()
                val jobId = job.getString("id")

                val response =
                    client.delete(port, "localhost", "/api/v1/workers/$workerId").send().coAwait()
                assertThat(response.statusCode()).isEqualTo(204)

                // Job should now be back to PENDING
                val workerId2 = registerWorker()
                val reacquired = acquireJobs(workerId2, 10)
                assertThat(reacquired).hasSize(1)
                assertThat(reacquired[0].getString("id")).isEqualTo(jobId)
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should return 409 noting the existing worker is still active on duplicate registration`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                registerWorker("dup-worker")
                val response =
                    client
                        .post(port, "localhost", "/api/v1/workers")
                        .sendJsonObject(
                            JsonObject()
                                .put("workerId", "dup-worker")
                                .put("heartbeatTimeout", 5000)
                                .put("sessionTimeout", 15000)
                        )
                        .coAwait()
                assertThat(response.statusCode()).isEqualTo(409)
                assertThat(response.bodyAsJsonObject().getString("error")).contains("still active")
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should return 409 noting the existing worker timed out when re-registering a dead worker id`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                registerWorker("dead-dup-worker")
                markWorkerDead("dead-dup-worker")
                val response =
                    client
                        .post(port, "localhost", "/api/v1/workers")
                        .sendJsonObject(
                            JsonObject()
                                .put("workerId", "dead-dup-worker")
                                .put("heartbeatTimeout", 5000)
                                .put("sessionTimeout", 15000)
                        )
                        .coAwait()
                assertThat(response.statusCode()).isEqualTo(409)
                assertThat(response.bodyAsJsonObject().getString("error")).contains("timed out")
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should return 409 not 500 when concurrent register requests race past pre-check`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val workerId = "race-worker"
                val body =
                    JsonObject()
                        .put("workerId", workerId)
                        .put("heartbeatTimeout", 5000)
                        .put("sessionTimeout", 15000)
                val results =
                    (1..5)
                        .map {
                            async {
                                client
                                    .post(port, "localhost", "/api/v1/workers")
                                    .sendJsonObject(body)
                                    .coAwait()
                            }
                        }
                        .awaitAll()
                val statusCodes = results.map { it.statusCode() }
                assertThat(statusCodes.count { it == 201 }).isEqualTo(1)
                assertThat(statusCodes.filter { it != 201 }).allMatch { it == 409 }
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should return 400 when registering worker with invalid timeouts`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val response =
                    client
                        .post(port, "localhost", "/api/v1/workers")
                        .sendJsonObject(
                            JsonObject()
                                .put("workerId", "w-bad")
                                .put("heartbeatTimeout", 5000)
                                .put("sessionTimeout", 5000)
                        )
                        .coAwait()
                assertThat(response.statusCode()).isEqualTo(400)
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should return 404 when acquiring with unregistered worker`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                createJob("test", "render")
                val response =
                    client
                        .post(port, "localhost", "/api/v1/jobs/acquire")
                        .sendJsonObject(
                            JsonObject()
                                .put("workerId", "non-existent")
                                .put("limit", 10)
                                .put("groupId", "test-group")
                                .put("type", "render")
                        )
                        .coAwait()
                assertThat(response.statusCode()).isEqualTo(404)
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should return 409 when completing job with wrong workerId`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val (job, _) = createAndAcquireJob()
                val response =
                    client
                        .post(port, "localhost", "/api/v1/jobs/${job.getString("id")}/complete")
                        .sendJsonObject(JsonObject().put("workerId", "wrong-worker"))
                        .coAwait()
                assertThat(response.statusCode()).isEqualTo(409)
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should return 404 when heartbeating non-existent worker`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val response =
                    client
                        .post(port, "localhost", "/api/v1/workers/non-existent/heartbeat")
                        .send()
                        .coAwait()
                assertThat(response.statusCode()).isEqualTo(404)
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should return 404 when deregistering non-existent worker`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val response =
                    client
                        .delete(port, "localhost", "/api/v1/workers/non-existent")
                        .send()
                        .coAwait()
                assertThat(response.statusCode()).isEqualTo(404)
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    // --- Validation (400) ---

    @Test
    fun `should return 400 when creating job with missing type`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val response =
                    client
                        .post(port, "localhost", "/api/v1/jobs")
                        .sendJsonObject(
                            JsonObject().put("name", "test").put("groupId", "test-group")
                        )
                        .coAwait()
                assertThat(response.statusCode()).isEqualTo(400)
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should return 400 when creating job without groupId`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val response =
                    client
                        .post(port, "localhost", "/api/v1/jobs")
                        .sendJsonObject(JsonObject().put("name", "test").put("type", "render"))
                        .coAwait()
                assertThat(response.statusCode()).isEqualTo(400)
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should return 400 when acquiring jobs without groupId`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val workerId = registerWorker()
                val response =
                    client
                        .post(port, "localhost", "/api/v1/jobs/acquire")
                        .sendJsonObject(JsonObject().put("workerId", workerId).put("limit", 10))
                        .coAwait()
                assertThat(response.statusCode()).isEqualTo(400)
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should return 400 when acquiring jobs with missing workerId`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val response =
                    client
                        .post(port, "localhost", "/api/v1/jobs/acquire")
                        .sendJsonObject(JsonObject().put("limit", 10).put("groupId", "test-group"))
                        .coAwait()
                assertThat(response.statusCode()).isEqualTo(400)
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should return 400 when registering worker with missing workerId`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val response =
                    client
                        .post(port, "localhost", "/api/v1/workers")
                        .sendJsonObject(
                            JsonObject().put("heartbeatTimeout", 5000).put("sessionTimeout", 15000)
                        )
                        .coAwait()
                assertThat(response.statusCode()).isEqualTo(400)
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should return 400 when registering worker with missing timeouts`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val response =
                    client
                        .post(port, "localhost", "/api/v1/workers")
                        .sendJsonObject(JsonObject().put("workerId", "worker-1"))
                        .coAwait()
                assertThat(response.statusCode()).isEqualTo(400)
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should return 400 when creating job with blank type`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val response =
                    client
                        .post(port, "localhost", "/api/v1/jobs")
                        .sendJsonObject(
                            JsonObject()
                                .put("name", "test")
                                .put("type", "  ")
                                .put("groupId", "test-group")
                        )
                        .coAwait()
                assertThat(response.statusCode()).isEqualTo(400)
                assertThat(response.bodyAsJsonObject().getString("error")).isNotBlank()
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should return 400 when saving checkpoint with blank name`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val (job, workerId) = createAndAcquireJob()
                val response =
                    client
                        .post(port, "localhost", "/api/v1/jobs/${job.getString("id")}/checkpoints")
                        .sendJsonObject(
                            JsonObject()
                                .put("workerId", workerId)
                                .put("name", "  ")
                                .put(
                                    "data",
                                    Base64.getEncoder().encodeToString("test".toByteArray()),
                                )
                        )
                        .coAwait()
                assertThat(response.statusCode()).isEqualTo(400)
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should return 400 for malformed JSON body`(vertx: Vertx, testContext: VertxTestContext) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val response =
                    client
                        .post(port, "localhost", "/api/v1/jobs")
                        .putHeader("content-type", "application/json")
                        .sendBuffer(io.vertx.core.buffer.Buffer.buffer("not valid json{{"))
                        .coAwait()
                assertThat(response.statusCode()).isEqualTo(400)
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should return 400 for malformed JSON body on worker registration`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val response =
                    client
                        .post(port, "localhost", "/api/v1/workers")
                        .putHeader("content-type", "application/json")
                        .sendBuffer(io.vertx.core.buffer.Buffer.buffer("not valid json{{"))
                        .coAwait()
                assertThat(response.statusCode()).isEqualTo(400)
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should return 400 when saving checkpoint with invalid base64 data`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val (job, workerId) = createAndAcquireJob()
                val response =
                    client
                        .post(port, "localhost", "/api/v1/jobs/${job.getString("id")}/checkpoints")
                        .sendJsonObject(
                            JsonObject()
                                .put("workerId", workerId)
                                .put("data", "not valid base64!!!")
                        )
                        .coAwait()
                assertThat(response.statusCode()).isEqualTo(400)
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should return 400 when limit query parameter is not a valid integer`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val job = createJob()
                val response =
                    client
                        .get(
                            port,
                            "localhost",
                            "/api/v1/jobs/${job.getString("id")}/checkpoints?limit=abc",
                        )
                        .send()
                        .coAwait()
                assertThat(response.statusCode()).isEqualTo(400)
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    // --- Not Found (404) ---

    @Test
    fun `should return 404 when completing a non-existent job`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val workerId = registerWorker()
                val response =
                    client
                        .post(port, "localhost", "/api/v1/jobs/non-existent-id/complete")
                        .sendJsonObject(JsonObject().put("workerId", workerId))
                        .coAwait()
                assertThat(response.statusCode()).isEqualTo(404)
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should return 404 when releasing a non-existent job`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val workerId = registerWorker()
                val response =
                    client
                        .post(port, "localhost", "/api/v1/jobs/non-existent-id/release")
                        .sendJsonObject(JsonObject().put("workerId", workerId))
                        .coAwait()
                assertThat(response.statusCode()).isEqualTo(404)
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should return 404 when saving checkpoint on non-existent job`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val workerId = registerWorker()
                val response =
                    client
                        .post(port, "localhost", "/api/v1/jobs/non-existent-id/checkpoints")
                        .sendJsonObject(
                            JsonObject()
                                .put("workerId", workerId)
                                .put(
                                    "data",
                                    Base64.getEncoder().encodeToString("test".toByteArray()),
                                )
                        )
                        .coAwait()
                assertThat(response.statusCode()).isEqualTo(404)
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    // --- Conflict (409) ---

    @Test
    fun `should return 409 with PENDING status when completing a pending job`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val workerId = registerWorker()
                val created = createJob("test", "render")
                val response =
                    client
                        .post(port, "localhost", "/api/v1/jobs/${created.getString("id")}/complete")
                        .sendJsonObject(JsonObject().put("workerId", workerId))
                        .coAwait()
                assertThat(response.statusCode()).isEqualTo(409)
                assertThat(response.bodyAsJsonObject().getString("error")).contains("PENDING")
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should return 409 with PENDING status when releasing a pending job`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val workerId = registerWorker()
                val created = createJob("test", "render")
                val response =
                    client
                        .post(port, "localhost", "/api/v1/jobs/${created.getString("id")}/release")
                        .sendJsonObject(JsonObject().put("workerId", workerId))
                        .coAwait()
                assertThat(response.statusCode()).isEqualTo(409)
                assertThat(response.bodyAsJsonObject().getString("error")).contains("PENDING")
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should return 409 with PENDING status when saving checkpoint on a pending job`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val workerId = registerWorker()
                val created = createJob("test", "render")
                val response =
                    client
                        .post(
                            port,
                            "localhost",
                            "/api/v1/jobs/${created.getString("id")}/checkpoints",
                        )
                        .sendJsonObject(
                            JsonObject()
                                .put("workerId", workerId)
                                .put(
                                    "data",
                                    Base64.getEncoder().encodeToString("test".toByteArray()),
                                )
                        )
                        .coAwait()
                assertThat(response.statusCode()).isEqualTo(409)
                assertThat(response.bodyAsJsonObject().getString("error")).contains("PENDING")
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should return 409 with FINISHED status when completing an already finished job`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val (job, workerId) = createAndAcquireJob()
                completeJob(job.getString("id"), workerId)

                val response =
                    client
                        .post(port, "localhost", "/api/v1/jobs/${job.getString("id")}/complete")
                        .sendJsonObject(JsonObject().put("workerId", workerId))
                        .coAwait()
                assertThat(response.statusCode()).isEqualTo(409)
                assertThat(response.bodyAsJsonObject().getString("error")).contains("FINISHED")
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    // --- Three-table invariant + sleep semantics ---

    /**
     * Reads the underlying state-machine tables for a job and asserts the three-table invariant:
     * the job lives in *exactly one* of `job_queue`, `job_acquired`, or has `jobs.terminal_status`
     * set. Any other shape (zero locations, or two locations) is a bug.
     */
    private fun assertThreeTableInvariant(jobId: String) {
        val conn = assertionConn()
        val inQueue =
            conn.prepareStatement("SELECT COUNT(*) FROM job_queue WHERE job_id = ?").use { s ->
                s.setString(1, jobId)
                s.executeQuery().use {
                    it.next()
                    it.getInt(1)
                }
            }
        val inAcquired =
            conn.prepareStatement("SELECT COUNT(*) FROM job_acquired WHERE job_id = ?").use { s ->
                s.setString(1, jobId)
                s.executeQuery().use {
                    it.next()
                    it.getInt(1)
                }
            }
        val terminalStatus =
            conn.prepareStatement("SELECT terminal_status FROM jobs WHERE id = ?").use { s ->
                s.setString(1, jobId)
                s.executeQuery().use { if (it.next()) it.getString(1) else null }
            }
        val locations = inQueue + inAcquired + (if (terminalStatus != null) 1 else 0)
        assertThat(locations)
            .describedAs(
                "three-table invariant violated for job $jobId: queue=$inQueue acquired=$inAcquired terminal=$terminalStatus"
            )
            .isEqualTo(1)
    }

    private fun readQueueAvailableAt(jobId: String): Long? =
        assertionConn()
            .prepareStatement("SELECT available_at FROM job_queue WHERE job_id = ?")
            .use { s ->
                s.setString(1, jobId)
                s.executeQuery().use { if (it.next()) it.getLong(1) else null }
            }

    private fun assertTerminalConsistency(jobId: String) {
        assertionConn()
            .prepareStatement("SELECT terminal_status, terminal_at FROM jobs WHERE id = ?")
            .use { s ->
                s.setString(1, jobId)
                s.executeQuery().use { rs ->
                    assertThat(rs.next()).describedAs("job $jobId should exist").isTrue()
                    val terminalStatus = rs.getString("terminal_status")
                    val terminalAtRaw = rs.getObject("terminal_at")
                    assertThat(terminalStatus == null)
                        .describedAs(
                            "terminal_status / terminal_at consistency violated for job $jobId: terminal_status=$terminalStatus, terminal_at=$terminalAtRaw"
                        )
                        .isEqualTo(terminalAtRaw == null)
                }
            }
    }

    @Test
    fun `should maintain three-table invariant across full lifecycle`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val workerId = registerWorker()
                val created = createJob("inv-job", "render")
                val jobId = created.getString("id")
                assertThreeTableInvariant(jobId)

                val acquired = acquireJobs(workerId, 1)
                assertThat(acquired).hasSize(1)
                assertThreeTableInvariant(jobId)

                releaseJob(jobId, workerId)
                assertThreeTableInvariant(jobId)

                acquireJobs(workerId, 1)
                assertThreeTableInvariant(jobId)

                completeJob(jobId, workerId)
                assertThreeTableInvariant(jobId)

                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should maintain three-table invariant across every transition`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val workerId = registerWorker()

                // PENDING -> ACQUIRED -> PENDING (release) -> ACQUIRED -> FINISHED.
                val completeFlow = createJob("inv-complete-flow", "render")
                val completeId = completeFlow.getString("id")
                assertThreeTableInvariant(completeId)
                acquireJobs(workerId, 1)
                assertThreeTableInvariant(completeId)
                releaseJob(completeId, workerId)
                assertThreeTableInvariant(completeId)
                acquireJobs(workerId, 1)
                assertThreeTableInvariant(completeId)
                completeJob(completeId, workerId)
                assertThreeTableInvariant(completeId)

                // PENDING -> ABORTED (pending path).
                val abortPending = createJob("inv-abort-pending", "render")
                val abortPendingId = abortPending.getString("id")
                abortJob(abortPendingId)
                assertThreeTableInvariant(abortPendingId)

                // PENDING -> ACQUIRED -> ABORTED (acquired path).
                val abortAcquired = createJob("inv-abort-acquired", "render")
                val abortAcquiredId = abortAcquired.getString("id")
                acquireJobs(workerId, 1)
                abortJob(abortAcquiredId)
                assertThreeTableInvariant(abortAcquiredId)

                // PENDING -> ACQUIRED -> FAILED (retries exhausted).
                val failFlow = createJob("inv-fail-flow", "render", maxRetries = 1)
                val failId = failFlow.getString("id")
                acquireJobs(workerId, 1)
                reportError(failId, workerId, "boom")
                assertThreeTableInvariant(failId)

                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should keep terminal_status and terminal_at consistent across every transition`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val workerId = registerWorker()

                // PENDING -> ACQUIRED -> PENDING (release) -> ACQUIRED -> FINISHED.
                val completeFlow = createJob("complete-flow", "render")
                val completeId = completeFlow.getString("id")
                assertTerminalConsistency(completeId)
                acquireJobs(workerId, 1)
                assertTerminalConsistency(completeId)
                releaseJob(completeId, workerId)
                assertTerminalConsistency(completeId)
                acquireJobs(workerId, 1)
                assertTerminalConsistency(completeId)
                completeJob(completeId, workerId)
                assertTerminalConsistency(completeId)

                // PENDING -> ABORTED (pending path).
                val abortPending = createJob("abort-pending", "render")
                val abortPendingId = abortPending.getString("id")
                abortJob(abortPendingId)
                assertTerminalConsistency(abortPendingId)

                // PENDING -> ACQUIRED -> ABORTED (acquired path).
                val abortAcquired = createJob("abort-acquired", "render")
                val abortAcquiredId = abortAcquired.getString("id")
                acquireJobs(workerId, 1)
                abortJob(abortAcquiredId)
                assertTerminalConsistency(abortAcquiredId)

                // PENDING -> ACQUIRED -> FAILED (retries exhausted).
                val failFlow = createJob("fail-flow", "render", maxRetries = 1)
                val failId = failFlow.getString("id")
                acquireJobs(workerId, 1)
                reportError(failId, workerId, "boom")
                assertTerminalConsistency(failId)

                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `acquire should skip rows whose availableAt is in the future`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val (job, workerId) = createAndAcquireJob()
                val jobId = job.getString("id")
                val futureDeadline = System.currentTimeMillis() + 60_000L

                val released = releaseJob(jobId, workerId, availableAt = futureDeadline)
                assertThat(released.getString("status")).isEqualTo("PENDING")
                assertThat(released.getLong("availableAt")).isEqualTo(futureDeadline)
                assertThat(readQueueAvailableAt(jobId)).isEqualTo(futureDeadline)

                val tooEarly = acquireJobs(workerId, 10)
                assertThat(tooEarly).isEmpty()

                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `acquire should pick up rows once availableAt has elapsed`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val (job, workerId) = createAndAcquireJob()
                val jobId = job.getString("id")
                // availableAt in the past — should be acquirable immediately.
                val pastDeadline = System.currentTimeMillis() - 1_000L
                releaseJob(jobId, workerId, availableAt = pastDeadline)

                val reacquired = acquireJobs(workerId, 10)
                assertThat(reacquired.map { it.getString("id") }).contains(jobId)
                assertThreeTableInvariant(jobId)
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `release should emit eventDetail with availableAt when nonzero`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val (job, workerId) = createAndAcquireJob()
                val jobId = job.getString("id")
                val deadline = System.currentTimeMillis() + 60_000L

                releaseJob(jobId, workerId, availableAt = deadline)

                val eventsResponse =
                    client.get(port, "localhost", "/api/v1/jobs/$jobId/events").send().coAwait()
                val events = eventsResponse.bodyAsJsonArray().map { it as JsonObject }
                val released =
                    events.firstOrNull { it.getString("eventType") == "RELEASED" }
                        ?: error("RELEASED event missing")
                val detail = released.getString("eventDetail")
                assertThat(detail).contains("\"availableAt\":$deadline")
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `release should not emit eventDetail when availableAt is zero or absent`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val (job, workerId) = createAndAcquireJob()
                val jobId = job.getString("id")

                releaseJob(jobId, workerId)

                val eventsResponse =
                    client.get(port, "localhost", "/api/v1/jobs/$jobId/events").send().coAwait()
                val events = eventsResponse.bodyAsJsonArray().map { it as JsonObject }
                val released =
                    events.firstOrNull { it.getString("eventType") == "RELEASED" }
                        ?: error("RELEASED event missing")
                assertThat(released.getString("eventDetail")).isNull()
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `release after worker has lost ownership should return 409 JobNotOwnedByWorkerException`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val (job, workerId) = createAndAcquireJob()
                val jobId = job.getString("id")
                releaseJob(jobId, workerId)
                // Second release: worker no longer owns the row.
                val response =
                    client
                        .post(port, "localhost", "/api/v1/jobs/$jobId/release")
                        .sendJsonObject(JsonObject().put("workerId", workerId))
                        .coAwait()
                assertThat(response.statusCode()).isEqualTo(409)
                assertThreeTableInvariant(jobId)
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `worker reaper writes availableAt = 0 unconditionally`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val (job, workerId) = createAndAcquireJob()
                val jobId = job.getString("id")
                // Deregister the worker: same job move-back the dead-worker reaper performs.
                val deregister =
                    client.delete(port, "localhost", "/api/v1/workers/$workerId").send().coAwait()
                assertThat(deregister.statusCode()).isEqualTo(204)
                assertThreeTableInvariant(jobId)
                assertThat(readQueueAvailableAt(jobId)).isEqualTo(0L)
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    // --- Race tests ---

    @Test
    fun `concurrent acquire and release race results in exactly one move`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val (job, workerA) = createAndAcquireJob()
                val workerB = registerWorker()
                val jobId = job.getString("id")

                // Worker A releases concurrently with a re-acquire by anyone.
                val results =
                    listOf(
                            async {
                                runCatching {
                                        client
                                            .post(port, "localhost", "/api/v1/jobs/$jobId/release")
                                            .sendJsonObject(JsonObject().put("workerId", workerA))
                                            .coAwait()
                                            .statusCode()
                                    }
                                    .getOrElse { -1 }
                            },
                            async { runCatching { acquireJobs(workerB, 1).size }.getOrElse { -1 } },
                        )
                        .awaitAll()
                // Whatever the interleaving, the invariant must hold afterwards.
                assertThreeTableInvariant(jobId)
                assertThat(results[0]).isEqualTo(200) // release succeeds — A had ownership
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `concurrent reaper and release race never produces dual state`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val (job, workerId) = createAndAcquireJob()
                val jobId = job.getString("id")
                listOf(
                        async {
                            client
                                .delete(port, "localhost", "/api/v1/workers/$workerId")
                                .send()
                                .coAwait()
                                .statusCode()
                        },
                        async {
                            client
                                .post(port, "localhost", "/api/v1/jobs/$jobId/release")
                                .sendJsonObject(JsonObject().put("workerId", workerId))
                                .coAwait()
                                .statusCode()
                        },
                    )
                    .awaitAll()
                assertThreeTableInvariant(jobId)
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `concurrent abort and acquire race never violates invariant`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val workerId = registerWorker()
                val created = createJob("race-job", "render")
                val jobId = created.getString("id")
                val results =
                    listOf(
                            async {
                                client
                                    .post(port, "localhost", "/api/v1/jobs/$jobId/abort")
                                    .send()
                                    .coAwait()
                                    .statusCode()
                            },
                            async {
                                runCatching { acquireJobs(workerId, 1).map { it.getString("id") } }
                                    .getOrElse { emptyList() }
                            },
                        )
                        .awaitAll()
                assertThreeTableInvariant(jobId)
                // At least one of the two operations must have observed the job.
                @Suppress("UNCHECKED_CAST") val acquiredIds = results[1] as List<String>
                val aborted = results[0] == 200
                assertThat(aborted || acquiredIds.contains(jobId))
                    .describedAs("neither abort nor acquire saw the job")
                    .isTrue()
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `concurrent abort and release race never violates invariant`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val (job, workerId) = createAndAcquireJob()
                val jobId = job.getString("id")
                listOf(
                        async {
                            client
                                .post(port, "localhost", "/api/v1/jobs/$jobId/abort")
                                .send()
                                .coAwait()
                                .statusCode()
                        },
                        async {
                            client
                                .post(port, "localhost", "/api/v1/jobs/$jobId/release")
                                .sendJsonObject(JsonObject().put("workerId", workerId))
                                .coAwait()
                                .statusCode()
                        },
                    )
                    .awaitAll()
                assertThreeTableInvariant(jobId)
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `concurrent abort and complete race never violates invariant`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val (job, workerId) = createAndAcquireJob()
                val jobId = job.getString("id")
                listOf(
                        async {
                            client
                                .post(port, "localhost", "/api/v1/jobs/$jobId/abort")
                                .send()
                                .coAwait()
                                .statusCode()
                        },
                        async {
                            client
                                .post(port, "localhost", "/api/v1/jobs/$jobId/complete")
                                .sendJsonObject(JsonObject().put("workerId", workerId))
                                .coAwait()
                                .statusCode()
                        },
                    )
                    .awaitAll()
                assertThreeTableInvariant(jobId)
                // After both, terminal_status is set to one of FINISHED or ABORTED.
                backend.getJdbcConnection().use { conn ->
                    val terminal =
                        conn
                            .prepareStatement("SELECT terminal_status FROM jobs WHERE id = ?")
                            .use { s ->
                                s.setString(1, jobId)
                                s.executeQuery().use {
                                    it.next()
                                    it.getString(1)
                                }
                            }
                    assertThat(terminal).isIn("FINISHED", "ABORTED")
                }
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.MINUTES)
    fun `concurrent abort and release race never violates invariant (stress)`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                // Each iteration uses a fresh group so acquire only ever picks up its own job, even
                // when a prior iteration's release "won" and left a PENDING job behind.
                repeat(stressRaceIterations) { i ->
                    val (job, workerId) = createAndAcquireJob(groupId = "abort-release-stress-$i")
                    val jobId = job.getString("id")
                    listOf(
                            async {
                                client
                                    .post(port, "localhost", "/api/v1/jobs/$jobId/abort")
                                    .send()
                                    .coAwait()
                                    .statusCode()
                            },
                            async {
                                client
                                    .post(port, "localhost", "/api/v1/jobs/$jobId/release")
                                    .sendJsonObject(JsonObject().put("workerId", workerId))
                                    .coAwait()
                                    .statusCode()
                            },
                        )
                        .awaitAll()
                    assertThreeTableInvariant(jobId)
                }
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.MINUTES)
    fun `concurrent complete and release race never violates invariant (stress)`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                repeat(stressRaceIterations) { i ->
                    val (job, workerId) =
                        createAndAcquireJob(groupId = "complete-release-stress-$i")
                    val jobId = job.getString("id")
                    listOf(
                            async {
                                client
                                    .post(port, "localhost", "/api/v1/jobs/$jobId/complete")
                                    .sendJsonObject(JsonObject().put("workerId", workerId))
                                    .coAwait()
                                    .statusCode()
                            },
                            async {
                                client
                                    .post(port, "localhost", "/api/v1/jobs/$jobId/release")
                                    .sendJsonObject(JsonObject().put("workerId", workerId))
                                    .coAwait()
                                    .statusCode()
                            },
                        )
                        .awaitAll()
                    assertThreeTableInvariant(jobId)
                }
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.MINUTES)
    fun `concurrent abort and acquire race never violates invariant (stress)`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val workerId = registerWorker()
                // Each iteration uses a fresh group so acquire only ever picks up its own job.
                repeat(stressRaceIterations) { i ->
                    val groupId = "abort-acquire-stress-$i"
                    val created = createJob(groupId = groupId)
                    val jobId = created.getString("id")
                    listOf(
                            async {
                                client
                                    .post(port, "localhost", "/api/v1/jobs/$jobId/abort")
                                    .send()
                                    .coAwait()
                                    .statusCode()
                            },
                            async {
                                runCatching { acquireJobs(workerId, 1, groupId = groupId) }
                                    .getOrElse { emptyList() }
                            },
                        )
                        .awaitAll()
                    assertThreeTableInvariant(jobId)
                }
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    // --- Concurrency race matrix (stress) ---
    //
    // Exhaustive pairwise coverage of the mutating job operations that can interleave on the SAME
    // job: { acquire, complete, release, error, abort, checkpoint, deregister }. Each runs the pair
    // concurrently for `stressRaceIterations` and asserts the three-table invariant + terminal
    // consistency after every round. `deregister` exercises the bulk reaper
    // (`releaseJobsByWorkerIds`)
    // synchronously — the same code the periodic dead-worker cleanup uses. Pairs whose
    // preconditions
    // can never overlap on one job (e.g. acquire vs complete — acquire needs PENDING, complete
    // needs
    // ACQUIRED and never enqueues) are intentionally omitted. `abort+release`, `abort+acquire` and
    // `complete+release` have dedicated stress tests above.

    // -- on a PENDING job --

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.MINUTES)
    fun `stress race - acquire vs acquire never double-acquires`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) =
        stressJobRace(
            vertx,
            testContext,
            "acquire-acquire",
            startAcquired = false,
            { fireAcquire(it.ownerWorkerId, it.groupId) },
            { fireAcquire(it.otherWorkerId, it.groupId) },
        )

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.MINUTES)
    fun `stress race - abort vs abort never violates invariant`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) =
        stressJobRace(
            vertx,
            testContext,
            "abort-abort",
            startAcquired = false,
            { fireAbort(it.jobId) },
            { fireAbort(it.jobId) },
        )

    // -- on an ACQUIRED job --

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.MINUTES)
    fun `stress race - acquire vs release never violates invariant`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) =
        stressJobRace(
            vertx,
            testContext,
            "acquire-release",
            startAcquired = true,
            { fireAcquire(it.otherWorkerId, it.groupId) },
            { fireRelease(it.jobId, it.ownerWorkerId) },
        )

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.MINUTES)
    fun `stress race - acquire vs error never violates invariant`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) =
        stressJobRace(
            vertx,
            testContext,
            "acquire-error",
            startAcquired = true,
            { fireAcquire(it.otherWorkerId, it.groupId) },
            { fireError(it.jobId, it.ownerWorkerId) },
        )

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.MINUTES)
    fun `stress race - acquire vs deregister never violates invariant`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) =
        stressJobRace(
            vertx,
            testContext,
            "acquire-deregister",
            startAcquired = true,
            { fireAcquire(it.otherWorkerId, it.groupId) },
            { fireDeregister(it.ownerWorkerId) },
        )

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.MINUTES)
    fun `stress race - complete vs abort never violates invariant`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) =
        stressJobRace(
            vertx,
            testContext,
            "complete-abort",
            startAcquired = true,
            { fireComplete(it.jobId, it.ownerWorkerId) },
            { fireAbort(it.jobId) },
        )

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.MINUTES)
    fun `stress race - complete vs error never violates invariant`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) =
        stressJobRace(
            vertx,
            testContext,
            "complete-error",
            startAcquired = true,
            { fireComplete(it.jobId, it.ownerWorkerId) },
            { fireError(it.jobId, it.ownerWorkerId) },
        )

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.MINUTES)
    fun `stress race - complete vs deregister never violates invariant`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) =
        stressJobRace(
            vertx,
            testContext,
            "complete-deregister",
            startAcquired = true,
            { fireComplete(it.jobId, it.ownerWorkerId) },
            { fireDeregister(it.ownerWorkerId) },
        )

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.MINUTES)
    fun `stress race - complete vs checkpoint never violates invariant`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) =
        stressJobRace(
            vertx,
            testContext,
            "complete-checkpoint",
            startAcquired = true,
            { fireComplete(it.jobId, it.ownerWorkerId) },
            { fireCheckpoint(it.jobId, it.ownerWorkerId) },
        )

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.MINUTES)
    fun `stress race - release vs error never violates invariant`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) =
        stressJobRace(
            vertx,
            testContext,
            "release-error",
            startAcquired = true,
            { fireRelease(it.jobId, it.ownerWorkerId) },
            { fireError(it.jobId, it.ownerWorkerId) },
        )

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.MINUTES)
    fun `stress race - release vs deregister never violates invariant`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) =
        stressJobRace(
            vertx,
            testContext,
            "release-deregister",
            startAcquired = true,
            { fireRelease(it.jobId, it.ownerWorkerId) },
            { fireDeregister(it.ownerWorkerId) },
        )

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.MINUTES)
    fun `stress race - release vs checkpoint never violates invariant`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) =
        stressJobRace(
            vertx,
            testContext,
            "release-checkpoint",
            startAcquired = true,
            { fireRelease(it.jobId, it.ownerWorkerId) },
            { fireCheckpoint(it.jobId, it.ownerWorkerId) },
        )

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.MINUTES)
    fun `stress race - error vs abort never violates invariant`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) =
        stressJobRace(
            vertx,
            testContext,
            "error-abort",
            startAcquired = true,
            { fireError(it.jobId, it.ownerWorkerId) },
            { fireAbort(it.jobId) },
        )

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.MINUTES)
    fun `stress race - error vs deregister never violates invariant`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) =
        stressJobRace(
            vertx,
            testContext,
            "error-deregister",
            startAcquired = true,
            { fireError(it.jobId, it.ownerWorkerId) },
            { fireDeregister(it.ownerWorkerId) },
        )

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.MINUTES)
    fun `stress race - error vs checkpoint never violates invariant`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) =
        stressJobRace(
            vertx,
            testContext,
            "error-checkpoint",
            startAcquired = true,
            { fireError(it.jobId, it.ownerWorkerId) },
            { fireCheckpoint(it.jobId, it.ownerWorkerId) },
        )

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.MINUTES)
    fun `stress race - abort vs deregister never violates invariant`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) =
        stressJobRace(
            vertx,
            testContext,
            "abort-deregister",
            startAcquired = true,
            { fireAbort(it.jobId) },
            { fireDeregister(it.ownerWorkerId) },
        )

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.MINUTES)
    fun `stress race - abort vs checkpoint never violates invariant`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) =
        stressJobRace(
            vertx,
            testContext,
            "abort-checkpoint",
            startAcquired = true,
            { fireAbort(it.jobId) },
            { fireCheckpoint(it.jobId, it.ownerWorkerId) },
        )

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.MINUTES)
    fun `stress race - deregister vs checkpoint never violates invariant`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) =
        stressJobRace(
            vertx,
            testContext,
            "deregister-checkpoint",
            startAcquired = true,
            { fireDeregister(it.ownerWorkerId) },
            { fireCheckpoint(it.jobId, it.ownerWorkerId) },
        )

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.MINUTES)
    fun `stress race - deregister vs deregister never violates invariant`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) =
        stressJobRace(
            vertx,
            testContext,
            "deregister-deregister",
            startAcquired = true,
            { fireDeregister(it.ownerWorkerId) },
            { fireDeregister(it.ownerWorkerId) },
        )

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.MINUTES)
    fun `stress race - checkpoint vs checkpoint never violates invariant`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) =
        stressJobRace(
            vertx,
            testContext,
            "checkpoint-checkpoint",
            startAcquired = true,
            { fireCheckpoint(it.jobId, it.ownerWorkerId) },
            { fireCheckpoint(it.jobId, it.ownerWorkerId) },
        )

    // -- worker-lifecycle eviction races (reaper) on the same worker --
    //
    // deregister (client) and the periodic dead-worker cleanup both release the worker's jobs and
    // delete the worker. Concurrent lifecycle ops on the SAME worker must serialise on the worker
    // row
    // (deregister locks it FOR UPDATE; cleanup locks dead rows FOR UPDATE SKIP LOCKED) —
    // without
    // that they deadlock on the worker-row vs jobs-row lock order.

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.MINUTES)
    fun `stress race - deregister vs cleanup never violates invariant`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) =
        stressWorkerEvictionRace(
            vertx,
            testContext,
            "deregister-cleanup",
            { fireDeregister(it) },
            { fireCleanup() },
        )

    @Test
    @Timeout(value = 10, timeUnit = TimeUnit.MINUTES)
    fun `stress race - cleanup vs cleanup never violates invariant`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) =
        stressWorkerEvictionRace(
            vertx,
            testContext,
            "cleanup-cleanup",
            { fireCleanup() },
            { fireCleanup() },
        )

    // -- worker eviction must DRAIN a worker whose job is concurrently locked (no FK violation) --
    //
    // The reaper must fully release every one of the worker's jobs before the FK-constrained worker
    // delete. If a job's `jobs` row is held by an in-flight transition (e.g. a checkpoint, which
    // keeps the job_acquired row), the reaper must block and drain it — not skip it and then
    // violate
    // fk_job_acquired_worker on deleteWorker(s). These tests hold the lock from a side connection,
    // run the eviction, release, and assert it SUCCEEDS (and the job is released, worker gone).

    @Test
    @Timeout(value = 2, timeUnit = TimeUnit.MINUTES)
    fun `deregister drains a worker whose job is concurrently locked`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val (job, workerId) = createAndAcquireJob()
                val jobId = job.getString("id")
                val status =
                    evictWhileJobLocked(jobId) {
                        client
                            .delete(port, "localhost", "/api/v1/workers/$workerId")
                            .send()
                            .coAwait()
                            .statusCode()
                    }
                assertThat(status)
                    .describedAs(
                        "deregister should block on the locked job, drain it, and delete the worker"
                    )
                    .isEqualTo(204)
                assertThreeTableInvariant(jobId)
                assertThat(readQueueAvailableAt(jobId))
                    .describedAs("job should be released back to the queue")
                    .isNotNull()
                assertThat(workerExists(workerId)).describedAs("worker should be deleted").isFalse()
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    @Timeout(value = 2, timeUnit = TimeUnit.MINUTES)
    fun `cleanup drains a dead worker whose job is concurrently locked`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val (job, workerId) = createAndAcquireJob()
                val jobId = job.getString("id")
                markWorkerDead(workerId)
                evictWhileJobLocked(jobId) { verticle.runDeadWorkerCleanup() }
                assertThreeTableInvariant(jobId)
                assertThat(readQueueAvailableAt(jobId))
                    .describedAs("job should be released back to the queue")
                    .isNotNull()
                assertThat(workerExists(workerId)).describedAs("worker should be evicted").isFalse()
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    /**
     * Holds a `FOR UPDATE` lock on [jobId]'s `jobs` row from a side connection (simulating an
     * in-flight transition that keeps the `job_acquired` row), runs [eviction] concurrently, then
     * releases the lock so a correctly-blocking eviction can drain and complete. Returns whatever
     * [eviction] produced.
     */
    private suspend fun <T> evictWhileJobLocked(jobId: String, eviction: suspend () -> T): T {
        val holder = backend.getJdbcConnection().also { it.autoCommit = false }
        return try {
            holder.prepareStatement("SELECT 1 FROM jobs WHERE id = ? FOR UPDATE").use { st ->
                st.setString(1, jobId)
                st.executeQuery().use { it.next() }
            }
            coroutineScope {
                val running = async { eviction() }
                // Give the eviction time to reach (and block on) the locked job, then release it.
                delay(500.milliseconds)
                holder.rollback()
                running.await()
            }
        } finally {
            runCatching { holder.rollback() }
            holder.close()
        }
    }

    private fun workerExists(workerId: String): Boolean =
        assertionConn().prepareStatement("SELECT 1 FROM workers WHERE id = ?").use { s ->
            s.setString(1, workerId)
            s.executeQuery().use { it.next() }
        }

    /**
     * Context handed to each race operation: the job under test plus the workers/group to act as.
     */
    private data class RaceCtx(
        val jobId: String,
        val ownerWorkerId: String,
        val otherWorkerId: String,
        val groupId: String,
    )

    /**
     * Runs [opA] and [opB] concurrently against the same job for [stressRaceIterations] rounds,
     * asserting the three-table invariant and terminal consistency after each. Each round gets a
     * fresh group (so acquire only ever sees its own job) and a fresh owner worker (so `deregister`
     * ops, which delete the worker, don't poison later rounds); a single shared "other" worker
     * plays the second actor for two-worker races. Operations fire-and-tolerate — a race loser
     * legitimately gets a 4xx; only the resulting state is asserted.
     */
    private fun stressJobRace(
        vertx: Vertx,
        testContext: VertxTestContext,
        label: String,
        startAcquired: Boolean,
        opA: suspend (RaceCtx) -> Unit,
        opB: suspend (RaceCtx) -> Unit,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val otherWorker = registerWorker()
                repeat(stressRaceIterations) { i ->
                    val groupId = "$label-$i"
                    val owner = registerWorker()
                    val jobId = createJob(groupId = groupId).getString("id")
                    if (startAcquired) acquireJobs(owner, 1, groupId = groupId)
                    val ctx = RaceCtx(jobId, owner, otherWorker, groupId)
                    listOf(async { runCatching { opA(ctx) } }, async { runCatching { opB(ctx) } })
                        .awaitAll()
                    assertThreeTableInvariant(jobId)
                    assertTerminalConsistency(jobId)
                }
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    // Fire-and-ignore-status variants of the operations, for use inside races (a race loser may get
    // a 4xx; the runner asserts the resulting state, not the status code).
    private suspend fun fireAcquire(workerId: String, groupId: String) {
        client
            .post(port, "localhost", "/api/v1/jobs/acquire")
            .sendJsonObject(
                JsonObject()
                    .put("workerId", workerId)
                    .put("limit", 1)
                    .put("groupId", groupId)
                    .put("type", "render")
            )
            .coAwait()
    }

    private suspend fun fireComplete(jobId: String, workerId: String) {
        client
            .post(port, "localhost", "/api/v1/jobs/$jobId/complete")
            .sendJsonObject(JsonObject().put("workerId", workerId))
            .coAwait()
    }

    private suspend fun fireRelease(jobId: String, workerId: String) {
        client
            .post(port, "localhost", "/api/v1/jobs/$jobId/release")
            .sendJsonObject(JsonObject().put("workerId", workerId))
            .coAwait()
    }

    private suspend fun fireError(jobId: String, workerId: String) {
        client
            .post(port, "localhost", "/api/v1/jobs/$jobId/error")
            .sendJsonObject(JsonObject().put("workerId", workerId))
            .coAwait()
    }

    private suspend fun fireAbort(jobId: String) {
        client.post(port, "localhost", "/api/v1/jobs/$jobId/abort").send().coAwait()
    }

    private suspend fun fireCheckpoint(jobId: String, workerId: String) {
        client
            .post(port, "localhost", "/api/v1/jobs/$jobId/checkpoints")
            .sendJsonObject(
                JsonObject()
                    .put("workerId", workerId)
                    .put("name", "cp")
                    .put("data", Base64.getEncoder().encodeToString("d".toByteArray()))
            )
            .coAwait()
    }

    private suspend fun fireDeregister(workerId: String) {
        client.delete(port, "localhost", "/api/v1/workers/$workerId").send().coAwait()
    }

    private suspend fun fireCleanup() {
        verticle.runDeadWorkerCleanup()
    }

    /**
     * Drives concurrent worker-lifecycle eviction on the SAME worker: each round acquires a job,
     * then marks the worker dead (so the dead-worker cleanup will reclaim it), then races
     * [opA]/[opB] (each a deregister or a cleanup pass). Whoever wins releases the job back to the
     * queue; the loser skips (cleanup) or 404s (deregister). Asserts the three-table invariant
     * holds (no deadlock, no dual state) every round.
     */
    private fun stressWorkerEvictionRace(
        vertx: Vertx,
        testContext: VertxTestContext,
        label: String,
        opA: suspend (workerId: String) -> Unit,
        opB: suspend (workerId: String) -> Unit,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                repeat(stressRaceIterations) { i ->
                    val groupId = "$label-$i"
                    val worker = registerWorker()
                    val jobId = createJob(groupId = groupId).getString("id")
                    acquireJobs(worker, 1, groupId = groupId)
                    markWorkerDead(worker)
                    listOf(
                            async { runCatching { opA(worker) } },
                            async { runCatching { opB(worker) } },
                        )
                        .awaitAll()
                    assertThreeTableInvariant(jobId)
                    assertTerminalConsistency(jobId)
                }
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    /**
     * Backdates a worker's heartbeat so it is past its `sessionTimeout` — i.e. dead — making it
     * eligible for the dead-worker cleanup ([WorkerGateway.findAndLockDeadWorkers]).
     */
    private fun markWorkerDead(workerId: String) {
        assertionConn()
            .prepareStatement("UPDATE workers SET last_heartbeat_at = 0 WHERE id = ?")
            .use { s ->
                s.setString(1, workerId)
                s.executeUpdate()
            }
    }

    /**
     * Overwrites a queued job's `enqueued_at` so a test can control FIFO order independently of the
     * order the jobs were created (and of the rows' physical/scan order).
     */
    private fun setEnqueuedAt(jobId: String, enqueuedAt: Long) {
        assertionConn()
            .prepareStatement("UPDATE job_queue SET enqueued_at = ? WHERE job_id = ?")
            .use { s ->
                s.setLong(1, enqueuedAt)
                s.setString(2, jobId)
                s.executeUpdate()
            }
    }

    // --- Helpers ---

    private suspend fun registerWorker(workerId: String = UUID.randomUUID().toString()): String {
        val response =
            client
                .post(port, "localhost", "/api/v1/workers")
                .sendJsonObject(
                    JsonObject()
                        .put("workerId", workerId)
                        .put("heartbeatTimeout", 5000)
                        // Long session so a worker stays alive across a whole (multi-second) test
                        // without heartbeating; tests that need a dead worker use markWorkerDead().
                        .put("sessionTimeout", 3_600_000)
                )
                .coAwait()
        assertThat(response.statusCode()).isEqualTo(201)
        return response.bodyAsJsonObject().getString("id")
    }

    private suspend fun createJob(
        name: String = "test-job",
        type: String = "render",
        maxRetries: Int? = null,
        idempotencyKey: String = UUID.randomUUID().toString(),
        inputData: String? = null,
        groupId: String = "test-group",
    ): JsonObject {
        val body =
            JsonObject()
                .put("name", name)
                .put("type", type)
                .put("groupId", groupId)
                .put("idempotencyKey", idempotencyKey)
        if (maxRetries != null) body.put("maxRetries", maxRetries)
        if (inputData != null) body.put("inputData", inputData)
        val response = client.post(port, "localhost", "/api/v1/jobs").sendJsonObject(body).coAwait()
        assertThat(response.statusCode()).isEqualTo(201)
        val json = response.bodyAsJsonObject()
        val id = json.getString("id")
        assertTerminalConsistency(id)
        assertThreeTableInvariant(id)
        return json
    }

    private suspend fun acquireJobs(
        workerId: String,
        limit: Int = 10,
        groupId: String = "test-group",
        type: String = "render",
    ): List<JsonObject> {
        val response =
            client
                .post(port, "localhost", "/api/v1/jobs/acquire")
                .sendJsonObject(
                    JsonObject()
                        .put("workerId", workerId)
                        .put("limit", limit)
                        .put("groupId", groupId)
                        .put("type", type)
                )
                .coAwait()
        assertThat(response.statusCode()).isEqualTo(200)
        val jobs = response.bodyAsJsonArray().map { it as JsonObject }
        jobs.forEach {
            val id = it.getString("id")
            assertTerminalConsistency(id)
            assertThreeTableInvariant(id)
        }
        return jobs
    }

    private suspend fun saveCheckpoint(
        jobId: String,
        workerId: String,
        previousCheckpointId: String? = null,
        name: String? = "test checkpoint",
        data: String? = Base64.getEncoder().encodeToString("test-data".toByteArray()),
        includeData: Boolean = true,
    ): JsonObject {
        val body = JsonObject().put("workerId", workerId)
        if (includeData) body.put("data", data)
        if (previousCheckpointId != null) body.put("previousCheckpointId", previousCheckpointId)
        if (name != null) body.put("name", name)
        val response =
            client
                .post(port, "localhost", "/api/v1/jobs/$jobId/checkpoints")
                .sendJsonObject(body)
                .coAwait()
        assertThat(response.statusCode()).isEqualTo(201)
        assertTerminalConsistency(jobId)
        assertThreeTableInvariant(jobId)
        return response.bodyAsJsonObject()
    }

    private suspend fun getCheckpoints(
        jobId: String,
        after: String? = null,
        limit: Int? = null,
    ): JsonObject {
        var uri = "/api/v1/jobs/$jobId/checkpoints"
        val params = mutableListOf<String>()
        if (after != null) params.add("after=$after")
        if (limit != null) params.add("limit=$limit")
        if (params.isNotEmpty()) uri += "?" + params.joinToString("&")
        val response = client.get(port, "localhost", uri).send().coAwait()
        assertThat(response.statusCode()).isEqualTo(200)
        return response.bodyAsJsonObject()
    }

    private suspend fun completeJob(
        jobId: String,
        workerId: String,
        outputData: String? = null,
    ): JsonObject {
        val body = JsonObject().put("workerId", workerId)
        if (outputData != null) body.put("outputData", outputData)
        val response =
            client
                .post(port, "localhost", "/api/v1/jobs/$jobId/complete")
                .sendJsonObject(body)
                .coAwait()
        assertThat(response.statusCode()).isEqualTo(200)
        assertTerminalConsistency(jobId)
        assertThreeTableInvariant(jobId)
        return response.bodyAsJsonObject()
    }

    private suspend fun releaseJob(
        jobId: String,
        workerId: String,
        availableAt: Long? = null,
    ): JsonObject {
        val body = JsonObject().put("workerId", workerId)
        if (availableAt != null) body.put("availableAt", availableAt)
        val response =
            client
                .post(port, "localhost", "/api/v1/jobs/$jobId/release")
                .sendJsonObject(body)
                .coAwait()
        assertThat(response.statusCode()).isEqualTo(200)
        assertTerminalConsistency(jobId)
        assertThreeTableInvariant(jobId)
        return response.bodyAsJsonObject()
    }

    private suspend fun abortJob(jobId: String): JsonObject {
        val response = client.post(port, "localhost", "/api/v1/jobs/$jobId/abort").send().coAwait()
        assertThat(response.statusCode()).isEqualTo(200)
        assertTerminalConsistency(jobId)
        assertThreeTableInvariant(jobId)
        return response.bodyAsJsonObject()
    }

    private suspend fun reportError(
        jobId: String,
        workerId: String,
        error: String? = null,
    ): JsonObject {
        val body = JsonObject().put("workerId", workerId)
        if (error != null) body.put("error", error)
        val response =
            client
                .post(port, "localhost", "/api/v1/jobs/$jobId/error")
                .sendJsonObject(body)
                .coAwait()
        assertThat(response.statusCode()).isEqualTo(200)
        assertTerminalConsistency(jobId)
        assertThreeTableInvariant(jobId)
        return response.bodyAsJsonObject()
    }

    private suspend fun createAndAcquireJob(
        inputData: String? = null,
        groupId: String = "test-group",
        type: String = "render",
    ): Pair<JsonObject, String> {
        val workerId = registerWorker()
        createJob(inputData = inputData, groupId = groupId, type = type)
        val acquired = acquireJobs(workerId, 1, groupId = groupId, type = type)
        assertThat(acquired).hasSize(1)
        return acquired[0] to workerId
    }

    private fun insertCheckpointDirectly(
        id: String,
        jobId: String,
        previousCheckpointId: String?,
        createdAt: Long,
        orderKey: Long,
    ) {
        backend.getJdbcConnection().use { conn ->
            conn
                .prepareStatement(
                    "INSERT INTO checkpoints (id, job_id, previous_checkpoint_id, name, created_at, order_key, data) VALUES (?, ?, ?, ?, ?, ?, ?)"
                )
                .use { stmt ->
                    stmt.setString(1, id)
                    stmt.setString(2, jobId)
                    stmt.setString(3, previousCheckpointId)
                    stmt.setString(4, null)
                    stmt.setLong(5, createdAt)
                    stmt.setLong(6, orderKey)
                    stmt.setBytes(7, "data".toByteArray())
                    stmt.executeUpdate()
                }
        }
    }
}
