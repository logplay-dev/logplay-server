package org.zeplinko.logplay.server.test

import io.vertx.core.DeploymentOptions
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.client.WebClient
import io.vertx.junit5.VertxExtension
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.coroutines.coAwait
import io.vertx.kotlin.coroutines.dispatcher
import java.util.Base64
import java.util.UUID
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.launch
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.*
import org.junit.jupiter.api.extension.ExtendWith
import org.zeplinko.logplay.server.MainVerticle

@ExtendWith(VertxExtension::class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class AbstractIntegrationTest {

    protected abstract fun createBackend(): IntegrationTestBackend

    private lateinit var backend: IntegrationTestBackend
    private lateinit var client: WebClient
    private lateinit var verticle: MainVerticle
    private var port: Int = -1

    @BeforeAll
    fun setUp(vertx: Vertx, testContext: VertxTestContext) {
        backend = createBackend()
        backend.initDatabase()

        val (jobGateway, workerGateway) = backend.createGateways(vertx)
        verticle = MainVerticle(jobGateway, workerGateway)
        val options =
            DeploymentOptions()
                .setConfig(JsonObject().put("http.port", 0).put("cleanup.interval.ms", 600000L))
        vertx
            .deployVerticle(verticle, options)
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
                stmt.execute("DELETE FROM job_events")
                stmt.execute("DELETE FROM checkpoints")
                stmt.execute("DELETE FROM jobs")
                stmt.execute("DELETE FROM workers")
            }
        }
    }

    @AfterAll
    fun tearDown(vertx: Vertx, testContext: VertxTestContext) {
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
                assertThat(body.getLong("version")).isEqualTo(1L)
                assertThat(body.getString("createdAt")).isNotBlank()
                assertThat(body.getString("updatedAt")).isNotBlank()
                assertThat(body.getString("lastAcquiredAt")).isNull()
                assertThat(body.getString("acquiredByWorkerId")).isNull()
                assertThat(body.getString("groupId")).isEqualTo("test-group")
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
    fun `should complete an acquired job`(vertx: Vertx, testContext: VertxTestContext) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val (job, workerId) = createAndAcquireJob()
                val completed = completeJob(job.getString("id"), workerId)
                assertThat(completed.getString("status")).isEqualTo("FINISHED")
                assertThat(completed.getLong("version")).isGreaterThan(job.getLong("version"))
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
                assertThat(released.getLong("version")).isGreaterThan(job.getLong("version"))
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
                assertThat(reacquired[0].getLong("version")).isGreaterThan(job.getLong("version"))
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
                assertThat(result.getInteger("retries")).isEqualTo(1)
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should return 409 when reporting error on non-acquired job`(
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
    fun `should return idempotencyKey in job response`(
        vertx: Vertx,
        testContext: VertxTestContext,
    ) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                val job = createJob("test", "render", idempotencyKey = "my-key-123")
                assertThat(job.getString("idempotencyKey")).isEqualTo("my-key-123")
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
                assertThat(afterError2.getInteger("retries")).isEqualTo(2)

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
    fun `should return 409 when aborting a finished job`(
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
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should return 409 when aborting an already aborted job`(
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
    fun `should return 409 when registering a duplicate worker`(
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
    fun `should return 409 when completing a pending job`(
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
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should return 409 when releasing a pending job`(
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
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should return 409 when saving checkpoint on a pending job`(
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
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
        }
    }

    @Test
    fun `should return 409 when completing an already finished job`(
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
                testContext.completeNow()
            } catch (e: Throwable) {
                testContext.failNow(e)
            }
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
                        .put("sessionTimeout", 15000)
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
        return response.bodyAsJsonObject()
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
        return response.bodyAsJsonArray().map { it as JsonObject }
    }

    private suspend fun saveCheckpoint(
        jobId: String,
        workerId: String,
        previousCheckpointId: String? = null,
        name: String? = "test checkpoint",
        data: String = Base64.getEncoder().encodeToString("test-data".toByteArray()),
    ): JsonObject {
        val body = JsonObject().put("workerId", workerId).put("data", data)
        if (previousCheckpointId != null) body.put("previousCheckpointId", previousCheckpointId)
        if (name != null) body.put("name", name)
        val response =
            client
                .post(port, "localhost", "/api/v1/jobs/$jobId/checkpoints")
                .sendJsonObject(body)
                .coAwait()
        assertThat(response.statusCode()).isEqualTo(201)
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
        return response.bodyAsJsonObject()
    }

    private suspend fun releaseJob(jobId: String, workerId: String): JsonObject {
        val response =
            client
                .post(port, "localhost", "/api/v1/jobs/$jobId/release")
                .sendJsonObject(JsonObject().put("workerId", workerId))
                .coAwait()
        assertThat(response.statusCode()).isEqualTo(200)
        return response.bodyAsJsonObject()
    }

    private suspend fun abortJob(jobId: String): JsonObject {
        val response = client.post(port, "localhost", "/api/v1/jobs/$jobId/abort").send().coAwait()
        assertThat(response.statusCode()).isEqualTo(200)
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
