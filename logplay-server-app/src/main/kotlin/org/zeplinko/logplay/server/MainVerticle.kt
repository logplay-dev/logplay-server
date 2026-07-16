package org.zeplinko.logplay.server

import com.fasterxml.jackson.module.kotlin.KotlinModule
import io.vertx.core.json.Json
import io.vertx.core.json.jackson.DatabindCodec
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.BodyHandler
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.coAwait
import io.vertx.kotlin.coroutines.coroutineRouter
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import org.zeplinko.logplay.server.config.AppConfig
import org.zeplinko.logplay.server.core.Metrics
import org.zeplinko.logplay.server.core.NoopMetrics
import org.zeplinko.logplay.server.core.UnitOfWork
import org.zeplinko.logplay.server.core.job.*
import org.zeplinko.logplay.server.core.worker.*
import org.zeplinko.logplay.server.job.use.case.JobUseCaseLookUp
import org.zeplinko.logplay.server.job.web.JobController
import org.zeplinko.logplay.server.telemetry.HttpMetrics
import org.zeplinko.logplay.server.telemetry.OtelMetrics
import org.zeplinko.logplay.server.telemetry.Telemetry
import org.zeplinko.logplay.server.web.ErrorResponse
import org.zeplinko.logplay.server.web.InvalidQueryParameterException
import org.zeplinko.logplay.server.web.InvalidRequestBodyException
import org.zeplinko.logplay.server.worker.use.case.WorkerUseCaseLookUp
import org.zeplinko.logplay.server.worker.web.WorkerController

class MainVerticle(
    private val jobGateway: JobGateway,
    private val workerGateway: WorkerGateway,
    private val unitOfWork: UnitOfWork,
    private val appConfig: AppConfig,
) : CoroutineVerticle() {

    private val logger = LoggerFactory.getLogger(MainVerticle::class.java)

    private lateinit var jobUseCases: JobUseCaseLookUp
    private lateinit var workerUseCases: WorkerUseCaseLookUp
    private lateinit var jobController: JobController
    private lateinit var workerController: WorkerController
    private var httpMetrics: HttpMetrics? = null

    var actualPort: Int = -1
        private set

    /**
     * Runs a single dead-worker cleanup pass and returns the number of workers affected. The single
     * entry point for a cleanup pass: the periodic timer ([start]) invokes it, and it is also
     * public so tests and operators can drive it deterministically instead of waiting for the
     * timer.
     */
    suspend fun runDeadWorkerCleanup(): Int = workerUseCases.cleanupDeadWorkersUseCase.execute()

    override suspend fun start() {
        val metrics = buildMetrics()
        jobUseCases = JobUseCaseLookUp(jobGateway, workerGateway, unitOfWork, metrics)
        workerUseCases = WorkerUseCaseLookUp(workerGateway, jobGateway, unitOfWork)
        jobController = JobController(jobUseCases)
        workerController = WorkerController(workerUseCases)

        DatabindCodec.mapper().registerModule(KotlinModule.Builder().build())

        val router = Router.router(vertx)
        httpMetrics?.let { hm -> router.route().handler(hm::handle) }
        val bodyHandler = BodyHandler.create()
        if (appConfig.maxBodyBytes >= 0) bodyHandler.setBodyLimit(appConfig.maxBodyBytes)
        router.route().handler(bodyHandler)

        val v1 = Router.router(vertx)
        coroutineRouter {
            jobController.registerRoutes(v1, this)
            workerController.registerRoutes(v1, this)
        }
        v1.route().failureHandler(::handleFailure)
        router.route("/api/v1/*").subRouter(v1)

        val server =
            vertx
                .createHttpServer()
                .requestHandler(router)
                .listen(appConfig.httpPort, appConfig.httpHost)
                .coAwait()
        actualPort = server.actualPort()
        logger.info("HTTP server started on {}:{}", appConfig.httpHost, actualPort)

        vertx.setPeriodic(appConfig.cleanupIntervalMs) {
            launch {
                try {
                    runDeadWorkerCleanup()
                } catch (e: Exception) {
                    logger.error("Dead worker cleanup failed", e)
                }
            }
        }
    }

    /**
     * Builds the domain [Metrics] implementation from config. Off by default; when
     * `metrics.enabled=true`, installs the OpenTelemetry SDK (autoconfigured via standard `OTEL_*`
     * settings) and also wires the per-request HTTP metrics handler.
     */
    private fun buildMetrics(): Metrics {
        if (!appConfig.metrics.enabled) return NoopMetrics
        val serviceName = appConfig.metrics.serviceName
        Telemetry.install(serviceName, appConfig.metrics.otlpEndpoint)
        val meter = Telemetry.meter()
        httpMetrics = HttpMetrics(meter)
        logger.info("OpenTelemetry metrics enabled (service={})", serviceName)
        return OtelMetrics(meter)
    }

    private fun handleFailure(rc: RoutingContext) {
        val failure = rc.failure()
        val (statusCode, message) =
            when (failure) {
                // 400 — Bad Request
                is BlankGroupIdException,
                is InvalidGroupIdException,
                is BlankJobTypeException,
                is InvalidJobTypeException,
                is InvalidJobNameException,
                is BlankJobIdException,
                is BlankWorkerIdException,
                is InvalidWorkerIdException,
                is InvalidCheckpointNameException,
                is InvalidCheckpointDataException,
                is InvalidJobInputDataException,
                is InvalidJobOutputDataException,
                is InvalidMaxRetriesException,
                is BlankIdempotencyKeyException,
                is InvalidIdempotencyKeyException,
                is InvalidLimitException,
                is InvalidWorkerTimeoutException,
                is InvalidRequestBodyException,
                is InvalidQueryParameterException -> 400 to failure.message

                // 404 — Not Found
                is JobNotFoundException,
                is CheckpointNotFoundException,
                is WorkerNotFoundException -> 404 to failure.message

                // 409 — Conflict
                is DuplicateIdempotencyKeyException,
                is JobNotAcquiredException,
                is JobNotAbortableException,
                is JobNotOwnedByWorkerException,
                is InvalidCheckpointOrderException,
                is WorkerAlreadyRegisteredException -> 409 to failure.message

                // 500 — Internal Server Error
                else -> {
                    logger.error(
                        "Unhandled error: {} {}",
                        rc.request().method(),
                        rc.request().path(),
                        failure,
                    )
                    500 to "Internal server error"
                }
            }
        rc.response()
            .setStatusCode(statusCode)
            .putHeader("content-type", "application/json")
            .end(Json.encode(ErrorResponse(message)))
    }
}
