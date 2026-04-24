package dev.logplay.server

import com.fasterxml.jackson.module.kotlin.KotlinModule
import dev.logplay.server.core.job.*
import dev.logplay.server.core.worker.*
import dev.logplay.server.job.use.case.JobUseCaseLookUp
import dev.logplay.server.job.web.JobController
import dev.logplay.server.web.ErrorResponse
import dev.logplay.server.web.InvalidQueryParameterException
import dev.logplay.server.web.InvalidRequestBodyException
import dev.logplay.server.worker.use.case.WorkerUseCaseLookUp
import dev.logplay.server.worker.web.WorkerController
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

class MainVerticle(private val jobGateway: JobGateway, private val workerGateway: WorkerGateway) :
    CoroutineVerticle() {

    private val logger = LoggerFactory.getLogger(MainVerticle::class.java)

    private lateinit var jobUseCases: JobUseCaseLookUp
    private lateinit var workerUseCases: WorkerUseCaseLookUp
    private lateinit var jobController: JobController
    private lateinit var workerController: WorkerController

    var actualPort: Int = -1
        private set

    override suspend fun start() {
        val condemnPeriodMs = config.getLong("cleanup.condemn.period.ms", 15000L)
        jobUseCases = JobUseCaseLookUp(jobGateway, workerGateway)
        workerUseCases = WorkerUseCaseLookUp(workerGateway, jobGateway, condemnPeriodMs)
        jobController = JobController(jobUseCases)
        workerController = WorkerController(workerUseCases)

        DatabindCodec.mapper().registerModule(KotlinModule.Builder().build())

        val router = Router.router(vertx)
        router.route().handler(BodyHandler.create())

        val v1 = Router.router(vertx)
        coroutineRouter {
            jobController.registerRoutes(v1, this)
            workerController.registerRoutes(v1, this)
        }
        v1.route().failureHandler(::handleFailure)
        router.route("/api/v1/*").subRouter(v1)

        val port = config.getInteger("http.port", 8080)
        val server = vertx.createHttpServer().requestHandler(router).listen(port).coAwait()
        actualPort = server.actualPort()
        logger.info("HTTP server started on port {}", actualPort)

        val cleanupIntervalMs = config.getLong("cleanup.interval.ms", 30000L)
        vertx.setPeriodic(cleanupIntervalMs) {
            launch {
                try {
                    workerUseCases.cleanupDeadWorkersUseCase.execute()
                } catch (e: Exception) {
                    logger.error("Dead worker cleanup failed", e)
                }
            }
        }
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

                // 403 — Forbidden
                is WorkerCondemnedException -> 403 to failure.message

                // 409 — Conflict
                is DuplicateIdempotencyKeyException,
                is JobNotAcquiredException,
                is JobNotAbortableException,
                is JobNotOwnedByWorkerException,
                is JobConcurrentModificationException,
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
