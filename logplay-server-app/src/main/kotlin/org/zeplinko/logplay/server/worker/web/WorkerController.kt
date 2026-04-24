package org.zeplinko.logplay.server.worker.web

import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.coroutines.CoroutineRouterSupport
import org.zeplinko.logplay.server.core.worker.*
import org.zeplinko.logplay.server.web.parseBody
import org.zeplinko.logplay.server.worker.use.case.WorkerUseCaseLookUp

class WorkerController(private val useCases: WorkerUseCaseLookUp) {

    fun registerRoutes(router: Router, support: CoroutineRouterSupport): Unit =
        with(support) {
            router.post("/workers").coHandler(requestHandler = ::registerWorker)
            router.post("/workers/:workerId/heartbeat").coHandler(requestHandler = ::heartbeat)
            router.delete("/workers/:workerId").coHandler(requestHandler = ::deregisterWorker)
        }

    private suspend fun registerWorker(rc: RoutingContext) {
        val request = rc.parseBody<RegisterWorkerRequest>()
        val worker = useCases.registerWorkerUseCase.execute(request.toCommand())
        rc.response().statusCode = 201
        rc.json(worker.toResponse())
    }

    private suspend fun heartbeat(rc: RoutingContext) {
        val workerId = rc.pathParam("workerId")!!
        val worker = useCases.heartbeatWorkerUseCase.execute(HeartbeatWorkerCommand(workerId))
        rc.json(worker.toResponse())
    }

    private suspend fun deregisterWorker(rc: RoutingContext) {
        val workerId = rc.pathParam("workerId")!!
        useCases.deregisterWorkerUseCase.execute(DeregisterWorkerCommand(workerId))
        rc.response().setStatusCode(204).end()
    }
}
