package dev.logplay.server

import dev.logplay.server.core.job.JobGateway
import dev.logplay.server.job.use.case.JobUseCaseLookUp
import dev.logplay.server.job.web.JobController
import io.vertx.core.Future
import io.vertx.core.VerticleBase

class MainVerticle(jobGateway: JobGateway) : VerticleBase() {

    private val useCases = JobUseCaseLookUp(jobGateway)
    private val controller = JobController(useCases)

    override fun start(): Future<*> {
        return vertx
            .createHttpServer()
            .requestHandler { req ->
                req.response().putHeader("content-type", "text/plain").end("Hello from Vert.x!")
            }
            .listen(8888)
            .onSuccess { println("HTTP server started on port 8888") }
    }
}
