package org.zeplinko.logplay.server.job.web

import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.coroutines.CoroutineRouterSupport
import org.zeplinko.logplay.server.core.job.*
import org.zeplinko.logplay.server.job.use.case.JobUseCaseLookUp
import org.zeplinko.logplay.server.web.InvalidQueryParameterException
import org.zeplinko.logplay.server.web.parseBody

class JobController(private val useCases: JobUseCaseLookUp) {

    fun registerRoutes(router: Router, support: CoroutineRouterSupport): Unit =
        with(support) {
            router.post("/jobs").coHandler(requestHandler = ::createJob)
            router.post("/jobs/acquire").coHandler(requestHandler = ::acquireJobs)
            router.get("/jobs/:jobId/checkpoints").coHandler(requestHandler = ::getCheckpoints)
            router.post("/jobs/:jobId/checkpoints").coHandler(requestHandler = ::saveCheckpoint)
            router.post("/jobs/:jobId/complete").coHandler(requestHandler = ::completeJob)
            router.post("/jobs/:jobId/release").coHandler(requestHandler = ::releaseJob)
            router.post("/jobs/:jobId/error").coHandler(requestHandler = ::reportError)
            router.post("/jobs/:jobId/abort").coHandler(requestHandler = ::abortJob)
            router.get("/jobs/:jobId/events").coHandler(requestHandler = ::getEvents)
        }

    private suspend fun createJob(rc: RoutingContext) {
        val request = rc.parseBody<CreateJobRequest>()
        val job = useCases.createJobUseCase.execute(request.toCommand())
        rc.response().statusCode = 201
        rc.json(job.toResponse())
    }

    private suspend fun acquireJobs(rc: RoutingContext) {
        val request = rc.parseBody<AcquireJobsRequest>()
        val jobs = useCases.acquirePendingJobsUseCase.execute(request.toCommand())
        rc.json(jobs.map { it.toResponse() })
    }

    private suspend fun getCheckpoints(rc: RoutingContext) {
        val jobId = rc.pathParam("jobId")!!
        val after = rc.queryParam("after").firstOrNull()
        val limitParam = rc.queryParam("limit").firstOrNull()
        val limit =
            if (limitParam != null)
                limitParam.toIntOrNull()
                    ?: throw InvalidQueryParameterException("limit", "must be a valid integer")
            else null
        val command = GetCheckpointsCommand(jobId = jobId, after = after, limit = limit)
        val page = useCases.getCheckpointsUseCase.execute(command)
        rc.json(page.toResponse())
    }

    private suspend fun saveCheckpoint(rc: RoutingContext) {
        val jobId = rc.pathParam("jobId")!!
        val request = rc.parseBody<SaveCheckpointRequest>()
        val checkpoint = useCases.saveJobCheckpointUseCase.execute(request.toCommand(jobId))
        rc.response().statusCode = 201
        rc.json(checkpoint.toResponse())
    }

    private suspend fun completeJob(rc: RoutingContext) {
        val jobId = rc.pathParam("jobId")!!
        val request = rc.parseBody<CompleteJobRequest>()
        val job = useCases.completeJobUseCase.execute(request.toCommand(jobId))
        rc.json(job.toResponse())
    }

    private suspend fun releaseJob(rc: RoutingContext) {
        val jobId = rc.pathParam("jobId")!!
        val request = rc.parseBody<ReleaseJobRequest>()
        val job = useCases.releaseJobUseCase.execute(request.toCommand(jobId))
        rc.json(job.toResponse())
    }

    private suspend fun reportError(rc: RoutingContext) {
        val jobId = rc.pathParam("jobId")!!
        val request = rc.parseBody<ReportExecutionErrorRequest>()
        val job = useCases.reportExecutionErrorUseCase.execute(request.toCommand(jobId))
        rc.json(job.toResponse())
    }

    private suspend fun abortJob(rc: RoutingContext) {
        val jobId = rc.pathParam("jobId")!!
        val job = useCases.abortJobUseCase.execute(AbortJobCommand(jobId))
        rc.json(job.toResponse())
    }

    private suspend fun getEvents(rc: RoutingContext) {
        val jobId = rc.pathParam("jobId")!!
        val events = useCases.getJobEventsUseCase.execute(jobId)
        rc.json(events.map { it.toResponse() })
    }
}
