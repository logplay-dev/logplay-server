package dev.logplay.server.test

import dev.logplay.server.core.job.JobGateway
import dev.logplay.server.core.worker.WorkerGateway
import io.vertx.core.Vertx
import java.sql.Connection

interface IntegrationTestBackend {

    fun initDatabase()

    fun createGateways(vertx: Vertx): Pair<JobGateway, WorkerGateway>

    fun getJdbcConnection(): Connection

    fun shutdown()
}
