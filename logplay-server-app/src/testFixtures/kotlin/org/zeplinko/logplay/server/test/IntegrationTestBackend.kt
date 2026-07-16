package org.zeplinko.logplay.server.test

import io.vertx.core.Vertx
import java.sql.Connection
import org.zeplinko.logplay.server.core.UnitOfWork
import org.zeplinko.logplay.server.core.job.JobGateway
import org.zeplinko.logplay.server.core.worker.WorkerGateway

interface IntegrationTestBackend {

    fun initDatabase()

    fun createGateways(vertx: Vertx): Pair<JobGateway, WorkerGateway>

    fun createUnitOfWork(): UnitOfWork

    fun getJdbcConnection(): Connection

    fun shutdown()
}
