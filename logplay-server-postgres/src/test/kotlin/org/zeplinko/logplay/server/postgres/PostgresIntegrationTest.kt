package org.zeplinko.logplay.server.postgres

import org.junit.jupiter.api.Tag
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.zeplinko.logplay.server.test.AbstractIntegrationTest
import org.zeplinko.logplay.server.test.IntegrationTestBackend

@Tag("integration")
@Testcontainers
class PostgresIntegrationTest : AbstractIntegrationTest() {

    companion object {
        @Container
        @JvmStatic
        val postgres =
            PostgreSQLContainer("postgres:17-alpine")
                .withDatabaseName("logplay_test")
                .withUsername("test")
                .withPassword("test")
    }

    override fun createBackend(): IntegrationTestBackend = PostgresTestBackend(postgres)
}
