package dev.logplay.server.postgres

import dev.logplay.server.test.AbstractIntegrationTest
import dev.logplay.server.test.IntegrationTestBackend
import org.junit.jupiter.api.Tag
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

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
