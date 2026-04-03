package dev.logplay.server.h2

import dev.logplay.server.test.AbstractIntegrationTest
import dev.logplay.server.test.IntegrationTestBackend
import org.junit.jupiter.api.Tag

@Tag("integration")
class H2IntegrationTest : AbstractIntegrationTest() {

    override fun createBackend(): IntegrationTestBackend = H2TestBackend()
}
