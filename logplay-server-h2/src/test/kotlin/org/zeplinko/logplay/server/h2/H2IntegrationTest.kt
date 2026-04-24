package org.zeplinko.logplay.server.h2

import org.junit.jupiter.api.Tag
import org.zeplinko.logplay.server.test.AbstractIntegrationTest
import org.zeplinko.logplay.server.test.IntegrationTestBackend

@Tag("integration")
class H2IntegrationTest : AbstractIntegrationTest() {

    override fun createBackend(): IntegrationTestBackend = H2TestBackend()
}
