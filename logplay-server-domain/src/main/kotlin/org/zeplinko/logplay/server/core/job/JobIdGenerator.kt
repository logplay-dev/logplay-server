package org.zeplinko.logplay.server.core.job

import java.util.UUID
import org.zeplinko.logplay.server.core.UUIDv5

object JobIdGenerator {

    // DO NOT CHANGE. This namespace is baked into every derived job id.
    // Changing it invalidates every existing job_id in the system: every
    // idempotency-key lookup would miss, and all existing rows would be
    // orphaned from their logical identity.
    private val LOGPLAY_JOB_NAMESPACE: UUID =
        UUID.fromString("a3c6e3f4-9c3e-4e2a-8b7f-1a9d3b8c6e4a")

    fun fromIdempotencyKey(groupId: String, idempotencyKey: String): String {
        val name = UUIDv5.encodeName(groupId, idempotencyKey)
        return UUIDv5.generate(LOGPLAY_JOB_NAMESPACE, name).toString()
    }
}
