package org.zeplinko.logplay.server.core.job

import java.util.UUID
import org.zeplinko.logplay.server.core.UUIDv5

object CheckpointIdGenerator {

    // DO NOT CHANGE. This namespace is baked into every derived checkpoint id.
    // Changing it invalidates every existing checkpoint_id in the system: every
    // chain lookup would miss, and all existing rows would be orphaned from
    // their logical identity.
    private val LOGPLAY_CHECKPOINT_NAMESPACE: UUID =
        UUID.fromString("b7d4f1a2-6e8c-4f3b-9a1d-2c5e7f8d9b3a")

    // Sentinel used in place of null previousCheckpointId (i.e., the first
    // checkpoint in a job's chain). Must never change once IDs have been
    // persisted.
    private const val ROOT_SENTINEL = "ROOT"

    fun fromChainPosition(jobId: String, previousCheckpointId: String?): String {
        val name = UUIDv5.encodeName(jobId, previousCheckpointId ?: ROOT_SENTINEL)
        return UUIDv5.generate(LOGPLAY_CHECKPOINT_NAMESPACE, name).toString()
    }
}
