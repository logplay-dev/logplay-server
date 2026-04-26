package org.zeplinko.logplay.server.core.job

import java.util.UUID
import org.zeplinko.logplay.server.core.UUIDv5

/**
 * Derives stable checkpoint ids from `(jobId, previousCheckpointId)` using UUIDv5 over a fixed
 * LogPlay namespace. Determinism is what makes the primary key alone sufficient to enforce chain
 * uniqueness: two writers attempting to insert at the same chain position collide on the PK and the
 * gateway translates the collision into [InvalidCheckpointOrderException].
 */
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

    /**
     * Returns the deterministic checkpoint id for the given chain position. `null`
     * `previousCheckpointId` is encoded as the literal sentinel "ROOT" — that sentinel must never
     * change once any rows have been persisted.
     */
    fun fromChainPosition(jobId: String, previousCheckpointId: String?): String {
        val name = UUIDv5.encodeName(jobId, previousCheckpointId ?: ROOT_SENTINEL)
        return UUIDv5.generate(LOGPLAY_CHECKPOINT_NAMESPACE, name).toString()
    }
}
