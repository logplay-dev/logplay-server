package org.zeplinko.logplay.server.core.job

import java.util.UUID
import org.zeplinko.logplay.server.core.UUIDv5

/**
 * Derives stable job ids from `(groupId, idempotencyKey)` using UUIDv5 (SHA-1) over a fixed LogPlay
 * namespace and a length-prefixed name encoding. Determinism is what makes the primary key alone
 * sufficient for idempotency: two `CreateJob` calls with the same `(groupId, idempotencyKey)`
 * collide on the PK without a separate unique index.
 *
 * **This object is part of the storage contract.** The mapping `(groupId, idempotencyKey) → id`
 * must remain stable across the lifetime of the database. The server does not store the idempotency
 * key — it only stores the derived `id`. If the algorithm changes:
 * - Every existing row becomes unaddressable via its original `(groupId, idempotencyKey)` — lookups
 *   will miss, and the SDK can no longer find jobs it previously created.
 * - Duplicate detection on create silently breaks: the same `(groupId, idempotencyKey)` will
 *   produce a different id, bypass the PK collision, and create a second job.
 *
 * A known-vector test in `JobIdGeneratorTest` locks the current mapping. Do not change the
 * namespace, the encoding, or the hash function without first migrating every existing row to its
 * new id, which has no in-band recovery path.
 */
object JobIdGenerator {

    // DO NOT CHANGE. This namespace is baked into every derived job id.
    // Changing it invalidates every existing job_id in the system: every
    // idempotency-key lookup would miss, and all existing rows would be
    // orphaned from their logical identity.
    private val LOGPLAY_JOB_NAMESPACE: UUID =
        UUID.fromString("a3c6e3f4-9c3e-4e2a-8b7f-1a9d3b8c6e4a")

    /**
     * Returns the deterministic job id for the given `(groupId, idempotencyKey)`. The same inputs
     * always produce the same output; different inputs produce different outputs (the
     * length-prefixed encoding rules out collisions from concatenation ambiguity).
     */
    fun fromIdempotencyKey(groupId: String, idempotencyKey: String): String {
        val name = UUIDv5.encodeName(groupId, idempotencyKey)
        return UUIDv5.generate(LOGPLAY_JOB_NAMESPACE, name).toString()
    }
}
