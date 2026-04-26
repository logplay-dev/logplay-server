package org.zeplinko.logplay.server.core.job

import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class JobIdGeneratorTest {

    @Test
    fun `fromIdempotencyKey returns a valid UUID string`() {
        val id = JobIdGenerator.fromIdempotencyKey("group", "key")

        // Round-trip parse — would throw if malformed.
        assertThat(UUID.fromString(id).toString()).isEqualTo(id)
    }

    @Test
    fun `fromIdempotencyKey is deterministic for the same inputs`() {
        val a = JobIdGenerator.fromIdempotencyKey("group-A", "order-123")
        val b = JobIdGenerator.fromIdempotencyKey("group-A", "order-123")

        assertThat(a).isEqualTo(b)
    }

    @Test
    fun `fromIdempotencyKey produces different ids for different keys in same group`() {
        val a = JobIdGenerator.fromIdempotencyKey("group", "key-1")
        val b = JobIdGenerator.fromIdempotencyKey("group", "key-2")

        assertThat(a).isNotEqualTo(b)
    }

    @Test
    fun `fromIdempotencyKey produces different ids for same key in different groups`() {
        val a = JobIdGenerator.fromIdempotencyKey("group-A", "shared-key")
        val b = JobIdGenerator.fromIdempotencyKey("group-B", "shared-key")

        assertThat(a).isNotEqualTo(b)
    }

    @Test
    fun `fromIdempotencyKey handles non-ASCII UTF-8 in both fields`() {
        val a = JobIdGenerator.fromIdempotencyKey("grüppe", "clé-ünîque")
        val b = JobIdGenerator.fromIdempotencyKey("grüppe", "clé-ünîque")
        val c = JobIdGenerator.fromIdempotencyKey("gruppe", "clé-ünîque")

        assertThat(a).isEqualTo(b)
        assertThat(a).isNotEqualTo(c)
    }

    @Test
    fun `length-prefix encoding prevents boundary-swap collisions`() {
        val a = JobIdGenerator.fromIdempotencyKey("a", "\u001fb")
        val b = JobIdGenerator.fromIdempotencyKey("a\u001f", "b")

        assertThat(a).isNotEqualTo(b)
    }

    @Test
    fun `1000 random pairs all produce unique ids`() {
        val ids =
            (0 until 1000).map { i ->
                JobIdGenerator.fromIdempotencyKey("group-${i % 7}", "key-$i")
            }

        assertThat(ids.toSet()).hasSize(1000)
    }

    /**
     * Storage-contract lockdown: the mapping `(groupId, idempotencyKey) → id` MUST stay stable
     * across the lifetime of the database. The server stores only the derived id; the idempotency
     * key is not stored. Any change to the namespace, encoding, or hash function orphans every
     * existing row from its logical identity. If you're updating this test, you are almost
     * certainly introducing a silent data-corruption bug. See [JobIdGenerator] for the full failure
     * mode.
     */
    @Test
    fun `fromIdempotencyKey produces locked output for fixed inputs (DO NOT UPDATE)`() {
        assertThat(JobIdGenerator.fromIdempotencyKey("logplay-storage-contract", "vector-1"))
            .isEqualTo("1694d977-2868-54c2-ba52-5b3f5b2d7a0b")
        assertThat(JobIdGenerator.fromIdempotencyKey("", ""))
            .isEqualTo("75a838af-9b02-5880-b440-0b526be1c4be")
        assertThat(JobIdGenerator.fromIdempotencyKey("group-A", "order-123"))
            .isEqualTo("323d8d5f-c875-5251-aa5e-5d7e3fe01912")
    }
}
