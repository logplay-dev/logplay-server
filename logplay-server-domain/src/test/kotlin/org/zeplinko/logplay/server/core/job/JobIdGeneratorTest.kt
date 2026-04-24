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
}
