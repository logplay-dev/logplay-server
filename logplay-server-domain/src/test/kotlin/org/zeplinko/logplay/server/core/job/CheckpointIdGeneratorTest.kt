package org.zeplinko.logplay.server.core.job

import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class CheckpointIdGeneratorTest {

    @Test
    fun `fromChainPosition returns a valid UUID string`() {
        val id = CheckpointIdGenerator.fromChainPosition("job-1", null)

        assertThat(UUID.fromString(id).toString()).isEqualTo(id)
    }

    @Test
    fun `fromChainPosition is deterministic for the same inputs`() {
        val a = CheckpointIdGenerator.fromChainPosition("job-1", "prev-cp-1")
        val b = CheckpointIdGenerator.fromChainPosition("job-1", "prev-cp-1")

        assertThat(a).isEqualTo(b)
    }

    @Test
    fun `fromChainPosition is deterministic for null previousCheckpointId`() {
        val a = CheckpointIdGenerator.fromChainPosition("job-1", null)
        val b = CheckpointIdGenerator.fromChainPosition("job-1", null)

        assertThat(a).isEqualTo(b)
    }

    @Test
    fun `fromChainPosition produces different ids for different jobs`() {
        val a = CheckpointIdGenerator.fromChainPosition("job-1", null)
        val b = CheckpointIdGenerator.fromChainPosition("job-2", null)

        assertThat(a).isNotEqualTo(b)
    }

    @Test
    fun `fromChainPosition produces different ids for different predecessors`() {
        val a = CheckpointIdGenerator.fromChainPosition("job-1", "prev-cp-1")
        val b = CheckpointIdGenerator.fromChainPosition("job-1", "prev-cp-2")

        assertThat(a).isNotEqualTo(b)
    }

    @Test
    fun `fromChainPosition produces different ids for null vs non-null predecessor`() {
        val a = CheckpointIdGenerator.fromChainPosition("job-1", null)
        val b = CheckpointIdGenerator.fromChainPosition("job-1", "some-cp")

        assertThat(a).isNotEqualTo(b)
    }

    @Test
    fun `fromChainPosition creates a fully deterministic chain`() {
        val jobId = "job-1"
        val cp1 = CheckpointIdGenerator.fromChainPosition(jobId, null)
        val cp2 = CheckpointIdGenerator.fromChainPosition(jobId, cp1)
        val cp3 = CheckpointIdGenerator.fromChainPosition(jobId, cp2)

        // Replay the exact same chain — must produce identical IDs.
        val cp1Again = CheckpointIdGenerator.fromChainPosition(jobId, null)
        val cp2Again = CheckpointIdGenerator.fromChainPosition(jobId, cp1Again)
        val cp3Again = CheckpointIdGenerator.fromChainPosition(jobId, cp2Again)

        assertThat(cp1).isEqualTo(cp1Again)
        assertThat(cp2).isEqualTo(cp2Again)
        assertThat(cp3).isEqualTo(cp3Again)

        // All three must be distinct from each other.
        assertThat(setOf(cp1, cp2, cp3)).hasSize(3)
    }

    @Test
    fun `fromChainPosition uses a different namespace than JobIdGenerator`() {
        // Ensure checkpoint IDs never collide with job IDs, even when the
        // encoded name bytes happen to match (e.g., both using same string pair).
        val jobId = JobIdGenerator.fromIdempotencyKey("group", "key")
        val checkpointId = CheckpointIdGenerator.fromChainPosition("group", "key")

        assertThat(jobId).isNotEqualTo(checkpointId)
    }

    @Test
    fun `1000 chain positions all produce unique ids`() {
        val ids = mutableListOf<String>()
        var prev: String? = null
        for (i in 0 until 1000) {
            val id = CheckpointIdGenerator.fromChainPosition("job-stress", prev)
            ids.add(id)
            prev = id
        }

        assertThat(ids.toSet()).hasSize(1000)
    }
}
