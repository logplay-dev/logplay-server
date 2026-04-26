package org.zeplinko.logplay.server.core.job

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class SaveJobCheckpointCommandTest {

    @Test
    fun `equals and hashCode should treat two null-data commands as equal`() {
        val a = SaveJobCheckpointCommand("job", "worker", null, "n", null)
        val b = SaveJobCheckpointCommand("job", "worker", null, "n", null)

        assertThat(a).isEqualTo(b)
        assertThat(a.hashCode()).isEqualTo(b.hashCode())
    }

    @Test
    fun `equals should distinguish null data from empty data`() {
        val nullData = SaveJobCheckpointCommand("job", "worker", null, "n", null)
        val emptyData = SaveJobCheckpointCommand("job", "worker", null, "n", byteArrayOf())

        assertThat(nullData).isNotEqualTo(emptyData)
    }

    @Test
    fun `equals should distinguish null data from non-null data`() {
        val nullData = SaveJobCheckpointCommand("job", "worker", null, "n", null)
        val nonNull = SaveJobCheckpointCommand("job", "worker", null, "n", "x".toByteArray())

        assertThat(nullData).isNotEqualTo(nonNull)
        assertThat(nonNull).isNotEqualTo(nullData)
    }

    @Test
    fun `equals should compare data by content not reference`() {
        val a = SaveJobCheckpointCommand("job", "worker", null, "n", "payload".toByteArray())
        val b = SaveJobCheckpointCommand("job", "worker", null, "n", "payload".toByteArray())

        assertThat(a).isEqualTo(b)
        assertThat(a.hashCode()).isEqualTo(b.hashCode())
    }

    @Test
    fun `hashCode should not throw when data is null`() {
        val cmd = SaveJobCheckpointCommand("job", "worker", null, null, null)
        cmd.hashCode()
    }
}
