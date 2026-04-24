package org.zeplinko.logplay.server.core

import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class UUIDv5Test {

    // --- RFC 4122 conformance ---

    @Test
    fun `generate matches Python uuid5(NAMESPACE_DNS, 'python_org') reference vector`() {
        // Well-known cross-verified vector:
        //   python3 -c "import uuid; print(uuid.uuid5(uuid.NAMESPACE_DNS, 'python.org'))"
        //   -> 886313e1-3b8a-5372-9b90-0c9aee199e5d
        val dnsNamespace = UUID.fromString("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
        val result = UUIDv5.generate(dnsNamespace, "python.org".toByteArray(Charsets.UTF_8))

        assertThat(result.toString()).isEqualTo("886313e1-3b8a-5372-9b90-0c9aee199e5d")
    }

    @Test
    fun `generate sets version nibble to 5`() {
        val dnsNamespace = UUID.fromString("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
        val result = UUIDv5.generate(dnsNamespace, "anything".toByteArray(Charsets.UTF_8))

        assertThat(result.version()).isEqualTo(5)
    }

    @Test
    fun `generate sets RFC 4122 variant`() {
        val dnsNamespace = UUID.fromString("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
        val result = UUIDv5.generate(dnsNamespace, "anything".toByteArray(Charsets.UTF_8))

        // RFC 4122 variant is reported as 2 by java.util.UUID.variant().
        assertThat(result.variant()).isEqualTo(2)
    }

    // --- encodeName byte-level check ---

    @Test
    fun `encodeName writes 4-byte BE length prefixes followed by UTF-8 bytes`() {
        val bytes = UUIDv5.encodeName("ab", "xyz")

        // [0..3]   = 0x00000002 (length of "ab")
        // [4..5]   = "ab"
        // [6..9]   = 0x00000003 (length of "xyz")
        // [10..12] = "xyz"
        assertThat(bytes).hasSize(4 + 2 + 4 + 3)
        assertThat(bytes.sliceArray(0..3)).isEqualTo(byteArrayOf(0, 0, 0, 2))
        assertThat(bytes.sliceArray(4..5)).isEqualTo("ab".toByteArray(Charsets.UTF_8))
        assertThat(bytes.sliceArray(6..9)).isEqualTo(byteArrayOf(0, 0, 0, 3))
        assertThat(bytes.sliceArray(10..12)).isEqualTo("xyz".toByteArray(Charsets.UTF_8))
    }

    // --- Length-prefix collision resistance ---

    @Test
    fun `encodeName prevents boundary-swap collisions`() {
        val a = UUIDv5.encodeName("a", "\u001fb")
        val b = UUIDv5.encodeName("a\u001f", "b")

        assertThat(a).isNotEqualTo(b)
    }

    @Test
    fun `encodeName handles empty fields distinctly`() {
        val a = UUIDv5.encodeName("", "ab")
        val b = UUIDv5.encodeName("a", "b")
        val c = UUIDv5.encodeName("ab", "")

        assertThat(setOf(a.toList(), b.toList(), c.toList())).hasSize(3)
    }

    // --- Different namespaces produce different UUIDs ---

    @Test
    fun `different namespaces produce different UUIDs for the same name`() {
        val ns1 = UUID.fromString("a3c6e3f4-9c3e-4e2a-8b7f-1a9d3b8c6e4a")
        val ns2 = UUID.fromString("b7d4f1a2-6e8c-4f3b-9a1d-2c5e7f8d9b3a")
        val name = "same-name".toByteArray(Charsets.UTF_8)

        val result1 = UUIDv5.generate(ns1, name)
        val result2 = UUIDv5.generate(ns2, name)

        assertThat(result1).isNotEqualTo(result2)
    }
}
