package dev.logplay.server.core

import java.nio.ByteBuffer
import java.security.MessageDigest
import java.util.UUID

object UUIDv5 {

    // UUIDv5 per RFC 4122 §4.3: SHA-1(namespace || name), with the version
    // nibble set to 5 and the RFC 4122 variant bits forced.
    fun generate(namespace: UUID, name: ByteArray): UUID {
        val nsBytes =
            ByteBuffer.allocate(16)
                .putLong(namespace.mostSignificantBits)
                .putLong(namespace.leastSignificantBits)
                .array()

        val md = MessageDigest.getInstance("SHA-1")
        md.update(nsBytes)
        md.update(name)
        val digest = md.digest()

        digest[6] = ((digest[6].toInt() and 0x0f) or 0x50).toByte()
        digest[8] = ((digest[8].toInt() and 0x3f) or 0x80).toByte()

        val buf = ByteBuffer.wrap(digest, 0, 16)
        val msb = buf.long
        val lsb = buf.long
        return UUID(msb, lsb)
    }

    // Each field is serialized as [4-byte big-endian length][UTF-8 bytes].
    // The length prefix is what makes the encoding collision-free: without
    // it, pairs like ("a", "Xb") and ("aX", "b") could serialize identically
    // for some separator X. With length prefixes, the first 4 bytes alone
    // already disambiguate.
    fun encodeName(field1: String, field2: String): ByteArray {
        val bytes1 = field1.toByteArray(Charsets.UTF_8)
        val bytes2 = field2.toByteArray(Charsets.UTF_8)
        return ByteBuffer.allocate(Int.SIZE_BYTES + bytes1.size + Int.SIZE_BYTES + bytes2.size)
            .putInt(bytes1.size)
            .put(bytes1)
            .putInt(bytes2.size)
            .put(bytes2)
            .array()
    }
}
