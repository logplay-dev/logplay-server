package org.zeplinko.logplay.server.config

/**
 * A small typed reader over an environment source (the process environment by default). The lookup
 * is injectable so config parsing can be unit-tested without touching the real environment.
 * Malformed values fail fast with a clear message instead of silently falling back to a default;
 * blank values are treated as unset.
 */
class Env(private val lookup: (String) -> String? = System::getenv) {

    fun string(name: String, default: String): String = value(name) ?: default

    fun stringOrNull(name: String): String? = value(name)

    fun int(name: String, default: Int): Int =
        value(name)?.let { it.toIntOrNull() ?: fail(name, it, "an integer") } ?: default

    fun long(name: String, default: Long): Long =
        value(name)?.let { it.toLongOrNull() ?: fail(name, it, "a long") } ?: default

    fun boolean(name: String, default: Boolean): Boolean =
        value(name)?.let {
            it.lowercase().toBooleanStrictOrNull() ?: fail(name, it, "a boolean (true/false)")
        } ?: default

    private fun value(name: String): String? = lookup(name)?.trim()?.takeIf { it.isNotEmpty() }

    private fun fail(name: String, raw: String, expected: String): Nothing =
        throw IllegalArgumentException(
            "Environment variable $name must be $expected, but was '$raw'"
        )
}
