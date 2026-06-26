package org.zeplinko.logplay.server.core

/**
 * Outbound port that runs a unit of work inside a single database transaction.
 *
 * The domain opens a transaction by wrapping its reads and writes in [transaction]; the gateway
 * primitives invoked inside the block enlist in that transaction automatically. Propagation is
 * **ambient** — the open connection is carried by the backend adapter (e.g. in the coroutine
 * context), not passed as a parameter — so this port names no driver type and the domain never
 * touches a `Connection`/`Pool`.
 *
 * **Contract for implementations:**
 * - Commit when `block` returns normally; roll back if it throws (the original exception then
 *   propagates to the caller).
 * - Re-entrant: invoking [transaction] while already inside one joins the active transaction rather
 *   than opening a second/nested one, so composing use cases never deadlocks waiting on a second
 *   connection.
 * - The connection is bound for the whole lifetime of `block`; backends over blocking drivers
 *   (H2/JDBC) must confine the entire block to a single thread.
 */
interface UnitOfWork {
    suspend fun <T> transaction(block: suspend () -> T): T
}
