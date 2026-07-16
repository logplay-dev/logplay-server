package org.zeplinko.logplay.server.job.adapters

import io.vertx.kotlin.coroutines.coAwait
import io.vertx.sqlclient.Pool
import io.vertx.sqlclient.SqlClient
import io.vertx.sqlclient.SqlConnection
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.withContext
import org.zeplinko.logplay.server.core.UnitOfWork

/**
 * Carries the connection of an open transaction through the coroutine context so gateway methods
 * invoked inside [PostgresUnitOfWork.transaction] enlist in it automatically (ambient propagation).
 */
internal class PostgresTransactionContext(val connection: SqlConnection) :
    AbstractCoroutineContextElement(PostgresTransactionContext) {
    companion object Key : CoroutineContext.Key<PostgresTransactionContext>
}

/**
 * The [SqlClient] to run a statement against: the active transaction's connection when one is open,
 * otherwise the pool (which auto-acquires and releases a connection per statement). This is how a
 * gateway query joins the ambient transaction without taking a connection as a parameter.
 *
 * Pass [requireTransaction] = `true` for row-locking reads (`SELECT ... FOR UPDATE [SKIP LOCKED]`)
 * whose lock must outlive the call: without an open transaction the lock would be taken on a pooled
 * connection and released the instant it returns to the pool — a silent no-op that races with no
 * error — so this fails loudly instead of falling back to the pool.
 */
internal suspend fun Pool.activeClient(requireTransaction: Boolean = false): SqlClient {
    val txConn = currentCoroutineContext()[PostgresTransactionContext]?.connection
    return when {
        txConn != null -> txConn
        requireTransaction ->
            error("This operation takes a row lock and must run inside unitOfWork.transaction { }")
        else -> this
    }
}

/**
 * Vert.x pg-client [UnitOfWork]. Opens a pooled connection, begins a transaction, publishes the
 * connection into the coroutine context for the duration of `block`, then commits on normal return
 * or rolls back if `block` throws. A re-entrant call joins the active transaction instead of
 * opening a second one. No thread confinement is needed — the pg client is fully non-blocking.
 */
class PostgresUnitOfWork(private val pool: Pool) : UnitOfWork {
    override suspend fun <T> transaction(block: suspend () -> T): T {
        // Re-entrant: already inside a transaction on this coroutine — join it.
        if (currentCoroutineContext()[PostgresTransactionContext] != null) return block()
        val conn = pool.connection.coAwait()
        try {
            val tx = conn.begin().coAwait()
            try {
                val result = withContext(PostgresTransactionContext(conn)) { block() }
                tx.commit().coAwait()
                return result
            } catch (e: Throwable) {
                tx.rollback().coAwait()
                throw e
            }
        } finally {
            conn.close().coAwait()
        }
    }
}
