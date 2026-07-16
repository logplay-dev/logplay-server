package org.zeplinko.logplay.server.job.adapters

import java.sql.Connection
import java.util.concurrent.Executors
import javax.sql.DataSource
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.withContext
import org.zeplinko.logplay.server.core.UnitOfWork

/**
 * Carries the JDBC [Connection] of an open transaction through the coroutine context so gateway
 * methods invoked inside [H2UnitOfWork.transaction] enlist in it automatically (ambient
 * propagation).
 */
internal class H2TransactionContext(
    val connection: Connection,
    val dispatcher: CoroutineDispatcher,
) : AbstractCoroutineContextElement(H2TransactionContext) {
    companion object Key : CoroutineContext.Key<H2TransactionContext>
}

/**
 * Runs [body] against a JDBC connection. Inside a transaction it re-dispatches to the connection's
 * pinned thread ([H2TransactionContext.dispatcher]) so [body] always touches the (non-thread-safe)
 * JDBC [Connection] on the thread that owns it — even if the caller switched dispatchers in the
 * meantime; the connection stays open for the rest of the transaction. Outside a transaction it
 * borrows a fresh pooled connection on [Dispatchers.IO] and closes it when done (auto-commit).
 *
 * Pass [requireTransaction] = `true` for row-locking reads (`SELECT ... FOR UPDATE [SKIP LOCKED]`)
 * whose lock must outlive the call: without an open transaction the lock would be taken on a fresh
 * auto-commit connection and released the instant it closes — a silent no-op that races with no
 * error — so this fails loudly instead of borrowing a pooled connection.
 */
internal suspend fun <T> DataSource.withConn(
    requireTransaction: Boolean = false,
    body: (Connection) -> T,
): T {
    val tx = currentCoroutineContext()[H2TransactionContext]
    return when {
        tx != null -> withContext(tx.dispatcher) { body(tx.connection) }
        requireTransaction ->
            error("This operation takes a row lock and must run inside unitOfWork.transaction { }")
        else -> withContext(Dispatchers.IO) { connection.use(body) }
    }
}

/**
 * H2/JDBC [UnitOfWork]. A JDBC [Connection] is not thread-safe, so each transaction is pinned to
 * one dedicated thread for its whole lifetime: [transaction] leases a confined dispatcher, borrows
 * a connection on it, disables auto-commit, runs `block` with the connection published to the
 * coroutine context, then commits on normal return or rolls back on throw — all on that one thread.
 * A re-entrant call joins the active transaction (already on the confined thread).
 *
 * The confined dispatchers form a fixed pool created once at construction (sized to the connection
 * pool), so there is no per-transaction thread creation and concurrent transactions still run in
 * parallel — matching the pre-UoW gateway, which already blocked one [Dispatchers.IO] thread per
 * transaction.
 */
class H2UnitOfWork(private val dataSource: DataSource, poolSize: Int = DEFAULT_CONFINED_POOL_SIZE) :
    UnitOfWork {

    private val confined = ConfinedDispatcherPool(poolSize)

    override suspend fun <T> transaction(block: suspend () -> T): T {
        // Re-entrant: already inside a transaction on this coroutine — join it.
        if (currentCoroutineContext()[H2TransactionContext] != null) return block()
        return confined.lease { dispatcher ->
            withContext(dispatcher) {
                dataSource.connection.use { conn ->
                    conn.autoCommit = false
                    try {
                        val result = withContext(H2TransactionContext(conn, dispatcher)) { block() }
                        conn.commit()
                        result
                    } catch (e: Throwable) {
                        conn.rollback()
                        throw e
                    } finally {
                        conn.autoCommit = true
                    }
                }
            }
        }
    }

    companion object {
        /**
         * Max concurrent write transactions. Each holds one JDBC connection for its whole block, so
         * this must stay strictly below the JDBC pool's max connections (see
         * [RECOMMENDED_JDBC_POOL_SIZE]) — otherwise a full set of in-flight transactions exhausts
         * the pool and a concurrent non-transactional read blocks until one commits.
         */
        const val DEFAULT_CONFINED_POOL_SIZE = 10

        /**
         * Recommended `JdbcConnectionPool.maxConnections`: the confined pool plus headroom for
         * non-transactional reads (`findJobById`, `getCheckpoints`, `getJobEvents`) so they never
         * wait behind [DEFAULT_CONFINED_POOL_SIZE] concurrent write transactions. Set the pool's
         * max to this at each creation site.
         */
        const val RECOMMENDED_JDBC_POOL_SIZE = DEFAULT_CONFINED_POOL_SIZE * 2
    }
}

/**
 * A fixed set of single-thread dispatchers, leased one per transaction so every statement in a
 * transaction runs on the same thread. [lease] suspends when all are in use (back-pressure
 * mirroring connection-pool exhaustion). Threads are daemons so they never block JVM shutdown.
 */
private class ConfinedDispatcherPool(size: Int) {
    private val dispatchers: List<CoroutineDispatcher> =
        (0 until size).map { i ->
            Executors.newSingleThreadExecutor { r ->
                    Thread(r, "h2-uow-confined-$i").apply { isDaemon = true }
                }
                .asCoroutineDispatcher()
        }

    private val available =
        Channel<CoroutineDispatcher>(size).also { ch -> dispatchers.forEach { ch.trySend(it) } }

    suspend fun <T> lease(body: suspend (CoroutineDispatcher) -> T): T {
        val dispatcher = available.receive()
        try {
            return body(dispatcher)
        } finally {
            // Return the dispatcher with the non-suspending trySend. A lease holds exactly one of
            // the `size` dispatchers, and the channel's capacity is `size`, so there is always a
            // free slot here and the return cannot block. trySend over send is deliberate: it never
            // suspends, so it can never observe cancellation in this finally (e.g. a transaction
            // cancelled by a request timeout) and the dispatcher is always handed back. (send would
            // in fact also be safe here for the same capacity reason — it only throws on cancel
            // when
            // it has to *suspend*, which it never does — but trySend states the intent and removes
            // the dependency on that subtlety.)
            available.trySend(dispatcher)
        }
    }
}
