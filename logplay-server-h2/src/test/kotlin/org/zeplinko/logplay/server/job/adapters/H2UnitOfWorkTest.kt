package org.zeplinko.logplay.server.job.adapters

import java.util.UUID
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import org.assertj.core.api.Assertions.assertThat
import org.h2.jdbcx.JdbcConnectionPool
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout

/**
 * Tests for the [H2UnitOfWork] concurrency fixes (PR review #2/#4/#5). The #2 (pool headroom)
 * and #5 (thread re-pin) tests are true regressions — verified to fail against the pre-fix code, so
 * a reintroduced deadlock or off-thread connection access re-breaks the build. The #4 test is an
 * invariant guard (its own note explains why the reviewed leak does not actually reproduce).
 *
 * These use [runBlocking] (real time, real threads), not `runTest`, because the behaviours under
 * test are genuine concurrency: dispatcher hand-off, thread confinement, and connection-pool
 * back-pressure — all of which `runTest`'s virtual clock would mask.
 */
@Timeout(60, unit = TimeUnit.SECONDS)
class H2UnitOfWorkTest {

    private fun h2Pool(maxConnections: Int): JdbcConnectionPool {
        val name = "uow_${UUID.randomUUID().toString().replace("-", "").take(8)}"
        return JdbcConnectionPool.create("jdbc:h2:mem:$name;DB_CLOSE_DELAY=-1", "sa", "").apply {
            this.maxConnections = maxConnections
        }
    }

    // #4 (invariant test, not a red→green reproduction): cancelling transactions must never starve
    // the dispatcher pool. NOTE: this passes with both `send` and `trySend` — the reviewed "send
    // leaks on cancel" claim does not actually hold, because a lease holds one of `size`
    // dispatchers
    // and the channel capacity is `size`, so the return never has to suspend and so never observes
    // cancellation. The test still earns its keep: it guards the return-on-cancellation logic in
    // `lease`'s finally against a future refactor that *would* leak (e.g. moving the return out of
    // the finally, or making the channel rendezvous-capacity). With poolSize=1 any such leak makes
    // the final transaction block forever, which the inner timeout turns into a failure.
    @Test
    fun `cancelled transactions never starve the confined dispatcher pool`() {
        runBlocking {
            val ds = h2Pool(maxConnections = 4)
            val uow = H2UnitOfWork(ds, poolSize = 1)
            try {
                // Cancel several transactions mid-flight; each must hand its dispatcher back.
                repeat(3) { withTimeoutOrNull(100) { uow.transaction { delay(10_000) } } }

                // If a dispatcher leaked, the single-slot pool is empty and this lease never
                // returns.
                val result = withTimeoutOrNull(3_000) { uow.transaction { 42 } }
                assertThat(result).isEqualTo(42)
            } finally {
                ds.dispose()
            }
        }
    }

    // #5: inside a transaction, gateway work must touch the JDBC connection on the thread that owns
    // it, even if the caller switched dispatchers. The pre-fix `withConn` ran the body inline, so a
    // `withContext(Dispatchers.IO)` block would touch the connection off its pinned thread.
    @Test
    fun `withConn inside a transaction re-pins to the connection's confined thread`() {
        runBlocking {
            val ds = h2Pool(maxConnections = 4)
            val uow = H2UnitOfWork(ds, poolSize = 1)
            try {
                uow.transaction {
                    val confinedThread = Thread.currentThread().name
                    assertThat(confinedThread).startsWith("h2-uow-confined")

                    withContext(Dispatchers.IO) {
                        // We are now off the confined thread...
                        assertThat(Thread.currentThread().name).doesNotContain("h2-uow-confined")
                        // ...but withConn must hop back to it before touching the connection.
                        val threadUsedForConn = ds.withConn { Thread.currentThread().name }
                        assertThat(threadUsedForConn).isEqualTo(confinedThread)
                    }
                }
            } finally {
                ds.dispose()
            }
        }
    }

    // #2: the JDBC pool must keep headroom over the confined pool so a non-transactional read does
    // not block behind a full set of in-flight write transactions. With the production sizing
    // (RECOMMENDED_JDBC_POOL_SIZE > DEFAULT_CONFINED_POOL_SIZE) a read still finds a free
    // connection.
    @Test
    fun `non-transactional read keeps headroom when all confined transactions are in flight`() {
        runBlocking {
            val confinedSize = H2UnitOfWork.DEFAULT_CONFINED_POOL_SIZE
            val ds = h2Pool(maxConnections = H2UnitOfWork.RECOMMENDED_JDBC_POOL_SIZE)
            val uow = H2UnitOfWork(ds, poolSize = confinedSize)
            val entered = Channel<Unit>(confinedSize)
            val release = CompletableDeferred<Unit>()
            try {
                // Park confinedSize transactions, each holding its JDBC connection open.
                val held =
                    (1..confinedSize).map {
                        async {
                            uow.transaction {
                                entered.send(Unit)
                                release.await()
                            }
                        }
                    }
                repeat(confinedSize) { entered.receive() }

                // Every confined connection is held; a non-transactional read must still get one.
                val readOk =
                    withTimeoutOrNull(3_000) {
                        ds.withConn { conn ->
                            conn.createStatement().use { it.execute("SELECT 1") }
                            true
                        }
                    }
                assertThat(readOk).isTrue()

                release.complete(Unit)
                held.awaitAll()
            } finally {
                ds.dispose()
            }
        }
    }
}
