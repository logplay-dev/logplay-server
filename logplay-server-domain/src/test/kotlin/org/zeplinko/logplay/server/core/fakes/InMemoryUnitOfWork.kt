package org.zeplinko.logplay.server.core.fakes

import org.zeplinko.logplay.server.core.UnitOfWork

/**
 * Test double for [UnitOfWork]. Domain unit tests run against map-backed in-memory fakes, so there
 * is no real transaction to manage — the block runs directly, and re-entrant calls simply nest
 * (matching the production "join the active transaction" contract).
 *
 * This intentionally does NOT simulate rollback: the migrated use cases validate and classify
 * (their only throw sites) before performing any write, so a failing transaction never leaves a
 * partial write to undo. Real rollback/atomicity is covered by the H2/Postgres integration tests.
 * If a future test ever needs write-then-throw fidelity, add snapshot/restore here.
 */
class InMemoryUnitOfWork : UnitOfWork {
    override suspend fun <T> transaction(block: suspend () -> T): T = block()
}
