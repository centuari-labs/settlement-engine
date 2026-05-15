/**
 * Phase A.1 burn-in harness.
 *
 * Synthesises a SettlementResult and applies it against the live
 * indexer-v3 Postgres schema, then verifies:
 *   1. lend_position + borrow_position rows land with correct stamps
 *   2. Re-applying the same SettlementResult is a no-op (idempotent)
 *   3. Applying a second SettlementResult for the same (market, user)
 *      accumulates principal but overwrites rate (latest wins), mirroring
 *      indexer-v3/src/processors/centuari.processor.ts.
 *
 * Usage:
 *   DATABASE_URL=postgres://centuari:password@localhost:5432/centuari \
 *     npx ts-node scripts/burn-in-apply-settlement.ts
 */

import { Pool } from 'pg';
import { applySettlementResult } from '../src/settlement/database/apply-settlement';
import type { SettlementResult } from '../src/settlement/smartContract';

const MARKET_ID =
  '0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef';
const LENDER = '0x1111111111111111111111111111111111111111';
const BORROWER = '0x2222222222222222222222222222222222222222';
const BOND_TOKEN = '0x3333333333333333333333333333333333333333';

const buildResult = (
  opts: {
    txHash: string;
    blockHash: string;
    blockNumber: number;
    lendPrincipal: bigint;
    lendRate: bigint;
    borrowPrincipal: bigint;
    borrowRate: bigint;
  },
): SettlementResult => ({
  transactionHash: opts.txHash,
  blockHash: opts.blockHash as `0x${string}`,
  blockNumber: opts.blockNumber,
  gasUsed: 500_000,
  timestamp: Date.now(),
  settledMatchIds: ['burn-in-match'],
  bondTokenEvents: [],
  lendPositionEvents: [
    {
      marketId: MARKET_ID,
      lender: LENDER,
      bondToken: BOND_TOKEN,
      cbtAmount: opts.lendPrincipal,
      principal: opts.lendPrincipal,
      rate: opts.lendRate,
      logIndex: 0,
    },
  ],
  borrowPositionEvents: [
    {
      marketId: MARKET_ID,
      borrower: BORROWER,
      principal: opts.borrowPrincipal,
      debt: opts.borrowPrincipal + opts.borrowPrincipal / 20n, // +5%
      rate: opts.borrowRate,
      logIndex: 1,
    },
  ],
});

async function main(): Promise<void> {
  const pool = new Pool({ connectionString: process.env.DATABASE_URL });

  // Clear any prior burn-in state so runs are deterministic.
  await pool.query(
    `DELETE FROM lend_position WHERE market_id = decode($1, 'hex')`,
    [MARKET_ID.slice(2)],
  );
  await pool.query(
    `DELETE FROM borrow_position WHERE market_id = decode($1, 'hex')`,
    [MARKET_ID.slice(2)],
  );

  const firstResult = buildResult({
    txHash: '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1',
    blockHash: '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb1',
    blockNumber: 1000,
    lendPrincipal: 1_000_000n,
    lendRate: 500n,
    borrowPrincipal: 1_000_000n,
    borrowRate: 500n,
  });

  console.log('▶ First apply...');
  await applySettlementResult(pool, firstResult);

  let lend = await pool.query(
    `SELECT principal::text, rate::text, cbt_balance::text,
            encode(applied_by_tx_hash, 'hex') AS tx, applied_by_log_index AS log_idx
       FROM lend_position WHERE market_id = decode($1, 'hex') AND lender = decode($2, 'hex')`,
    [MARKET_ID.slice(2), LENDER.slice(2)],
  );
  let borrow = await pool.query(
    `SELECT principal::text, debt::text, rate::text,
            encode(applied_by_tx_hash, 'hex') AS tx, applied_by_log_index AS log_idx
       FROM borrow_position WHERE market_id = decode($1, 'hex') AND borrower = decode($2, 'hex')`,
    [MARKET_ID.slice(2), BORROWER.slice(2)],
  );
  console.log('lend:', lend.rows[0]);
  console.log('borrow:', borrow.rows[0]);

  if (
    lend.rows[0].principal !== '1000000' ||
    lend.rows[0].rate !== '500' ||
    lend.rows[0].log_idx !== 0
  ) {
    throw new Error('first apply: lend_position row mismatch');
  }
  if (
    borrow.rows[0].principal !== '1000000' ||
    borrow.rows[0].rate !== '500' ||
    borrow.rows[0].log_idx !== 1
  ) {
    throw new Error('first apply: borrow_position row mismatch');
  }
  console.log('✓ First apply landed expected rows');

  console.log('▶ Re-apply identical result (idempotency)...');
  await applySettlementResult(pool, firstResult);

  lend = await pool.query(
    `SELECT principal::text, rate::text,
            encode(applied_by_tx_hash, 'hex') AS tx, applied_by_log_index AS log_idx
       FROM lend_position WHERE market_id = decode($1, 'hex') AND lender = decode($2, 'hex')`,
    [MARKET_ID.slice(2), LENDER.slice(2)],
  );
  if (
    lend.rows[0].principal !== '1000000' ||
    lend.rows[0].rate !== '500' ||
    lend.rows[0].log_idx !== 0
  ) {
    throw new Error('idempotency: lend_position got mutated on replay');
  }
  console.log('✓ Replay was a no-op (stamps held)');

  console.log('▶ Second settlement (new tx) — accumulate principal, latest rate wins...');
  const secondResult = buildResult({
    txHash: '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa2',
    blockHash: '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb2',
    blockNumber: 1001,
    lendPrincipal: 500_000n,
    lendRate: 700n,
    borrowPrincipal: 500_000n,
    borrowRate: 700n,
  });
  await applySettlementResult(pool, secondResult);

  lend = await pool.query(
    `SELECT principal::text, rate::text, cbt_balance::text,
            encode(applied_by_tx_hash, 'hex') AS tx, applied_by_log_index AS log_idx
       FROM lend_position WHERE market_id = decode($1, 'hex') AND lender = decode($2, 'hex')`,
    [MARKET_ID.slice(2), LENDER.slice(2)],
  );
  borrow = await pool.query(
    `SELECT principal::text, debt::text, rate::text,
            encode(applied_by_tx_hash, 'hex') AS tx, applied_by_log_index AS log_idx
       FROM borrow_position WHERE market_id = decode($1, 'hex') AND borrower = decode($2, 'hex')`,
    [MARKET_ID.slice(2), BORROWER.slice(2)],
  );
  console.log('lend:', lend.rows[0]);
  console.log('borrow:', borrow.rows[0]);

  if (
    lend.rows[0].principal !== '1500000' ||
    lend.rows[0].rate !== '700' ||
    lend.rows[0].cbt_balance !== '1500000'
  ) {
    throw new Error('accumulation: lend_position rollup wrong');
  }
  if (
    borrow.rows[0].principal !== '1500000' ||
    borrow.rows[0].rate !== '700'
  ) {
    throw new Error('accumulation: borrow_position rollup wrong');
  }
  console.log('✓ Second apply accumulated principal + overwrote rate correctly');

  await pool.query(
    `DELETE FROM lend_position WHERE market_id = decode($1, 'hex')`,
    [MARKET_ID.slice(2)],
  );
  await pool.query(
    `DELETE FROM borrow_position WHERE market_id = decode($1, 'hex')`,
    [MARKET_ID.slice(2)],
  );
  await pool.end();
  console.log('\n✅ Phase A.1 burn-in passed');
}

main().catch((err) => {
  console.error('\n❌ Burn-in failed:', err);
  process.exit(1);
});
