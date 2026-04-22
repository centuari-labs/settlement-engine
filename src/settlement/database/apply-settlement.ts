import type { Pool, PoolClient } from 'pg';
import type { Hash, Hex } from 'viem';
import { logger } from '../../logger';
import type {
  ParsedBorrowPosition,
  ParsedLendPosition,
  SettlementResult,
} from '../smartContract';

/**
 * Eager-write settlement events into indexer-v3's BYTEA-keyed schema
 * (`lend_position`, `borrow_position`) stamped with `applied_by_tx_hash` /
 * `applied_by_log_index` / `applied_by_block_hash` / `applied_by_block_number`.
 *
 * The indexer-v3 `centuari.processor.ts` tail watches the same events and
 * no-ops when it observes a row already stamped with a matching
 * `(tx_hash, log_index)` — this is the C10 two-writer pattern from the Phase 1
 * plan. Crash-recovery for settlement-engine gaps is therefore the tail's
 * responsibility; settlement-engine does not need a durable raw_events queue.
 *
 * SQL upsert shapes mirror
 * [indexer-v3/src/processors/centuari.processor.ts](../../../../indexer-v3/src/processors/centuari.processor.ts)
 * byte-for-byte, including `rate = EXCLUDED.rate` on repeat (latest match
 * wins — no weighted-average rollup). Any divergence between the two writers
 * would break idempotency on replay, so keep them in lockstep.
 *
 * `bond_token` rows are owned exclusively by the indexer tail because bond
 * tokens are immutable after creation and only one writer is needed.
 * BalanceLedger.Credited / Debited deltas also flow through the tail
 * (sub-second latency under normal load) — eager-writing them from
 * settlement-engine would only buy milliseconds and is out of A.1 scope.
 */

interface Stamp {
  readonly txHash: Hash;
  readonly blockHash: Hash;
  readonly blockNumber: bigint;
  readonly logIndex: number;
}

/**
 * Convert a `0x`-prefixed hex string to a Postgres BYTEA buffer.
 */
const hexToBytea = (hex: string): Buffer => {
  const clean = hex.startsWith('0x') ? hex.slice(2) : hex;
  return Buffer.from(clean, 'hex');
};

/**
 * Row was already stamped with this exact `(tx_hash, log_index)` — this is
 * a no-op (either the eager path re-ran or the tail got there first).
 */
const alreadyStamped = async (
  client: PoolClient,
  table: 'lend_position' | 'borrow_position',
  pkCondition: string,
  pkValues: unknown[],
  stamp: Stamp,
): Promise<boolean> => {
  const result = await client.query<{ count: string }>(
    `SELECT count(*)::text AS count
       FROM ${table}
      WHERE ${pkCondition}
        AND applied_by_tx_hash = $${pkValues.length + 1}
        AND applied_by_log_index = $${pkValues.length + 2}`,
    [...pkValues, hexToBytea(stamp.txHash), stamp.logIndex],
  );
  return Boolean(result.rows[0] && Number(result.rows[0].count) > 0);
};

/**
 * Apply a LendPositionCreated event: upsert `(market_id, lender)` with
 * principal + cbt_balance accumulation and latest-wins rate.
 */
const applyLendPosition = async (
  pool: Pool,
  event: ParsedLendPosition,
  stamp: Stamp,
): Promise<void> => {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const pkValues = [hexToBytea(event.marketId), hexToBytea(event.lender)];
    if (
      await alreadyStamped(
        client,
        'lend_position',
        'market_id = $1 AND lender = $2',
        pkValues,
        stamp,
      )
    ) {
      await client.query('ROLLBACK');
      return;
    }

    await client.query(
      `INSERT INTO lend_position
          (market_id, lender, bond_token, cbt_balance, principal, rate,
           applied_by_tx_hash, applied_by_log_index,
           applied_by_block_hash, applied_by_block_number, updated_at)
       VALUES ($1, $2, $3, $4::numeric, $5::numeric, $6::numeric,
               $7, $8, $9, $10, now())
       ON CONFLICT (market_id, lender) DO UPDATE SET
          bond_token = EXCLUDED.bond_token,
          cbt_balance = lend_position.cbt_balance + EXCLUDED.cbt_balance,
          principal = lend_position.principal + EXCLUDED.principal,
          rate = EXCLUDED.rate,
          applied_by_tx_hash = EXCLUDED.applied_by_tx_hash,
          applied_by_log_index = EXCLUDED.applied_by_log_index,
          applied_by_block_hash = EXCLUDED.applied_by_block_hash,
          applied_by_block_number = EXCLUDED.applied_by_block_number,
          updated_at = now()`,
      [
        hexToBytea(event.marketId),
        hexToBytea(event.lender),
        hexToBytea(event.bondToken),
        event.cbtAmount.toString(),
        event.principal.toString(),
        event.rate.toString(),
        hexToBytea(stamp.txHash),
        stamp.logIndex,
        hexToBytea(stamp.blockHash),
        stamp.blockNumber.toString(),
      ],
    );
    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
};

/**
 * Apply a BorrowPositionCreated event: upsert `(market_id, borrower)` with
 * principal + debt accumulation and latest-wins rate.
 */
const applyBorrowPosition = async (
  pool: Pool,
  event: ParsedBorrowPosition,
  stamp: Stamp,
): Promise<void> => {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const pkValues = [hexToBytea(event.marketId), hexToBytea(event.borrower)];
    if (
      await alreadyStamped(
        client,
        'borrow_position',
        'market_id = $1 AND borrower = $2',
        pkValues,
        stamp,
      )
    ) {
      await client.query('ROLLBACK');
      return;
    }

    await client.query(
      `INSERT INTO borrow_position
          (market_id, borrower, principal, debt, rate,
           applied_by_tx_hash, applied_by_log_index,
           applied_by_block_hash, applied_by_block_number, updated_at)
       VALUES ($1, $2, $3::numeric, $4::numeric, $5::numeric,
               $6, $7, $8, $9, now())
       ON CONFLICT (market_id, borrower) DO UPDATE SET
          principal = borrow_position.principal + EXCLUDED.principal,
          debt = borrow_position.debt + EXCLUDED.debt,
          rate = EXCLUDED.rate,
          applied_by_tx_hash = EXCLUDED.applied_by_tx_hash,
          applied_by_log_index = EXCLUDED.applied_by_log_index,
          applied_by_block_hash = EXCLUDED.applied_by_block_hash,
          applied_by_block_number = EXCLUDED.applied_by_block_number,
          updated_at = now()`,
      [
        hexToBytea(event.marketId),
        hexToBytea(event.borrower),
        event.principal.toString(),
        event.debt.toString(),
        event.rate.toString(),
        hexToBytea(stamp.txHash),
        stamp.logIndex,
        hexToBytea(stamp.blockHash),
        stamp.blockNumber.toString(),
      ],
    );
    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
};

/**
 * Apply every position event in a settlement result to indexer-v3 tables.
 * Each event is committed in its own pg transaction so partial failures are
 * recoverable via the tail — no cross-event atomicity needed (each row is
 * independently identified by its `(market_id, user, tx_hash, log_index)`
 * stamp tuple).
 */
export const applySettlementResult = async (
  pool: Pool,
  result: SettlementResult,
): Promise<void> => {
  const baseStamp = {
    txHash: result.transactionHash as Hex as Hash,
    blockHash: result.blockHash,
    blockNumber: BigInt(result.blockNumber),
  };

  for (const event of result.lendPositionEvents) {
    await applyLendPosition(pool, event, {
      ...baseStamp,
      logIndex: event.logIndex,
    });
  }

  for (const event of result.borrowPositionEvents) {
    await applyBorrowPosition(pool, event, {
      ...baseStamp,
      logIndex: event.logIndex,
    });
  }

  logger.info(
    {
      component: 'apply-settlement',
      txHash: result.transactionHash,
      blockNumber: result.blockNumber,
      lendPositions: result.lendPositionEvents.length,
      borrowPositions: result.borrowPositionEvents.length,
    },
    'Applied settlement result to indexer-v3 schema',
  );
};
