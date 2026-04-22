import {
  applyOnChainEffect,
  type ApplyOnChainEffectResult,
  type IdempotencyStamp,
} from '@centuari-labs/on-chain-effects';
import type { Pool, PoolClient } from 'pg';
import {
  type Address,
  type Hex,
  type PublicClient,
  type TransactionReceipt,
  keccak256,
  toHex,
} from 'viem';
import { logger } from '../../logger';
import {
  BORROW_POSITION_CREATED_EVENT,
  LEND_POSITION_CREATED_EVENT,
} from '../eventAbis';
import type {
  ParsedBorrowPosition,
  ParsedLendPosition,
  SettlementResult,
} from '../smartContract';

/**
 * Eager-write settlement events into indexer-v3's BYTEA-keyed schema
 * (`lend_position`, `borrow_position`) stamped with `applied_by_tx_hash` /
 * `applied_by_log_index` / `applied_by_block_hash` / `applied_by_block_number`
 * via the shared `applyOnChainEffect` primitive from
 * `@centuari-labs/on-chain-effects`.
 *
 * The indexer-v3 `centuari.processor.ts` tail watches the same events and
 * no-ops when it observes a row already stamped with a matching
 * `(tx_hash, log_index)` — this is the C10 two-writer pattern from the Phase 1
 * plan. The shared primitive is the single source of truth for the
 * verify-then-apply invariant.
 *
 * SQL upsert shapes mirror
 * [indexer-v3/src/processors/centuari.processor.ts](../../../../indexer-v3/src/processors/centuari.processor.ts)
 * byte-for-byte (latest-wins on `rate`, accumulation on `cbt_balance` /
 * `principal` / `debt`). Any divergence between the two writers would break
 * idempotency on replay, so keep them in lockstep.
 *
 * `bond_token` rows are owned exclusively by the indexer tail because bond
 * tokens are immutable after creation and only one writer is needed.
 * BalanceLedger.Credited / Debited deltas also flow through the tail
 * (sub-second latency under normal load) — eager-writing them from
 * settlement-engine would only buy milliseconds and is out of Phase A scope.
 */

const LEND_POSITION_CREATED_TOPIC0 = keccak256(
  toHex(
    'LendPositionCreated(bytes32,address,address,uint256,uint256,uint256)',
  ),
);
const BORROW_POSITION_CREATED_TOPIC0 = keccak256(
  toHex('BorrowPositionCreated(bytes32,address,uint256,uint256,uint256)'),
);

interface LendEventArgs {
  marketId: Hex;
  lender: Address;
  bondToken: Address;
  cbtAmount: bigint;
  principal: bigint;
  rate: bigint;
}

interface BorrowEventArgs {
  marketId: Hex;
  borrower: Address;
  principal: bigint;
  debt: bigint;
  rate: bigint;
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
  tx: PoolClient,
  table: 'lend_position' | 'borrow_position',
  pkCondition: string,
  pkValues: readonly unknown[],
  stamp: IdempotencyStamp,
): Promise<boolean> => {
  const result = await tx.query<{ count: string }>(
    `SELECT count(*)::text AS count
       FROM ${table}
      WHERE ${pkCondition}
        AND applied_by_tx_hash = $${pkValues.length + 1}
        AND applied_by_log_index = $${pkValues.length + 2}`,
    [...pkValues, hexToBytea(stamp.txHash), stamp.logIndex],
  );
  return Boolean(result.rows[0] && Number(result.rows[0].count) > 0);
};

const applyLendEvent = async (
  pool: Pool,
  client: PublicClient,
  receipt: TransactionReceipt,
  event: ParsedLendPosition,
): Promise<ApplyOnChainEffectResult> => {
  return await applyOnChainEffect<LendEventArgs>({
    client,
    pool,
    receipt,
    txHash: receipt.transactionHash,
    expectedEventTopic: LEND_POSITION_CREATED_TOPIC0,
    logIndex: event.logIndex,
    abi: [LEND_POSITION_CREATED_EVENT],
    expectedArgsPredicate: (args) =>
      args.marketId.toLowerCase() === event.marketId.toLowerCase() &&
      args.lender.toLowerCase() === event.lender.toLowerCase(),
    alreadyAppliedCheck: (tx, stamp) =>
      alreadyStamped(
        tx,
        'lend_position',
        'market_id = $1 AND lender = $2',
        [hexToBytea(event.marketId), hexToBytea(event.lender)],
        stamp,
      ),
    mutation: async (tx, _args, stamp) => {
      await tx.query(
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
    },
  });
};

const applyBorrowEvent = async (
  pool: Pool,
  client: PublicClient,
  receipt: TransactionReceipt,
  event: ParsedBorrowPosition,
): Promise<ApplyOnChainEffectResult> => {
  return await applyOnChainEffect<BorrowEventArgs>({
    client,
    pool,
    receipt,
    txHash: receipt.transactionHash,
    expectedEventTopic: BORROW_POSITION_CREATED_TOPIC0,
    logIndex: event.logIndex,
    abi: [BORROW_POSITION_CREATED_EVENT],
    expectedArgsPredicate: (args) =>
      args.marketId.toLowerCase() === event.marketId.toLowerCase() &&
      args.borrower.toLowerCase() === event.borrower.toLowerCase(),
    alreadyAppliedCheck: (tx, stamp) =>
      alreadyStamped(
        tx,
        'borrow_position',
        'market_id = $1 AND borrower = $2',
        [hexToBytea(event.marketId), hexToBytea(event.borrower)],
        stamp,
      ),
    mutation: async (tx, _args, stamp) => {
      await tx.query(
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
    },
  });
};

/**
 * Structured result entry for observability. One per event processed.
 */
interface EventOutcome {
  readonly eventName: 'LendPositionCreated' | 'BorrowPositionCreated';
  readonly logIndex: number;
  readonly applied: boolean;
  readonly reason?: string;
}

const logOutcome = (
  txHash: Hex,
  outcome: EventOutcome,
  key: Record<string, unknown>,
): void => {
  if (outcome.applied) return;
  if (outcome.reason === 'already_stamped') {
    logger.debug(
      {
        component: 'apply-settlement',
        txHash,
        ...outcome,
        ...key,
      },
      'Event already stamped — no-op',
    );
    return;
  }
  logger.warn(
    {
      component: 'apply-settlement',
      txHash,
      ...outcome,
      ...key,
    },
    'Event not applied',
  );
};

/**
 * Apply every position event in a settlement result to indexer-v3 tables via
 * the shared `applyOnChainEffect` primitive. Each call scopes the event to a
 * specific `logIndex` so that a single settlement tx can emit multiple events
 * for the same `(marketId, lender|borrower)` key (e.g. one lender partial-
 * filled by two borrowers in the same batch) and each event is applied
 * individually — the `ON CONFLICT` upsert then accumulates principal and
 * cbt_balance correctly. Relying on match-first topic selection here would
 * silently drop every event past the first.
 */
export const applySettlementResult = async (
  pool: Pool,
  client: PublicClient,
  result: SettlementResult,
): Promise<void> => {
  const { receipt } = result;

  for (const event of result.lendPositionEvents) {
    const res = await applyLendEvent(pool, client, receipt, event);
    logOutcome(
      receipt.transactionHash,
      {
        eventName: 'LendPositionCreated',
        logIndex: event.logIndex,
        applied: res.applied,
        reason: res.applied ? undefined : res.reason,
      },
      { marketId: event.marketId, lender: event.lender },
    );
  }

  for (const event of result.borrowPositionEvents) {
    const res = await applyBorrowEvent(pool, client, receipt, event);
    logOutcome(
      receipt.transactionHash,
      {
        eventName: 'BorrowPositionCreated',
        logIndex: event.logIndex,
        applied: res.applied,
        reason: res.applied ? undefined : res.reason,
      },
      { marketId: event.marketId, borrower: event.borrower },
    );
  }

  logger.info(
    {
      component: 'apply-settlement',
      txHash: receipt.transactionHash,
      blockNumber: Number(receipt.blockNumber),
      lendPositions: result.lendPositionEvents.length,
      borrowPositions: result.borrowPositionEvents.length,
    },
    'Applied settlement result to indexer-v3 schema',
  );
};
