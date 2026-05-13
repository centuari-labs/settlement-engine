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
  decodeEventLog,
  keccak256,
  toHex,
} from 'viem';
import { logger } from '../../logger';
import {
  BORROW_POSITION_CREATED_EVENT,
  COLLATERAL_FLAG_SET_EVENT,
  LEND_POSITION_CREATED_EVENT,
} from '../eventAbis';
import type { Match } from '../../schemas/match';
import type {
  ParsedBorrowPosition,
  ParsedLendPosition,
  SettlementResult,
} from '../smartContract';
import { withTransaction } from './connection';
import { writebackSettledMatches } from './lock-release';
import { clearForEvent as clearPendingCollateralFlag } from './pending-collateral-flags';

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
const COLLATERAL_FLAG_SET_TOPIC0 = keccak256(
  toHex('CollateralFlagSet(address,address,address,bool,uint64)'),
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
 * Parse `CollateralFlagSet` events from the receipt and DELETE the matching
 * `pending_collateral_flags` rows in a single transaction. Idempotent:
 * DELETEs no-op if the row was already removed (e.g. by the indexer-v3 tail
 * writer or by a prior eager run). Defensively DELETEs on `used=false`
 * events too — these never appear in a settle-driven receipt today
 * (Centuari.settleMatch only emits `used=true`), but the DELETE is cheap and
 * future-proofs against an unflag emission slipping into the same tx.
 *
 * We don't go through `applyOnChainEffect` here because there's no
 * idempotency stamp to maintain — the queue is a transient backend-owned
 * buffer, not part of the indexer-v3 shared schema. `DELETE WHERE` is its
 * own idempotency.
 */
const clearPendingCollateralFlagsFromReceipt = async (
  pool: Pool,
  receipt: TransactionReceipt,
): Promise<number> => {
  interface DecodedFlagEvent {
    readonly user: Address;
    readonly asset: Address;
    readonly used: boolean;
    readonly logIndex: number;
  }

  const events: DecodedFlagEvent[] = [];
  for (const log of receipt.logs) {
    if (log.topics[0]?.toLowerCase() !== COLLATERAL_FLAG_SET_TOPIC0.toLowerCase()) {
      continue;
    }
    try {
      const decoded = decodeEventLog({
        abi: [COLLATERAL_FLAG_SET_EVENT],
        data: log.data,
        topics: log.topics,
      });
      if (decoded.eventName !== 'CollateralFlagSet') continue;
      // Narrow `decoded.args`: event ABIs are now JSON-imported (synced from
      // smart-contract-revamp) and viem can't infer literal field types from
      // a wide AbiEvent.
      const args = decoded.args as unknown as {
        user: Address;
        asset: Address;
        used: boolean;
      };
      events.push({
        user: args.user,
        asset: args.asset,
        used: args.used,
        logIndex: Number(log.logIndex ?? -1),
      });
    } catch (err) {
      logger.warn(
        {
          component: 'apply-settlement',
          txHash: receipt.transactionHash,
          logIndex: log.logIndex,
          err: (err as Error).message,
        },
        'Failed to decode CollateralFlagSet log — skipping',
      );
    }
  }

  if (events.length === 0) return 0;

  await withTransaction(async (tx) => {
    for (const ev of events) {
      await clearPendingCollateralFlag(tx, ev.user, ev.asset);
    }
  });

  logger.info(
    {
      component: 'apply-settlement',
      txHash: receipt.transactionHash,
      collateralFlagEvents: events.length,
    },
    'Cleared pending_collateral_flags rows from CollateralFlagSet events',
  );

  return events.length;
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
 *
 * After position events apply, parse `CollateralFlagSet` events from the
 * same receipt and DELETE matching `pending_collateral_flags` rows (Phase 3
 * eager queue cleanup). The indexer-v3 `balance-ledger.processor.ts` tail
 * writer (Phase 4) does the same DELETE idempotently for direct-caller
 * events and any eager-path crashes.
 *
 * Last, write back the order-lock lifecycle: flip every settled match from
 * `settlement_status='PENDING'` to `'SETTLED'` and decrement both sides of
 * `portfolio.locked_amount` by the exact amounts the db-writer added at
 * match time. Idempotent on retry — see lock-release.ts for details. The
 * caller passes the original `Match[]` payloads so we have the per-side fee
 * decomposition; `result.settledMatchIds` filters the array down to the
 * subset that actually settled this batch.
 */
export const applySettlementResult = async (
  pool: Pool,
  client: PublicClient,
  result: SettlementResult,
  matches: readonly Match[],
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

  const collateralFlagsCleared = await clearPendingCollateralFlagsFromReceipt(
    pool,
    receipt,
  );

  const settledMatchIdSet = new Set(result.settledMatchIds);
  const writebackResult = await writebackSettledMatches(
    pool,
    matches,
    settledMatchIdSet,
    receipt.transactionHash,
  );

  logger.info(
    {
      component: 'apply-settlement',
      txHash: receipt.transactionHash,
      blockNumber: Number(receipt.blockNumber),
      lendPositions: result.lendPositionEvents.length,
      borrowPositions: result.borrowPositionEvents.length,
      collateralFlagsCleared,
      matchesWrittenBack: writebackResult.settled,
      matchesAlreadySettled: writebackResult.alreadySettled,
    },
    'Applied settlement result to indexer-v3 schema',
  );
};
