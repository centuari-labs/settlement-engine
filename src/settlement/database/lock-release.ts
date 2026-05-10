import type { Pool, PoolClient } from 'pg';
import type { Match } from '../../schemas/match';
import { logger } from '../../logger';
import { withTransaction } from './connection';

/**
 * Settlement-engine writeback for the order-lock lifecycle.
 *
 * The db-writer (matching-engine codebase) increments
 * `portfolio.locked_amount` at MATCH time for both lender and borrower
 * — see matching-engine/src/services/db/postgres-db-client.ts:216-244
 * for the exact decomposition that this file mirrors. Once the on-chain
 * settlement lands, the BalanceLedger.Debited event is observed by
 * indexer-v3 and `wallet_balance` drops; without a corresponding decrement
 * to `locked_amount`, the backend's available-balance formula
 * (`wallet - locked_amount - sum_open_orders`) under-counts the user's
 * available funds for as long as the lock is stale.
 *
 * This helper extends the existing `applyOnChainEffect` mutation closure
 * in apply-settlement.ts to:
 *   1. Mark `matches.settlement_status = 'SETTLED'` (idempotent — only
 *      transitions PENDING → SETTLED via the WHERE guard).
 *   2. Decrement both sides of `portfolio.locked_amount` by the exact
 *      amounts the db-writer added at match time.
 *
 * Idempotency: a retried settlement attempt on the same match is a natural
 * no-op because the `settlement_status` UPDATE returns no rows on the
 * second run, and we only fire the portfolio decrements when that UPDATE
 * actually transitions a row. `GREATEST(..., 0)` is a belt-and-suspenders
 * guard for any case where a manual SQL fix could otherwise underflow.
 *
 * Pending_collateral_flags cleanup is handled separately in
 * apply-settlement.ts (event-driven via CollateralFlagSet receipt logs);
 * this helper does NOT touch that table.
 */

/**
 * Per-match decomposition of the trade fee — borrower pays taker if
 * `borrowerIsTaker`, else maker. Lender pays the opposite. Mirrors the
 * db-writer's match-time increment exactly.
 */
const splitTradeFees = (match: Match): {
  lenderTradeFee: string;
  borrowerTradeFee: string;
} => {
  const lenderTradeFee = match.borrowerIsTaker
    ? match.makerFeeAmount
    : match.takerFeeAmount;
  const borrowerTradeFee = match.borrowerIsTaker
    ? match.takerFeeAmount
    : match.makerFeeAmount;
  return { lenderTradeFee, borrowerTradeFee };
};

/**
 * Mark a single match as SETTLED and decrement both sides' locked_amount.
 * Returns true if the writeback transitioned the match from PENDING to
 * SETTLED, false if it was already settled (idempotent no-op).
 *
 * The decrement happens on the same `(account_id, asset_id)` row that
 * db-writer incremented; we read the IDs back from the matches row rather
 * than re-resolving (wallet → account_id, token → asset_id) because the
 * matches row is the canonical record and was already populated by
 * db-writer at match time.
 */
export const applyMatchSettlementWriteback = async (
  tx: PoolClient,
  match: Match,
  txHash: string,
): Promise<boolean> => {
  const { lenderTradeFee, borrowerTradeFee } = splitTradeFees(match);

  // Step 1: Conditional UPDATE — only flips PENDING → SETTLED.
  // RETURNING gives us the asset/account ids db-writer wrote at match time
  // so we don't need a wallet→account or token→asset lookup here.
  const settledRow = await tx.query<{
    lender_account_id: string;
    borrower_account_id: string;
    asset_id: string;
  }>(
    `UPDATE matches
        SET settlement_status = 'SETTLED',
            settled_tx_hash = $2,
            settled_at = NOW(),
            updated_at = NOW()
      WHERE id = $1
        AND settlement_status = 'PENDING'
      RETURNING lender_account_id, borrower_account_id, asset_id`,
    [match.matchId, txHash],
  );

  if (settledRow.rowCount === 0) {
    // Already SETTLED on a prior attempt — nothing else to do.
    return false;
  }

  const row = settledRow.rows[0]!;

  // Step 2: Lock release decomposition. Sort by account_id ascending to
  // mirror the db-writer's match-time deadlock-avoidance ordering — when
  // both sides happen to share the same (account_id, asset_id) the order
  // is irrelevant, but for the typical lender ≠ borrower case it keeps
  // any concurrent transactions consistent in their lock acquisition order.
  const lenderRelease = {
    accountId: row.lender_account_id,
    amounts: [
      match.matchedAmount,
      match.lenderSettlementFeeAmount,
      lenderTradeFee,
    ],
    sql: `UPDATE portfolio
             SET locked_amount = GREATEST(
                   locked_amount - ($1::numeric + $2::numeric + $3::numeric),
                   0),
                 updated_at = NOW()
           WHERE account_id = $4 AND asset_id = $5`,
    params: [
      match.matchedAmount,
      match.lenderSettlementFeeAmount,
      lenderTradeFee,
      row.lender_account_id,
      row.asset_id,
    ],
  };

  const borrowerRelease = {
    accountId: row.borrower_account_id,
    amounts: [match.borrowerSettlementFeeAmount, borrowerTradeFee],
    sql: `UPDATE portfolio
             SET locked_amount = GREATEST(
                   locked_amount - ($1::numeric + $2::numeric),
                   0),
                 updated_at = NOW()
           WHERE account_id = $3 AND asset_id = $4`,
    params: [
      match.borrowerSettlementFeeAmount,
      borrowerTradeFee,
      row.borrower_account_id,
      row.asset_id,
    ],
  };

  const ordered =
    row.lender_account_id < row.borrower_account_id
      ? [lenderRelease, borrowerRelease]
      : [borrowerRelease, lenderRelease];

  for (const release of ordered) {
    await tx.query(release.sql, release.params);
  }

  return true;
};

/**
 * Settlement-engine entry point: write back every settled match in a batch.
 * Each match runs in its own transaction so a partial failure on one match
 * doesn't block the rest. The on-chain settlement is already final at this
 * point (caller has the receipt) so retries are safe — see the idempotency
 * note on `applyMatchSettlementWriteback` above.
 *
 * Per-match transactions also avoid holding row locks across the full
 * batch, which can be 10+ matches under load.
 */
export const writebackSettledMatches = async (
  pool: Pool,
  matches: readonly Match[],
  settledMatchIds: ReadonlySet<string>,
  txHash: string,
): Promise<{ settled: number; alreadySettled: number }> => {
  let settled = 0;
  let alreadySettled = 0;

  for (const match of matches) {
    if (!settledMatchIds.has(match.matchId)) {
      continue;
    }

    try {
      const transitioned = await withTransaction(async (tx) => {
        return applyMatchSettlementWriteback(tx, match, txHash);
      });
      if (transitioned) {
        settled += 1;
      } else {
        alreadySettled += 1;
      }
    } catch (err) {
      // Observability — we don't rethrow because the on-chain settlement
      // already happened; a stuck `settlement_status='PENDING'` row gets
      // picked up by the (separate) reconciliation job. Rethrowing here
      // would block the rest of the batch's writeback.
      logger.error(
        {
          component: 'lock-release',
          matchId: match.matchId,
          txHash,
          err: (err as Error).message,
        },
        'Failed to apply match settlement writeback',
      );
    }
  }

  logger.info(
    {
      component: 'lock-release',
      txHash,
      settled,
      alreadySettled,
    },
    'Wrote back settled matches',
  );

  return { settled, alreadySettled };
};
