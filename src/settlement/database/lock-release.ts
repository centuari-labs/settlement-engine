import type { Pool, PoolClient } from 'pg';
import type { Match } from '../../schemas/match';
import { logger } from '../../logger';
import { withTransaction } from './connection';

/**
 * Settlement-engine writeback for the order-lock lifecycle.
 *
 * The db-writer (matching-engine codebase) increments
 * `user_balance.in_orders` at MATCH time for both lender and borrower
 * — see matching-engine/src/services/db/postgres-db-client.ts:251-271
 * for the exact decomposition that this file mirrors. Once the on-chain
 * settlement lands the reserved funds are spent, so without a corresponding
 * decrement to `in_orders` the user's reservable balance stays under-counted
 * for as long as the lock is stale.
 *
 * This helper runs alongside `applyOnChainEffect` (called from
 * `applySettlementResult` in apply-settlement.ts AFTER the per-event
 * position writes), in its own per-match `withTransaction`. It does NOT
 * extend the `applyOnChainEffect` mutation closure — keeping the
 * writeback in a separate transaction means a partial failure on one
 * match's writeback doesn't block writeback for the rest of the batch.
 *
 * For each settled match it:
 *   1. Marks `matches.settlement_status = 'SETTLED'` (idempotent — only
 *      transitions PENDING → SETTLED via the WHERE guard).
 *   2. Decrements both sides of `user_balance.in_orders` by the exact
 *      amounts the db-writer added at match time.
 *
 * Idempotency: a retried settlement attempt on the same match is a natural
 * no-op because the `settlement_status` UPDATE returns no rows on the
 * second run, and we only fire the in_orders decrements when that UPDATE
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
 * Mark a single match as SETTLED and decrement both sides' in_orders.
 * Returns true if the writeback transitioned the match from PENDING to
 * SETTLED, false if it was already settled (idempotent no-op).
 *
 * The decrement happens on the same `user_balance (user_address, asset)`
 * row the db-writer incremented, keyed by the match's BYTEA-encoded wallet
 * + loan token (both sides lock the loan token).
 */
export const applyMatchSettlementWriteback = async (
  tx: PoolClient,
  match: Match,
  txHash: string,
): Promise<boolean> => {
  const { lenderTradeFee, borrowerTradeFee } = splitTradeFees(match);

  // Step 1: Conditional UPDATE — only flips PENDING → SETTLED. rowCount is
  // non-zero only on the transition, so the in_orders decrements below fire
  // exactly once per match (idempotent across settlement retries).
  const settledRow = await tx.query(
    `UPDATE matches
        SET settlement_status = 'SETTLED',
            settled_tx_hash = $2,
            settled_at = NOW(),
            updated_at = NOW()
      WHERE id = $1
        AND settlement_status = 'PENDING'`,
    [match.matchId, txHash],
  );

  if (settledRow.rowCount === 0) {
    // Already SETTLED on a prior attempt — nothing else to do.
    return false;
  }

  // Step 2: Lock release decomposition on `user_balance.in_orders`, keyed by
  // BYTEA (user_address, asset). Sort by wallet ascending to mirror the
  // db-writer's match-time deadlock-avoidance ordering. Both sides lock the
  // loan token, so `asset` is `match.loanToken` for lender and borrower.
  const lenderRelease = {
    wallet: match.lenderWallet,
    sql: `UPDATE user_balance
             SET in_orders = GREATEST(
                   in_orders - ($1::numeric + $2::numeric + $3::numeric),
                   0),
                 updated_at = NOW()
           WHERE user_address = decode(substring($4 from 3), 'hex')
             AND asset = decode(substring($5 from 3), 'hex')`,
    params: [
      match.matchedAmount,
      match.lenderSettlementFeeAmount,
      lenderTradeFee,
      match.lenderWallet,
      match.loanToken,
    ],
  };

  const borrowerRelease = {
    wallet: match.borrowerWallet,
    sql: `UPDATE user_balance
             SET in_orders = GREATEST(
                   in_orders - ($1::numeric + $2::numeric),
                   0),
                 updated_at = NOW()
           WHERE user_address = decode(substring($3 from 3), 'hex')
             AND asset = decode(substring($4 from 3), 'hex')`,
    params: [
      match.borrowerSettlementFeeAmount,
      borrowerTradeFee,
      match.borrowerWallet,
      match.loanToken,
    ],
  };

  const ordered =
    lenderRelease.wallet.toLowerCase() < borrowerRelease.wallet.toLowerCase()
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
