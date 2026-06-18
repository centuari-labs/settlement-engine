import { logger } from '../../logger';
import { getPool, withTransaction } from './connection';

/**
 * Stuck-PENDING settlement sweeper — database layer (Track C2).
 *
 * A match is "stuck" when it has been `settlement_status = 'PENDING'` past a
 * threshold: settlement never landed (engine crash mid-batch, the process died
 * before the lock-release writeback, or the already-settled fast-path acked
 * without persisting). Those paths bypass both the success writeback
 * (`lock-release.ts`) and the batch-failure handling (`order-failure.ts`), so
 * the match's `user_balance.in_orders` reservation is never released.
 *
 * `findStuckPendingMatches` detects them; `remediateUnsettledMatch` resolves a
 * match the sweeper has confirmed is NOT settled on-chain.
 *
 * The remediation SQL deliberately MIRRORS `order-failure.ts`
 * (`recordFailedMatches` + `restoreOrdersForFailedMatches` +
 * `unlockFailedMatches`) but runs all three in a SINGLE `withTransaction` gated
 * by a conditional `settlement_status = 'PENDING'` flip. That atomicity +
 * idempotency is the whole point: releasing `in_orders` and restoring orders
 * across three separate batch transactions (as the helpers do) would risk a
 * double-release if the process crashed between them. Keep this in sync with
 * `order-failure.ts` and the match-time increment in
 * `matching-engine/src/services/db/postgres-db-client.ts`.
 */

/** A candidate stuck-PENDING match, with everything needed to remediate it. */
export interface StuckMatch {
  readonly id: string;
  /** orders.id of the lend order (matches.lend_order_market_id currently holds orders.id). */
  readonly lendOrderId: string;
  /** orders.id of the borrow order. */
  readonly borrowOrderId: string;
  readonly lenderWallet: string;
  readonly borrowerWallet: string;
  /** Loan-token address — both sides reserve their lock against this asset. */
  readonly loanToken: string;
  readonly matchedAmount: string;
  readonly lenderSettlementFee: string;
  readonly borrowerSettlementFee: string;
  readonly makerFee: string;
  readonly takerFee: string;
  readonly borrowerIsTaker: boolean;
  readonly createdAt: Date;
}

/** Sentinel written to matches.settlement_failure_reason by the sweeper. */
export const SWEEPER_FAILURE_REASON = 'SWEEPER_NO_SETTLEMENT';

interface StuckMatchRow {
  id: string;
  lend_order_id: string;
  borrow_order_id: string;
  lender_wallet: string;
  borrower_wallet: string;
  loan_token: string;
  matched_amount: string;
  lender_settlement_fee: string;
  borrower_settlement_fee: string;
  maker_fee: string;
  taker_fee: string;
  borrower_is_taker: boolean;
  created_at: Date;
}

/**
 * Find matches stuck in `settlement_status = 'PENDING'` past `stuckThresholdMs`,
 * oldest first. Joins accounts (wallets) + assets (loan-token address) so the
 * caller has everything needed to release the lock without a second round-trip.
 *
 * Amounts are NUMERIC in the DB and selected `::text` so they round-trip as
 * exact integer base-unit strings (no float precision loss).
 *
 * @param stuckThresholdMs - Age (ms) past which a PENDING match is "stuck".
 * @param limit - Max candidates to return in one sweep.
 */
export const findStuckPendingMatches = async (
  stuckThresholdMs: number,
  limit: number,
): Promise<StuckMatch[]> => {
  const result = await getPool().query<StuckMatchRow>(
    `
    SELECT
      m.id,
      m.lend_order_market_id          AS lend_order_id,
      m.borrow_order_market_id        AS borrow_order_id,
      la.user_wallet                  AS lender_wallet,
      ba.user_wallet                  AS borrower_wallet,
      a.token_address                 AS loan_token,
      m.match_amount::text            AS matched_amount,
      m.lender_settlement_fee::text   AS lender_settlement_fee,
      m.borrower_settlement_fee::text AS borrower_settlement_fee,
      m.maker_fee::text               AS maker_fee,
      m.taker_fee::text               AS taker_fee,
      m.is_borrower_taker             AS borrower_is_taker,
      m.created_at                    AS created_at
    FROM matches m
    JOIN accounts la ON la.id = m.lender_account_id
    JOIN accounts ba ON ba.id = m.borrower_account_id
    JOIN assets   a  ON a.id  = m.asset_id
    WHERE m.settlement_status = 'PENDING'
      AND m.created_at < NOW() - ($1::bigint * INTERVAL '1 millisecond')
    ORDER BY m.created_at ASC
    LIMIT $2
    `,
    [stuckThresholdMs, limit],
  );

  return result.rows.map((row) => ({
    id: row.id,
    lendOrderId: row.lend_order_id,
    borrowOrderId: row.borrow_order_id,
    lenderWallet: row.lender_wallet,
    borrowerWallet: row.borrower_wallet,
    loanToken: row.loan_token,
    matchedAmount: row.matched_amount,
    lenderSettlementFee: row.lender_settlement_fee,
    borrowerSettlementFee: row.borrower_settlement_fee,
    makerFee: row.maker_fee,
    takerFee: row.taker_fee,
    borrowerIsTaker: row.borrower_is_taker,
    createdAt: row.created_at,
  }));
};

/**
 * Atomically resolve a match the sweeper has confirmed is NOT settled on-chain:
 *   1. Flip `settlement_status` PENDING -> FAILED (conditional — this is the
 *      idempotency gate; a concurrent settle/fail makes this a no-op).
 *   2. Restore both orders' filled quantity/fee (re-list remainder or cancel),
 *      mirroring `restoreOrdersForFailedMatches`.
 *   3. Release both sides' `user_balance.in_orders`, mirroring
 *      `unlockFailedMatches` (same decomposition + deadlock-safe wallet order).
 *
 * All in one transaction: if anything fails it rolls back wholesale and the
 * match stays PENDING to be retried next sweep — never partially released.
 *
 * @returns true if this call actioned the match; false if it was already
 *          resolved (no longer PENDING) and nothing was done.
 */
export const remediateUnsettledMatch = async (
  match: StuckMatch,
): Promise<boolean> => {
  return withTransaction(async (client) => {
    // 1. Idempotency gate: only the transition out of PENDING does the work.
    const flip = await client.query(
      `
      UPDATE matches
      SET settlement_status = 'FAILED',
          settlement_failure_reason = $2,
          updated_at = NOW()
      WHERE id = $1 AND settlement_status = 'PENDING'
      `,
      [match.id, SWEEPER_FAILURE_REASON],
    );

    if (!flip.rowCount) {
      logger.info(
        { component: 'sweeper', matchId: match.id },
        'Match no longer PENDING; skipping remediation',
      );
      return false;
    }

    // 2. Restore order quantities (mirrors order-failure.restoreOrdersForFailedMatches).
    const restoreOrderSql = `
      UPDATE orders
      SET filled_quantity = GREATEST(0, filled_quantity::numeric - $2::numeric),
          filled_settlement_fee = GREATEST(0, filled_settlement_fee::numeric - $3::numeric),
          status = CASE
            WHEN filled_quantity::numeric - $2::numeric <= 0 THEN 'CANCELLED'
            ELSE 'PARTIALLY_FILLED'
          END,
          cancel_reason = CASE
            WHEN filled_quantity::numeric - $2::numeric <= 0 THEN 'SETTLEMENT_FAILED'
            ELSE cancel_reason
          END,
          updated_at = NOW()
      WHERE id = $1
    `;
    await client.query(restoreOrderSql, [
      match.lendOrderId,
      match.matchedAmount,
      match.lenderSettlementFee,
    ]);
    await client.query(restoreOrderSql, [
      match.borrowOrderId,
      match.matchedAmount,
      match.borrowerSettlementFee,
    ]);

    // 3. Release both sides' in_orders lock (mirrors order-failure.unlockFailedMatches).
    const lenderTradeFee = match.borrowerIsTaker ? match.makerFee : match.takerFee;
    const borrowerTradeFee = match.borrowerIsTaker ? match.takerFee : match.makerFee;

    const lenderUnlock = {
      wallet: match.lenderWallet,
      query: `
        UPDATE user_balance
        SET in_orders = GREATEST(0, in_orders - ($1::numeric + $2::numeric + $3::numeric)),
            updated_at = NOW()
        WHERE user_address = decode(substring($4 from 3), 'hex')
          AND asset = decode(substring($5 from 3), 'hex')
      `,
      params: [
        match.matchedAmount,
        match.lenderSettlementFee,
        lenderTradeFee,
        match.lenderWallet,
        match.loanToken,
      ],
    };

    const borrowerUnlock = {
      wallet: match.borrowerWallet,
      query: `
        UPDATE user_balance
        SET in_orders = GREATEST(0, in_orders - ($1::numeric + $2::numeric)),
            updated_at = NOW()
        WHERE user_address = decode(substring($3 from 3), 'hex')
          AND asset = decode(substring($4 from 3), 'hex')
      `,
      params: [
        match.borrowerSettlementFee,
        borrowerTradeFee,
        match.borrowerWallet,
        match.loanToken,
      ],
    };

    // Lock the lower wallet first to avoid deadlocks with concurrent writeback.
    const ordered =
      lenderUnlock.wallet.toLowerCase() < borrowerUnlock.wallet.toLowerCase()
        ? [lenderUnlock, borrowerUnlock]
        : [borrowerUnlock, lenderUnlock];

    for (const update of ordered) {
      await client.query(update.query, update.params);
    }

    return true;
  });
};
