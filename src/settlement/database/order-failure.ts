import type { Match } from '../../schemas/match';
import { logger } from '../../logger';
import { getPool, withTransaction } from './connection';

/**
 * Sentinel written to matches.settlement_failure_reason when a match is
 * quarantined by poison-match isolation (Track C8). Distinct from the C2
 * sweeper's SWEEPER_NO_SETTLEMENT. A decoded contract error name may be
 * appended, e.g. `POISON_PREFLIGHT_REVERT:INSUFFICIENT_FUNDS`.
 */
export const POISON_FAILURE_REASON = 'POISON_PREFLIGHT_REVERT';

/**
 * Failure-path helpers for matching-engine reservation state.
 *
 * Invoked only when a batch hits a non-retryable smart-contract error.
 * `unlockFailedMatches` releases the match-time `user_balance.in_orders`
 * lock; `recordFailedMatches` / `restoreOrdersForFailedMatches` reset the
 * `matches` + `orders` rows so the order book can re-list the quantity.
 */

/**
 * Release the user_balance.in_orders lock for matches that failed with a
 * non-retryable error. Prevents reserved amounts from being stuck forever
 * when settlement cannot proceed.
 */
export const unlockFailedMatches = async (
  matches: readonly Match[],
): Promise<void> => {
  const db = getPool();
  const client = await db.connect();

  try {
    await client.query('BEGIN');

    for (const match of matches) {
      const lenderTradeFee = match.borrowerIsTaker ? match.makerFeeAmount : match.takerFeeAmount;
      const borrowerTradeFee = match.borrowerIsTaker ? match.takerFeeAmount : match.makerFeeAmount;

      // Release the match-time in_orders lock on user_balance, keyed by BYTEA
      // (user_address, asset). Both sides lock the loan token. Mirrors the
      // decrement in lock-release.ts.
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
          match.lenderSettlementFeeAmount,
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
          match.borrowerSettlementFeeAmount,
          borrowerTradeFee,
          match.borrowerWallet,
          match.loanToken,
        ],
      };

      // Always lock lower wallet first to prevent deadlocks
      const ordered = lenderUnlock.wallet.toLowerCase() < borrowerUnlock.wallet.toLowerCase()
        ? [lenderUnlock, borrowerUnlock]
        : [borrowerUnlock, lenderUnlock];

      for (const update of ordered) {
        await client.query(update.query, update.params);
      }
    }

    await client.query('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK');
    logger.error({ component: 'database', err: error }, 'Failed to unlock failed matches');
  } finally {
    client.release();
  }
};

/**
 * Mark matches as FAILED in the database with a failure reason.
 * Called when settlement fails with a non-retryable error.
 */
export const recordFailedMatches = async (
  matches: readonly Match[],
  failureReason: string,
): Promise<void> => {
  const db = getPool();
  const client = await db.connect();

  try {
    await client.query('BEGIN');

    for (const match of matches) {
      await client.query(
        `
        UPDATE matches
        SET settlement_status = 'FAILED',
            settlement_failure_reason = $2,
            updated_at = NOW()
        WHERE id = $1
        `,
        [match.matchId, failureReason],
      );
    }

    await client.query('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK');
    logger.error({ component: 'database', err: error }, 'Failed to record failed matches');
  } finally {
    client.release();
  }
};

/**
 * Restore order quantities for failed matches.
 * Reduces filled_quantity by the match amount and updates order status:
 * - If filled_quantity drops to 0 → CANCELLED with reason SETTLEMENT_FAILED
 * - If filled_quantity > 0 → PARTIALLY_FILLED (other matches settled OK)
 */
export const restoreOrdersForFailedMatches = async (
  matches: readonly Match[],
): Promise<void> => {
  const db = getPool();
  const client = await db.connect();

  try {
    await client.query('BEGIN');

    for (const match of matches) {
      const lenderSettlementFee = match.lenderSettlementFeeAmount;
      const borrowerSettlementFee = match.borrowerSettlementFeeAmount;

      // Restore lend order
      await client.query(
        `
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
        `,
        [match.lendOrderId, match.matchedAmount, lenderSettlementFee],
      );

      // Restore borrow order
      await client.query(
        `
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
        `,
        [match.borrowOrderId, match.matchedAmount, borrowerSettlementFee],
      );
    }

    await client.query('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK');
    logger.error({ component: 'database', err: error }, 'Failed to restore orders for failed matches');
  } finally {
    client.release();
  }
};

/**
 * Quarantine a single poison match (Track C8): atomically flip its
 * `settlement_status` PENDING -> FAILED, restore both orders, and release both
 * sides' `user_balance.in_orders` lock — all in ONE transaction.
 *
 * Unlike the three-helper batch path (`unlockFailedMatches` +
 * `recordFailedMatches` + `restoreOrdersForFailedMatches`, which run as three
 * separate transactions), this mirrors the sweeper's `remediateUnsettledMatch`:
 * a conditional `PENDING -> FAILED` flip gates the work, so a crash leaves the
 * match PENDING (never partially released) and a retry is a no-op. This is the
 * right shape for dropping one bad match out of an otherwise-valid batch while
 * the survivors settle.
 *
 * @param match - The poison match to quarantine.
 * @param failureReason - Sentinel stored in `settlement_failure_reason`
 *   (e.g. {@link POISON_FAILURE_REASON} optionally suffixed with the decoded
 *   contract error name).
 * @returns true if this call actioned the match; false if it was already
 *          resolved (no longer PENDING) and nothing was done.
 */
export const quarantineFailedMatch = async (
  match: Match,
  failureReason: string,
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
      [match.matchId, failureReason],
    );

    if (!flip.rowCount) {
      logger.info(
        { component: 'poison-isolation', matchId: match.matchId },
        'Match no longer PENDING; skipping quarantine',
      );
      return false;
    }

    // 2. Restore order quantities (mirrors restoreOrdersForFailedMatches).
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
      match.lenderSettlementFeeAmount,
    ]);
    await client.query(restoreOrderSql, [
      match.borrowOrderId,
      match.matchedAmount,
      match.borrowerSettlementFeeAmount,
    ]);

    // 3. Release both sides' in_orders lock (mirrors unlockFailedMatches).
    const lenderTradeFee = match.borrowerIsTaker ? match.makerFeeAmount : match.takerFeeAmount;
    const borrowerTradeFee = match.borrowerIsTaker ? match.takerFeeAmount : match.makerFeeAmount;

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
        match.lenderSettlementFeeAmount,
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
        match.borrowerSettlementFeeAmount,
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
