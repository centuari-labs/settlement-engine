import type { Match } from '../../schemas/match';
import { logger } from '../../logger';
import { getPool } from './connection';

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
