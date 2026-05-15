import type { Match } from '../../schemas/match';
import { logger } from '../../logger';
import { getPool } from './connection';

/**
 * Failure-path helpers for matching-engine reservation state.
 *
 * These touch legacy backend-v2 UUID tables (`portfolio.locked_amount`,
 * `matches.settlement_status`, `orders`) and are invoked only when a batch
 * hits a non-retryable smart-contract error. They stay in place until
 * Phase B retires matching-engine's reservation model; Phase A does not
 * own this state.
 *
 * Extracted verbatim from the deleted `recovery.ts` — only the
 * `events_processed` loop went away.
 */

/**
 * Unlock portfolio locked_amount for matches that failed with a non-retryable error.
 * This prevents locked amounts from being stuck forever when settlement cannot proceed.
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

      // Resolve account IDs from wallet addresses
      const lenderResult = await client.query<{ id: string }>(
        `SELECT id FROM accounts WHERE LOWER(user_wallet) = LOWER($1) LIMIT 1`,
        [match.lenderWallet],
      );
      const borrowerResult = await client.query<{ id: string }>(
        `SELECT id FROM accounts WHERE LOWER(user_wallet) = LOWER($1) LIMIT 1`,
        [match.borrowerWallet],
      );

      if (!lenderResult.rows[0] || !borrowerResult.rows[0]) {
        logger.warn({ component: 'database', matchId: match.matchId }, 'Cannot unlock match: account not found');
        continue;
      }

      // Resolve asset ID from loan token address
      const assetResult = await client.query<{ id: string }>(
        `SELECT id FROM assets WHERE LOWER(token_address) = LOWER($1) LIMIT 1`,
        [match.loanToken],
      );
      if (!assetResult.rows[0]) {
        logger.warn({ component: 'database', matchId: match.matchId, loanToken: match.loanToken }, 'Cannot unlock match: asset not found');
        continue;
      }

      const lenderAccountId = lenderResult.rows[0].id;
      const borrowerAccountId = borrowerResult.rows[0].id;
      const assetId = assetResult.rows[0].id;

      // Build lender and borrower unlock updates
      const lenderUnlock = {
        accountId: lenderAccountId,
        query: `
          UPDATE portfolio
          SET locked_amount = GREATEST(0, locked_amount - ($1::numeric + $2::numeric + $3::numeric)),
              updated_at = NOW()
          WHERE account_id = $4 AND asset_id = $5
        `,
        params: [
          match.matchedAmount,
          match.lenderSettlementFeeAmount,
          lenderTradeFee,
          lenderAccountId,
          assetId,
        ],
      };

      const borrowerUnlock = {
        accountId: borrowerAccountId,
        query: `
          UPDATE portfolio
          SET locked_amount = GREATEST(0, locked_amount - ($1::numeric + $2::numeric)),
              updated_at = NOW()
          WHERE account_id = $3 AND asset_id = $4
        `,
        params: [
          match.borrowerSettlementFeeAmount,
          borrowerTradeFee,
          borrowerAccountId,
          assetId,
        ],
      };

      // Always lock lower account ID first to prevent deadlocks
      const ordered = lenderUnlock.accountId < borrowerUnlock.accountId
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
