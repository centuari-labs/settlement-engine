import type { Pool, PoolClient } from 'pg';
import { Pool as PgPool } from 'pg';
import { z } from 'zod';
import type {
  SettlementResult,
  ParsedBondToken,
  ParsedLendPosition,
  ParsedBorrowPosition,
} from './smartContract';
import type { Match } from '../schemas/match';
import {
  bytes32ToUuid,
  positionUuidFor,
  cbtAssetUuidFor,
} from './helpers';

/**
 * Error information for failed database operations.
 */
export interface DatabaseError {
  /**
   * Error message describing the failure.
   */
  readonly message: string;
  /**
   * Error code if available.
   */
  readonly code?: string;
  /**
   * Whether the error is retryable (transient).
   */
  readonly retryable: boolean;
}

/**
 * Options for persisting settlement results to the database.
 */
export interface PersistSettlementResultsOptions {
  /**
   * Array of settlement results to persist.
   */
  readonly results: readonly SettlementResult[];
  /**
   * Map of matchId → Match payload for upserting match rows before settlement items.
   * This ensures the foreign key constraint on settlement_items is satisfied
   * even if the DB writer hasn't persisted the match yet.
   */
  readonly matchPayloads: ReadonlyMap<string, Match>;
  /**
   * Maximum number of retries for transient errors.
   */
  readonly maxRetries?: number;
  /**
   * Initial retry delay in milliseconds (exponential backoff).
   */
  readonly retryDelayMs?: number;
}

/**
 * Allowed settlement batch statuses.
 *
 * We only persist completed or failed batches because the smart contract
 * settlement call waits until the transaction is mined before we write to
 * the database.
 */
export const settlementBatchStatusSchema = z.enum(['COMPLETED', 'FAILED']);

export type SettlementBatchStatus = z.infer<typeof settlementBatchStatusSchema>;

/**
 * Minimal shape of a settlement batch record.
 */
export interface SettlementBatch {
  readonly id: string;
  readonly txHash: string;
  readonly status: SettlementBatchStatus;
}

/**
 * Minimal shape of a settlement item record.
 */
export interface SettlementItem {
  readonly id: string;
  readonly settlementBatchId: string;
  readonly matchId: string;
}

/**
 * Zod schema for database URL validation.
 */
const databaseUrlSchema = z.string().url('DATABASE_URL must be a valid URL');

/**
 * Singleton Postgres pool instance.
 */
let pool: Pool | null = null;

/**
 * Get or create the Postgres pool.
 *
 * @returns Postgres connection pool.
 * @throws Error if DATABASE_URL is not set or invalid.
 */
const getPool = (): Pool => {
  if (pool) {
    return pool;
  }

  const databaseUrl = process.env.DATABASE_URL;

  if (!databaseUrl) {
    throw new Error(
      'DATABASE_URL environment variable is not set. Please configure a database connection string.',
    );
  }

  const parsed = databaseUrlSchema.parse(databaseUrl);

  pool = new PgPool({
    connectionString: parsed,
  });

  return pool;
};

/**
 * Run a function within a database transaction.
 *
 * @param fn - Function that receives a client bound to an open transaction.
 * @returns Result of the function.
 */
const withTransaction = async <T>(fn: (client: PoolClient) => Promise<T>): Promise<T> => {
  const client = await getPool().connect();
  try {
    await client.query('BEGIN');
    const result = await fn(client);
    await client.query('COMMIT');
    return result;
  } catch (error) {
    try {
      await client.query('ROLLBACK');
    } catch {
      // Ignore rollback errors
    }
    throw error;
  } finally {
    client.release();
  }
};

/**
 * Normalize a Postgres error into a DatabaseError.
 *
 * @param error - Original error.
 * @returns Normalized DatabaseError.
 */
const mapPostgresErrorToDatabaseError = (error: unknown): DatabaseError => {
  const err = error as { message?: string; code?: string };
  const message = err.message ?? 'Unknown database error';
  const code = err.code;

  // Retryable errors: connection issues, serialization failures, deadlocks, admin shutdown.
  const retryableCodes = new Set([
    '40001', // serialization_failure
    '40P01', // deadlock_detected
    '55P03', // lock_not_available
    '57P01', // admin_shutdown
    '57P02', // crash_shutdown
    '57P03', // cannot_connect_now
    '08006', // connection_failure
    '08001', // sqlclient_unable_to_establish_sqlconnection
    '08003', // connection_does_not_exist
  ]);

  // Non-retryable errors: constraint violations, invalid enum, invalid input, etc.
  const nonRetryableCodes = new Set([
    '23505', // unique_violation
    '23503', // foreign_key_violation
    '23502', // not_null_violation
    '23514', // check_violation
    '22P02', // invalid_text_representation
  ]);

  let retryable = false;
  if (code) {
    if (retryableCodes.has(code)) {
      retryable = true;
    } else if (nonRetryableCodes.has(code)) {
      retryable = false;
    } else {
      // Default for unknown codes: assume retryable for safety.
      retryable = true;
    }
  }

  return {
    message,
    code,
    retryable,
  };
};

/**
 * Execute a database operation with retry and exponential backoff.
 *
 * @param operation - Operation to execute.
 * @param maxRetries - Maximum number of retries.
 * @param retryDelayMs - Initial retry delay in milliseconds.
 * @returns Result of the operation.
 * @throws DatabaseError if the operation ultimately fails.
 */
const executeWithRetry = async <T>(
  operation: () => Promise<T>,
  maxRetries: number,
  retryDelayMs: number,
): Promise<T> => {
  let attempt = 0;
  let delay = retryDelayMs;

  // eslint-disable-next-line no-constant-condition
  while (true) {
    try {
      return await operation();
    } catch (error) {
      const dbError = mapPostgresErrorToDatabaseError(error);

      if (!dbError.retryable || attempt >= maxRetries) {
        // eslint-disable-next-line no-console
        console.error(
          '[database] Persistence failed (non-retryable or max retries reached)',
          {
            message: dbError.message,
            code: dbError.code,
            retryable: dbError.retryable,
            attempt,
            maxRetries,
          },
        );
        throw dbError;
      }

      // eslint-disable-next-line no-console
      console.warn(
        `[database] Retryable error during persistence (attempt ${attempt + 1} of ${
          maxRetries + 1
        })`,
        {
          message: dbError.message,
          code: dbError.code,
        },
      );

      await new Promise((resolve) => setTimeout(resolve, delay));
      delay *= 2;
      attempt += 1;
    }
  }
};

/**
 * Persist BondTokenCreated event: ensure market and cbt_asset exist.
 */
const persistBondTokenCreated = async (
  client: PoolClient,
  ev: ParsedBondToken,
  batchId: string,
): Promise<void> => {
  const marketIdUuid = bytes32ToUuid(ev.marketId);

  // Look up asset by loanToken address
  const assetRows = await client.query<{ id: string }>(
    `SELECT id FROM assets WHERE LOWER(token_address) = LOWER($1) LIMIT 1`,
    [ev.loanToken],
  );
  const asset = assetRows.rows[0];
  if (!asset) return;

  // Upsert market (insert if not exists)
  await client.query(
    `
      INSERT INTO markets (id, asset_id, maturity, created_at)
      VALUES ($1, $2, to_timestamp($3::bigint), NOW())
      ON CONFLICT (id) DO NOTHING
    `,
    [marketIdUuid, asset.id, ev.maturity.toString()],
  );

  // Upsert cbt_asset
  const cbtAssetId = cbtAssetUuidFor(ev.marketId, ev.bondToken);
  await client.query(
    `
      INSERT INTO cbt_assets (id, market_id, name, symbol, token_address, settlement_batch_id, created_at, updated_at)
      VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())
      ON CONFLICT (id) DO UPDATE SET
        name = EXCLUDED.name,
        symbol = EXCLUDED.symbol,
        token_address = EXCLUDED.token_address,
        settlement_batch_id = EXCLUDED.settlement_batch_id,
        updated_at = NOW()
    `,
    [cbtAssetId, marketIdUuid, ev.name, ev.symbol, ev.bondToken, batchId],
  );
};

/**
 * Persist LendPositionCreated event: upsert lend_position.
 */
const persistLendPositionCreated = async (
  client: PoolClient,
  ev: ParsedLendPosition,
  batchId: string,
): Promise<void> => {
  const marketIdUuid = bytes32ToUuid(ev.marketId);
  const positionId = positionUuidFor(ev.marketId, ev.lender);

  const accountRows = await client.query<{ id: string }>(
    `SELECT id FROM accounts WHERE LOWER(user_wallet) = LOWER($1) LIMIT 1`,
    [ev.lender],
  );
  const account = accountRows.rows[0];
  if (!account) return;

  const marketRows = await client.query<{ asset_id: string }>(
    `SELECT asset_id FROM markets WHERE id = $1 LIMIT 1`,
    [marketIdUuid],
  );
  const market = marketRows.rows[0];
  if (!market) return;

  const cbtAssetRows = await client.query<{ id: string }>(
    `SELECT id FROM cbt_assets WHERE LOWER(token_address) = LOWER($1) AND market_id = $2 LIMIT 1`,
    [ev.bondToken, marketIdUuid],
  );
  const cbtAsset = cbtAssetRows.rows[0];
  const cbtAssetId = cbtAsset?.id ?? null;

  const cbtAmount = Number(ev.cbtAmount);
  const principal = Number(ev.principal);

  await client.query(
    `
      INSERT INTO lend_positions (id, account_id, asset_id, market_id, cbt_asset_id, settlement_batch_id, shares, original_shares, amount, created_at, updated_at)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW(), NOW())
      ON CONFLICT (id) DO UPDATE SET
        cbt_asset_id = COALESCE(EXCLUDED.cbt_asset_id, lend_positions.cbt_asset_id),
        shares = lend_positions.shares + EXCLUDED.shares,
        original_shares = lend_positions.original_shares + EXCLUDED.original_shares,
        amount = lend_positions.amount + EXCLUDED.amount,
        settlement_batch_id = EXCLUDED.settlement_batch_id,
        updated_at = NOW()
    `,
    [
      positionId,
      account.id,
      market.asset_id,
      marketIdUuid,
      cbtAssetId,
      batchId,
      cbtAmount,
      cbtAmount,
      principal,
    ],
  );
};

/**
 * Persist BorrowPositionCreated event: upsert borrow_position.
 */
const persistBorrowPositionCreated = async (
  client: PoolClient,
  ev: ParsedBorrowPosition,
  batchId: string,
): Promise<void> => {
  const marketIdUuid = bytes32ToUuid(ev.marketId);
  const positionId = positionUuidFor(ev.marketId, ev.borrower);

  const accountRows = await client.query<{ id: string }>(
    `SELECT id FROM accounts WHERE LOWER(user_wallet) = LOWER($1) LIMIT 1`,
    [ev.borrower],
  );
  const account = accountRows.rows[0];
  if (!account) return;

  const marketRows = await client.query<{ asset_id: string }>(
    `SELECT asset_id FROM markets WHERE id = $1 LIMIT 1`,
    [marketIdUuid],
  );
  const market = marketRows.rows[0];
  if (!market) return;

  const principal = Number(ev.principal);
  const debt = Number(ev.debt);

  await client.query(
    `
      INSERT INTO borrow_positions (id, account_id, asset_id, market_id, settlement_batch_id, amount, original_debt, debt, created_at, updated_at)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW(), NOW())
      ON CONFLICT (id) DO UPDATE SET
        original_debt = borrow_positions.original_debt + EXCLUDED.original_debt,
        debt = borrow_positions.debt + EXCLUDED.debt,
        amount = borrow_positions.amount + EXCLUDED.amount,
        settlement_batch_id = EXCLUDED.settlement_batch_id,
        updated_at = NOW()
    `,
    [positionId, account.id, market.asset_id, marketIdUuid, batchId, principal, debt, debt],
  );
};

/**
 * Upsert a match row to satisfy the foreign key constraint on settlement_items.
 * Uses ON CONFLICT DO NOTHING so it's safe to call even if the DB writer has
 * already persisted the match.
 */
const upsertMatch = async (
  client: PoolClient,
  matchId: string,
  match: Match,
): Promise<void> => {
  // Look up asset by loanToken address
  const assetRows = await client.query<{ id: string }>(
    `SELECT id FROM assets WHERE LOWER(token_address) = LOWER($1) LIMIT 1`,
    [match.loanToken],
  );
  const asset = assetRows.rows[0];
  if (!asset) {
    // eslint-disable-next-line no-console
    console.warn(`[database] Asset not found for token ${match.loanToken}, skipping match upsert for ${matchId}`);
    return;
  }

  // Look up lender account
  const lenderRows = await client.query<{ id: string }>(
    `SELECT id FROM accounts WHERE LOWER(user_wallet) = LOWER($1) LIMIT 1`,
    [match.lenderWallet],
  );
  const lenderAccount = lenderRows.rows[0];
  if (!lenderAccount) {
    // eslint-disable-next-line no-console
    console.warn(`[database] Lender account not found for wallet ${match.lenderWallet}, skipping match upsert for ${matchId}`);
    return;
  }

  // Look up borrower account
  const borrowerRows = await client.query<{ id: string }>(
    `SELECT id FROM accounts WHERE LOWER(user_wallet) = LOWER($1) LIMIT 1`,
    [match.borrowerWallet],
  );
  const borrowerAccount = borrowerRows.rows[0];
  if (!borrowerAccount) {
    // eslint-disable-next-line no-console
    console.warn(`[database] Borrower account not found for wallet ${match.borrowerWallet}, skipping match upsert for ${matchId}`);
    return;
  }

  await client.query(
    `
      INSERT INTO matches (
        id, lend_order_market_id, borrow_order_market_id, asset_id,
        lender_account_id, borrower_account_id, match_amount, rate,
        is_borrower_taker, maker_fee, taker_fee,
        lender_settlement_fee, borrower_settlement_fee,
        maturity, created_at, updated_at
      )
      VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
        to_timestamp($14 / 1000.0), to_timestamp($15 / 1000.0), to_timestamp($15 / 1000.0)
      )
      ON CONFLICT (id) DO NOTHING
    `,
    [
      matchId,
      match.lendOrderId,
      match.borrowOrderId,
      asset.id,
      lenderAccount.id,
      borrowerAccount.id,
      match.matchedAmount,
      match.rate,
      match.borrowerIsTaker,
      match.makerFeeAmount,
      match.takerFeeAmount,
      match.lenderSettlementFeeAmount,
      match.borrowerSettlementFeeAmount,
      match.maturity,
      match.timestamp,
    ],
  );
};

/**
 * Persist settlement results to the database.
 *
 * Inserts settlement batches, settlement items, and processes parsed events
 * (BondTokenCreated, LendPositionCreated, BorrowPositionCreated) in dependency order.
 *
 * @param options - Options for persisting settlement results.
 * @returns Promise that resolves when persistence is complete.
 * @throws DatabaseError if the persistence fails.
 */
export const persistSettlementResults = async (
  options: PersistSettlementResultsOptions,
): Promise<void> => {
  const { results, matchPayloads, maxRetries = 3, retryDelayMs = 1000 } = options;

  if (results.length === 0) {
    return;
  }

  // eslint-disable-next-line no-console
  console.log('[database] Persisting settlement results', {
    batchCount: results.length,
    transactionHashes: results.map((result) => result.transactionHash),
  });

  await executeWithRetry(
    () =>
      withTransaction(async (client) => {
        // Insert one settlement batch per settlement result.
        for (const result of results) {
          const batchInsert = await client.query<{
            id: string;
          }>(
            `
              INSERT INTO settlement_batches (tx_hash, status, created_at, updated_at)
              VALUES ($1, $2, NOW(), NOW())
              RETURNING id
            `,
            [result.transactionHash, 'COMPLETED'],
          );

          if (batchInsert.rows.length === 0) {
            throw new Error('Failed to insert settlement batch');
          }

          const batchId = batchInsert.rows[0].id;

          // Ensure match rows exist before inserting settlement items
          // to avoid FK violation race condition with the DB writer.
          for (const matchId of result.settledMatchIds) {
            const matchPayload = matchPayloads.get(matchId);
            if (matchPayload) {
              await upsertMatch(client, matchId, matchPayload);
            }
          }

          // Insert settlement items for each settled match ID.
          for (const matchId of result.settledMatchIds) {
            await client.query(
              `
                INSERT INTO settlement_items (settlement_batch_id, match_id, created_at, updated_at)
                VALUES ($1, $2, NOW(), NOW())
              `,
              [batchId, matchId],
            );
          }

          // Step A: BondTokenCreated -> markets + cbt_assets (must run first)
          for (const ev of result.bondTokenEvents) {
            await persistBondTokenCreated(client, ev, batchId);
          }

          // Step B: LendPositionCreated -> lend_positions
          for (const ev of result.lendPositionEvents) {
            await persistLendPositionCreated(client, ev, batchId);
          }

          // Step C: BorrowPositionCreated -> borrow_positions
          for (const ev of result.borrowPositionEvents) {
            await persistBorrowPositionCreated(client, ev, batchId);
          }
        }
      }),
    maxRetries,
    retryDelayMs,
  );

  // eslint-disable-next-line no-console
  console.log('[database] Settlement results persisted successfully', {
    batchCount: results.length,
  });
};

