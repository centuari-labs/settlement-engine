import type { Pool, PoolClient } from 'pg';
import { Pool as PgPool } from 'pg';
import { z } from 'zod';
import type { Address } from 'viem';
import type {
  SettlementResult,
  ParsedBondToken,
  ParsedLendPosition,
  ParsedBorrowPosition,
} from './smartContract';
import { getPublicClient } from './smartContract';
import type { Match } from '../schemas/match';
import type { AppConfig } from '../config';
import {
  bytes32ToUuid,
  positionUuidFor,
  cbtAssetUuidFor,
} from './helpers';

const erc20MetadataAbi = [
  { type: 'function', name: 'name', inputs: [], outputs: [{ type: 'string' }], stateMutability: 'view' },
  { type: 'function', name: 'symbol', inputs: [], outputs: [{ type: 'string' }], stateMutability: 'view' },
] as const;

async function fetchErc20Metadata(
  config: AppConfig,
  tokenAddress: string,
): Promise<{ name: string; symbol: string }> {
  const client = getPublicClient(config);
  const [name, symbol] = await Promise.all([
    client.readContract({ address: tokenAddress as Address, abi: erc20MetadataAbi, functionName: 'name' }),
    client.readContract({ address: tokenAddress as Address, abi: erc20MetadataAbi, functionName: 'symbol' }),
  ]);
  return { name, symbol };
}

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
   * App configuration (needed for on-chain ERC20 metadata reads).
   */
  readonly config: AppConfig;
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
 * Raw events stored alongside settlement batch for recovery.
 */
export interface RawSettlementEvents {
  readonly bondTokenEvents: readonly ParsedBondToken[];
  readonly lendPositionEvents: readonly ParsedLendPosition[];
  readonly borrowPositionEvents: readonly ParsedBorrowPosition[];
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
export const getPool = (): Pool => {
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
 * Throws if required lookups fail so the caller can handle the error.
 */
const persistBondTokenCreated = async (
  client: PoolClient,
  ev: ParsedBondToken,
  batchId: string,
): Promise<void> => {
  const marketIdUuid = bytes32ToUuid(ev.marketId);
  const loanTokenLower = ev.loanToken.toLowerCase();

  // Look up asset by loanToken address
  const assetRows = await client.query<{ id: string }>(
    `SELECT id FROM assets WHERE LOWER(token_address) = LOWER($1) LIMIT 1`,
    [loanTokenLower],
  );
  const asset = assetRows.rows[0];
  if (!asset) {
    throw new Error(
      `[database] Asset not found for loanToken ${loanTokenLower}, cannot create market ${marketIdUuid} for BondTokenCreated event`,
    );
  }

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
  const bondTokenLower = ev.bondToken.toLowerCase();
  const cbtAssetId = cbtAssetUuidFor(ev.marketId, bondTokenLower);
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
    [cbtAssetId, marketIdUuid, ev.name, ev.symbol, bondTokenLower, batchId],
  );
};

/**
 * Persist LendPositionCreated event: upsert lend_position.
 * Throws if required lookups fail so the caller can handle the error.
 */
const persistLendPositionCreated = async (
  client: PoolClient,
  ev: ParsedLendPosition,
  batchId: string,
  config: AppConfig,
): Promise<void> => {
  const marketIdUuid = bytes32ToUuid(ev.marketId);
  const lenderLower = ev.lender.toLowerCase();
  const positionId = positionUuidFor(ev.marketId, lenderLower);

  const accountRows = await client.query<{ id: string }>(
    `SELECT id FROM accounts WHERE LOWER(user_wallet) = LOWER($1) LIMIT 1`,
    [lenderLower],
  );
  const account = accountRows.rows[0];
  if (!account) {
    throw new Error(
      `[database] Account not found for lender ${lenderLower}, cannot create LendPosition for market ${marketIdUuid}`,
    );
  }

  const marketRows = await client.query<{ asset_id: string }>(
    `SELECT asset_id FROM markets WHERE id = $1 LIMIT 1`,
    [marketIdUuid],
  );
  const market = marketRows.rows[0];
  if (!market) {
    throw new Error(
      `[database] Market not found for id ${marketIdUuid}, cannot create LendPosition for lender ${lenderLower}`,
    );
  }

  const bondTokenLower = ev.bondToken.toLowerCase();
  const cbtAssetRows = await client.query<{ id: string }>(
    `SELECT id FROM cbt_assets WHERE LOWER(token_address) = LOWER($1) AND market_id = $2 LIMIT 1`,
    [bondTokenLower, marketIdUuid],
  );
  let cbtAssetId: string;
  if (cbtAssetRows.rows[0]) {
    cbtAssetId = cbtAssetRows.rows[0].id;
  } else {
    // CBT asset not found — fetch ERC20 metadata on-chain and create it
    // eslint-disable-next-line no-console
    console.log(
      `[database] CBT asset not found for bondToken ${bondTokenLower} in market ${marketIdUuid}, fetching ERC20 metadata on-chain`,
    );
    const metadata = await fetchErc20Metadata(config, bondTokenLower);
    cbtAssetId = cbtAssetUuidFor(ev.marketId, bondTokenLower);
    await client.query(
      `INSERT INTO cbt_assets (id, market_id, name, symbol, token_address, settlement_batch_id, created_at, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())
       ON CONFLICT (id) DO UPDATE SET
         name = EXCLUDED.name,
         symbol = EXCLUDED.symbol,
         token_address = EXCLUDED.token_address,
         settlement_batch_id = EXCLUDED.settlement_batch_id,
         updated_at = NOW()`,
      [cbtAssetId, marketIdUuid, metadata.name, metadata.symbol, bondTokenLower, batchId],
    );
  }

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
 * Throws if required lookups fail so the caller can handle the error.
 */
const persistBorrowPositionCreated = async (
  client: PoolClient,
  ev: ParsedBorrowPosition,
  batchId: string,
): Promise<void> => {
  const marketIdUuid = bytes32ToUuid(ev.marketId);
  const borrowerLower = ev.borrower.toLowerCase();
  const positionId = positionUuidFor(ev.marketId, borrowerLower);

  const accountRows = await client.query<{ id: string }>(
    `SELECT id FROM accounts WHERE LOWER(user_wallet) = LOWER($1) LIMIT 1`,
    [borrowerLower],
  );
  const account = accountRows.rows[0];
  if (!account) {
    throw new Error(
      `[database] Account not found for borrower ${borrowerLower}, cannot create BorrowPosition for market ${marketIdUuid}`,
    );
  }

  const marketRows = await client.query<{ asset_id: string }>(
    `SELECT asset_id FROM markets WHERE id = $1 LIMIT 1`,
    [marketIdUuid],
  );
  const market = marketRows.rows[0];
  if (!market) {
    throw new Error(
      `[database] Market not found for id ${marketIdUuid}, cannot create BorrowPosition for borrower ${borrowerLower}`,
    );
  }

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
  const loanTokenLower = match.loanToken.toLowerCase();
  const lenderWalletLower = match.lenderWallet.toLowerCase();
  const borrowerWalletLower = match.borrowerWallet.toLowerCase();

  // Look up asset by loanToken address
  const assetRows = await client.query<{ id: string }>(
    `SELECT id FROM assets WHERE LOWER(token_address) = LOWER($1) LIMIT 1`,
    [loanTokenLower],
  );
  const asset = assetRows.rows[0];
  if (!asset) {
    // eslint-disable-next-line no-console
    console.warn(`[database] Asset not found for token ${loanTokenLower}, skipping match upsert for ${matchId}`);
    return;
  }

  // Look up lender account
  const lenderRows = await client.query<{ id: string }>(
    `SELECT id FROM accounts WHERE LOWER(user_wallet) = LOWER($1) LIMIT 1`,
    [lenderWalletLower],
  );
  const lenderAccount = lenderRows.rows[0];
  if (!lenderAccount) {
    // eslint-disable-next-line no-console
    console.warn(`[database] Lender account not found for wallet ${lenderWalletLower}, skipping match upsert for ${matchId}`);
    return;
  }

  // Look up borrower account
  const borrowerRows = await client.query<{ id: string }>(
    `SELECT id FROM accounts WHERE LOWER(user_wallet) = LOWER($1) LIMIT 1`,
    [borrowerWalletLower],
  );
  const borrowerAccount = borrowerRows.rows[0];
  if (!borrowerAccount) {
    // eslint-disable-next-line no-console
    console.warn(`[database] Borrower account not found for wallet ${borrowerWalletLower}, skipping match upsert for ${matchId}`);
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
 * Process settlement events (bond tokens, lend positions, borrow positions)
 * for a single batch. This is separated from record persistence so it can
 * be retried independently via the recovery loop.
 */
const processSettlementEvents = async (
  client: PoolClient,
  batchId: string,
  events: RawSettlementEvents,
  config: AppConfig,
): Promise<void> => {
  // Step A: BondTokenCreated -> markets + cbt_assets (must run first)
  for (const ev of events.bondTokenEvents) {
    await persistBondTokenCreated(client, ev, batchId);
  }

  // Step B: LendPositionCreated -> lend_positions
  for (const ev of events.lendPositionEvents) {
    await persistLendPositionCreated(client, ev, batchId, config);
  }

  // Step C: BorrowPositionCreated -> borrow_positions
  for (const ev of events.borrowPositionEvents) {
    await persistBorrowPositionCreated(client, ev, batchId);
  }

  // Mark batch as processed
  await client.query(
    `UPDATE settlement_batches SET events_processed = true, updated_at = NOW() WHERE id = $1`,
    [batchId],
  );
};

/**
 * Persist settlement results to the database using two-phase approach.
 *
 * Phase 1 (Transaction 1): Inserts settlement batches, matches, settlement items,
 * and stores raw events as JSON. This always commits so settlement records are never lost.
 *
 * Phase 2 (Transaction 2): Processes events into positions (markets, cbt_assets,
 * lend_positions, borrow_positions). If this fails, the raw events are stored in
 * Phase 1 and can be retried via the recovery loop.
 *
 * @param options - Options for persisting settlement results.
 * @returns Promise that resolves when persistence is complete.
 * @throws DatabaseError if Phase 1 fails.
 */
export const persistSettlementResults = async (
  options: PersistSettlementResultsOptions,
): Promise<void> => {
  const { results, matchPayloads, config, maxRetries = 3, retryDelayMs = 1000 } = options;

  if (results.length === 0) {
    return;
  }

  // eslint-disable-next-line no-console
  console.log('[database] Persisting settlement results', {
    batchCount: results.length,
    transactionHashes: results.map((result) => result.transactionHash),
  });

  // Phase 1: Persist settlement records + raw events (must succeed)
  const batchIds: { batchId: string; events: RawSettlementEvents }[] = [];

  await executeWithRetry(
    () =>
      withTransaction(async (client) => {
        for (const result of results) {
          const rawEvents: RawSettlementEvents = {
            bondTokenEvents: result.bondTokenEvents,
            lendPositionEvents: result.lendPositionEvents,
            borrowPositionEvents: result.borrowPositionEvents,
          };

          const batchInsert = await client.query<{
            id: string;
          }>(
            `
              INSERT INTO settlement_batches (tx_hash, status, raw_events, events_processed, created_at, updated_at)
              VALUES ($1, $2, $3, false, NOW(), NOW())
              RETURNING id
            `,
            [result.transactionHash, 'COMPLETED', JSON.stringify(rawEvents, (_, v) => typeof v === 'bigint' ? v.toString() : v)],
          );

          if (batchInsert.rows.length === 0) {
            throw new Error('Failed to insert settlement batch');
          }

          const batchId = batchInsert.rows[0].id;

          // Ensure match rows exist before inserting settlement items
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

          batchIds.push({ batchId, events: rawEvents });
        }
      }),
    maxRetries,
    retryDelayMs,
  );

  // eslint-disable-next-line no-console
  console.log('[database] Phase 1 complete: settlement records persisted', {
    batchCount: results.length,
    batchIds: batchIds.map((b) => b.batchId),
  });

  // Phase 2: Process events into positions (can fail — data is safe in raw_events)
  for (const { batchId, events } of batchIds) {
    try {
      await withTransaction(async (client) => {
        await processSettlementEvents(client, batchId, events, config);
      });

      // eslint-disable-next-line no-console
      console.log('[database] Phase 2 complete: events processed for batch', {
        batchId,
        bondTokenEvents: events.bondTokenEvents.length,
        lendPositionEvents: events.lendPositionEvents.length,
        borrowPositionEvents: events.borrowPositionEvents.length,
      });
    } catch (error) {
      // eslint-disable-next-line no-console
      console.error(
        `[database] Phase 2 failed for batch ${batchId}: event processing failed. Raw events are stored for recovery.`,
        {
          batchId,
          error: error instanceof Error ? error.message : String(error),
          bondTokenEvents: events.bondTokenEvents.length,
          lendPositionEvents: events.lendPositionEvents.length,
          borrowPositionEvents: events.borrowPositionEvents.length,
        },
      );
      // Do NOT rethrow — Phase 1 data is committed, recovery loop will retry.
    }
  }
};

/**
 * Find settlement batches where event processing has not completed.
 * Used by the recovery loop to retry failed event processing.
 *
 * @param limit - Maximum number of unprocessed batches to fetch.
 * @returns Array of unprocessed batches with their raw events.
 */
export const findUnprocessedSettlementBatches = async (
  limit = 10,
): Promise<{ id: string; rawEvents: RawSettlementEvents }[]> => {
  const pool = getPool();
  const result = await pool.query<{ id: string; raw_events: RawSettlementEvents }>(
    `SELECT id, raw_events FROM settlement_batches WHERE events_processed = false AND raw_events IS NOT NULL ORDER BY created_at ASC LIMIT $1`,
    [limit],
  );

  return result.rows.map((row) => ({
    id: row.id,
    rawEvents: typeof row.raw_events === 'string'
      ? JSON.parse(row.raw_events)
      : row.raw_events,
  }));
};

/**
 * Retry event processing for a single unprocessed settlement batch.
 *
 * @param batchId - Settlement batch ID.
 * @param rawEvents - Raw events to process.
 * @throws Error if event processing fails.
 */
export const retryEventProcessing = async (
  batchId: string,
  rawEvents: RawSettlementEvents,
  config: AppConfig,
): Promise<void> => {
  await withTransaction(async (client) => {
    await processSettlementEvents(client, batchId, rawEvents, config);
  });
};
