import type { Pool, PoolClient } from 'pg';
import { Pool as PgPool } from 'pg';
import { z } from 'zod';
import type { SettlementResult } from './smartContract';

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
 * Persist settlement results to the database.
 *
 * This is a placeholder implementation that will be replaced with actual
 * database integration (e.g., PostgreSQL, MongoDB, etc.).
 *
 * @param options - Options for persisting settlement results.
 * @returns Promise that resolves when persistence is complete.
 * @throws DatabaseError if the persistence fails.
 */
export const persistSettlementResults = async (
  options: PersistSettlementResultsOptions,
): Promise<void> => {
  const { results, maxRetries = 3, retryDelayMs = 1000 } = options;

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

