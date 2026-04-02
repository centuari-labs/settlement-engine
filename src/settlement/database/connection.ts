import type { Pool, PoolClient } from 'pg';
import { logger } from '../../logger';
import { Pool as PgPool } from 'pg';
import { z } from 'zod';
import type {
  SettlementResult,
  ParsedBondToken,
  ParsedLendPosition,
  ParsedBorrowPosition,
} from '../smartContract';
import type { Match } from '../../schemas/match';
import type { AppConfig } from '../../config';
import { calculateBackoffDelay } from '../helpers';

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
export const withTransaction = async <T>(fn: (client: PoolClient) => Promise<T>): Promise<T> => {
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
export const mapPostgresErrorToDatabaseError = (error: unknown): DatabaseError => {
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
export const executeWithRetry = async <T>(
  operation: () => Promise<T>,
  maxRetries: number,
  retryDelayMs: number,
): Promise<T> => {
  let attempt = 0;
  const maxDelayMs = retryDelayMs * Math.pow(2, maxRetries);

  // eslint-disable-next-line no-constant-condition
  while (true) {
    try {
      return await operation();
    } catch (error) {
      const dbError = mapPostgresErrorToDatabaseError(error);

      if (!dbError.retryable || attempt >= maxRetries) {
        logger.error(
          {
            component: 'database',
            message: dbError.message,
            code: dbError.code,
            retryable: dbError.retryable,
            attempt,
            maxRetries,
          },
          'Persistence failed (non-retryable or max retries reached)',
        );
        throw dbError;
      }

      logger.warn(
        {
          component: 'database',
          message: dbError.message,
          code: dbError.code,
          attempt: attempt + 1,
          total: maxRetries + 1,
        },
        'Retryable error during persistence',
      );

      const delay = calculateBackoffDelay(attempt + 1, retryDelayMs, maxDelayMs);
      await new Promise((resolve) => setTimeout(resolve, delay));
      attempt += 1;
    }
  }
};
