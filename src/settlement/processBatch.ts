import type Redis from 'ioredis';
import type { MatchWithMeta } from '../redis/settlementMatchConsumer';
import type { AppConfig } from '../config';
import { logger } from '../logger';
import {
  settleBatch,
  filterAlreadySettledMatches,
  getPublicClient,
  type SettlementError,
} from './smartContract';
import { applySettlementResult, getPool, type DatabaseError } from './database';
import { loadConfig } from '../config';
import type { NonceManager } from './nonceManager';

/**
 * Context for batch settlement processing that provides access to Redis and
 * stream configuration needed for deletion and trimming.
 */
export interface SettlementBatchContext {
  /**
   * Redis client used for stream operations.
   */
  readonly redis: Redis;
  /**
   * Name of the Redis stream that carries settlement matches.
   */
  readonly stream: string;
  /**
   * Consumer group name for ACKing entries.
   */
  readonly consumerGroup: string;
  /**
   * Maximum number of entries to keep in the stream after trimming.
   */
  readonly streamMaxLen: number;
  /**
   * Nonce manager for explicit nonce sequencing across instances.
   */
  readonly nonceManager?: NonceManager;
}

/**
 * Error that occurred during batch processing.
 */
export class BatchProcessingError extends Error {
  /**
   * Whether the error is retryable.
   */
  readonly retryable: boolean;
  /**
   * Original error that caused the failure.
   */
  readonly originalError: unknown;

  constructor(message: string, retryable: boolean, originalError: unknown) {
    super(message);
    this.name = 'BatchProcessingError';
    this.retryable = retryable;
    this.originalError = originalError;
  }
}

/**
 * ACK and delete stream entries from Redis.
 */
const ackAndDeleteEntries = async (
  context: SettlementBatchContext,
  matches: readonly MatchWithMeta[],
): Promise<void> => {
  const idsByStream = new Map<string, string[]>();
  for (const match of matches) {
    const ids = idsByStream.get(match.stream) ?? [];
    ids.push(match.id);
    idsByStream.set(match.stream, ids);
  }

  for (const [stream, ids] of idsByStream.entries()) {
    for (const id of ids) {
      await context.redis.xack(stream, context.consumerGroup, id);
    }
  }

  const DELETE_BATCH_SIZE = 100;
  for (const [stream, ids] of idsByStream.entries()) {
    for (let i = 0; i < ids.length; i += DELETE_BATCH_SIZE) {
      const batch = ids.slice(i, i + DELETE_BATCH_SIZE);
      await context.redis.xdel(stream, ...batch);
    }
  }
};

/**
 * Process a batch of settlement matches.
 *
 * This function:
 * 1. Filters out already-settled matches (ACKs them from Redis immediately)
 * 2. Calls the smart contract to settle the remaining batch
 * 3. Persists settlement results to the database
 * 4. ACKs and deletes stream entries after successful settlement
 *
 * If any step fails, the entries remain in pending state for retry.
 *
 * @param matches - Array of matches to process in the batch.
 * @param context - Context providing Redis client and stream configuration.
 * @param config - Optional application configuration. If not provided, will be loaded from environment.
 * @throws BatchProcessingError if settlement fails (entries remain pending).
 */
export const processSettlementBatch = async (
  matches: readonly MatchWithMeta[],
  context: SettlementBatchContext,
  config?: AppConfig,
): Promise<void> => {
  if (matches.length === 0) {
    return;
  }

  //@todo : what if only 1 matches that is not valid, that will make the entire transaction fail, we need to handle this case.
  //@todo : if calling the smart contract failed, we need to handle the error and retry the transaction.

  logger.info(
    {
      component: 'process-settlement-batch',
      matchCount: matches.length,
      matches: matches.map((match) => ({
        id: match.id,
        matchId: match.payload.matchId,
        lendOrderId: match.payload.lendOrderId,
        borrowOrderId: match.payload.borrowOrderId,
      })),
    },
    'Processing batch',
  );

  // Step 0: Filter out already-settled matches (prevents infinite retry loop)
  const { unsettled, alreadySettled } = await filterAlreadySettledMatches(
    matches,
    config,
  );

  if (alreadySettled.length > 0) {
    await ackAndDeleteEntries(context, alreadySettled);
    logger.info(
      { component: 'process-settlement-batch', count: alreadySettled.length },
      'ACKed already-settled matches',
    );
  }

  if (unsettled.length === 0) {
    // All matches were already settled; we're done
    return;
  }

  let settlementResult;
  try {
    // Step 1: Call smart contract to settle the batch
    const startTime = Date.now();
    settlementResult = await settleBatch({
      matches: unsettled.map((m) => m.payload),
      config,
      nonceManager: context.nonceManager,
    });
    const duration = Date.now() - startTime;

    logger.info(
      {
        component: 'process-settlement-batch',
        transactionHash: settlementResult.transactionHash,
        blockNumber: settlementResult.blockNumber,
        gasUsed: settlementResult.gasUsed,
        duration,
        matchCount: unsettled.length,
      },
      'Smart contract settlement successful',
    );
  } catch (error) {
    const settlementError = error as SettlementError;
    const isRetryable =
      settlementError.retryable !== undefined
        ? settlementError.retryable
        : true;

    logger.error(
      {
        component: 'process-settlement-batch',
        err: settlementError.message,
        code: settlementError.code,
        retryable: isRetryable,
        matchCount: unsettled.length,
      },
      'Smart contract settlement failed',
    );

    throw new BatchProcessingError(
      `Smart contract settlement failed: ${settlementError.message}`,
      isRetryable,
      error,
    );
  }

  // Step 2: Eager-write the parsed events to indexer-v3's schema. Each event
  // is committed in its own pg tx with applied_by_* stamps. If this fails
  // mid-batch the on-chain tx is already final, so we don't ACK Redis —
  // retry will no-op the already-stamped rows via their stamps, and the
  // indexer-v3 tail is a secondary safety net for any gaps we leave behind.
  try {
    const startTime = Date.now();
    // The cached viem public client is typed narrowly by createPublicClient;
    // applyOnChainEffect declares the general PublicClient interface. Cast
    // is safe because we always pass a pre-fetched receipt — client is never
    // dereferenced by the helper in this path.
    const publicClient = getPublicClient(
      config ?? loadConfig(),
    ) as unknown as import('viem').PublicClient;
    await applySettlementResult(getPool(), publicClient, settlementResult);
    const duration = Date.now() - startTime;

    logger.info(
      { component: 'process-settlement-batch', duration, matchCount: unsettled.length },
      'Applied settlement result to indexer-v3 schema',
    );
  } catch (error) {
    const dbError = error as DatabaseError;
    const isRetryable =
      dbError.retryable !== undefined ? dbError.retryable : true;

    logger.error(
      {
        component: 'process-settlement-batch',
        err: dbError.message ?? (error as Error).message,
        code: dbError.code,
        retryable: isRetryable,
        matchCount: unsettled.length,
      },
      'Settlement apply to indexer-v3 schema failed',
    );

    throw new BatchProcessingError(
      `Settlement apply failed: ${dbError.message ?? (error as Error).message}`,
      isRetryable,
      error,
    );
  }

  // Step 3: ACK and delete entries from Redis.
  // Phase 1 committed settlement records + raw events, so even if Phase 2
  // (event processing) failed, the data is safe and the recovery loop will retry.
  await ackAndDeleteEntries(context, unsettled);

  // Apply a length-based trim on the main settlement stream to enforce a
  // bounded retention window and avoid unbounded growth.
  await context.redis.xtrim(
    context.stream,
    'MAXLEN',
    '~',
    context.streamMaxLen,
  );

  logger.info(
    {
      component: 'process-settlement-batch',
      matchCount: unsettled.length,
      transactionHash: settlementResult.transactionHash,
    },
    'Batch processing complete',
  );
};


