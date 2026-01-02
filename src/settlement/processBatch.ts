import type Redis from 'ioredis';
import type { MatchWithMeta } from '../redis/settlementMatchConsumer';

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
   * Maximum number of entries to keep in the stream after trimming.
   */
  readonly streamMaxLen: number;
}

/**
 * Placeholder for future batch settlement processing.
 *
 * This is where we will:
 * - Group matches into batches.
 * - Call the on-chain Settlement smart contract.
 * - Persist settlement results to the database.
 *
 * For now this simply logs the matches for observability.
 */
export const processSettlementBatch = async (
  matches: readonly MatchWithMeta[],
  context: SettlementBatchContext,
): Promise<void> => {
  // eslint-disable-next-line no-console
  console.log(
    `[process-settlement-batch] Received batch of ${matches.length} matches`,
    matches.map((match) => ({
      id: match.id,
      matchId: match.payload.matchId,
      lendOrderId: match.payload.lendOrderId,
      borrowOrderId: match.payload.borrowOrderId,
    })),
  );

  // In the future, real settlement logic (on-chain calls, persistence, etc.)
  // will run here. Only after successful settlement do we delete entries and
  // trim the stream.

  if (matches.length === 0) {
    return;
  }

  // Group entry IDs by stream so we can delete them efficiently, in case
  // multiple streams are ever supported.
  const idsByStream = new Map<string, string[]>();
  for (const match of matches) {
    const ids = idsByStream.get(match.stream) ?? [];
    ids.push(match.id);
    idsByStream.set(match.stream, ids);
  }

  // Delete successfully processed entries from their respective streams.
  // Batch deletions to avoid potential Redis argument limits (typically 1000+ args)
  // and to handle large batches more efficiently.
  const BATCH_SIZE = 100;
  for (const [stream, ids] of idsByStream.entries()) {
    // Process deletions in batches to avoid hitting Redis argument limits
    for (let i = 0; i < ids.length; i += BATCH_SIZE) {
      const batch = ids.slice(i, i + BATCH_SIZE);
      await context.redis.xdel(stream, ...batch);
    }
  }

  // Apply a length-based trim on the main settlement stream to enforce a
  // bounded retention window and avoid unbounded growth.
  await context.redis.xtrim(
    context.stream,
    'MAXLEN',
    '~',
    context.streamMaxLen,
  );
};


