import type Redis from 'ioredis';
import { matchSchema, type Match } from '../schemas/match';
import { logger } from '../logger';

type StreamEntry = [string, string[]];
type StreamReadResult = [string, StreamEntry[]][];

export interface MatchWithMeta {
  readonly id: string;
  readonly stream: string;
  readonly payload: Match;
}

/**
 * Options for reading matches from Redis stream.
 */
export interface ReadMatchesOptions {
  /**
   * Redis client.
   */
  readonly redis: Redis;
  /**
   * Stream name.
   */
  readonly stream: string;
  /**
   * Consumer group name.
   */
  readonly consumerGroup: string;
  /**
   * Consumer name.
   */
  readonly consumerName: string;
  /**
   * Maximum number of entries to read per call.
   */
  readonly readCount: number;
  /**
   * Optional handler for invalid entries.
   */
  readonly onInvalid?: (args: {
    readonly id: string;
    readonly stream: string;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    readonly raw: any;
    readonly error: unknown;
  }) => Promise<void>;
  /**
   * Maximum total entries to load (caps memory usage). Default: readCount * 3.
   */
  readonly maxEntries?: number;
  /**
   * Minimum idle time in ms for XCLAIM (only claim entries idle this long). Default: 60000.
   */
  readonly xclaimMinIdleMs?: number;
}

/**
 * Ensure the Redis consumer group exists for the settlement matches stream.
 */
export const ensureConsumerGroup = async (
  redis: Redis,
  stream: string,
  group: string,
): Promise<void> => {
  try {
    await redis.xgroup('CREATE', stream, group, '0', 'MKSTREAM');
    logger.info({ component: 'settlement-consumer', group, stream }, 'Created consumer group');
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (message.includes('BUSYGROUP')) {
      // Group already exists; this is fine.
      return;
    }
    throw error;
  }
};

const fieldsArrayToObject = (fields: string[]): Record<string, string> => {
  const result: Record<string, string> = {};
  for (let index = 0; index < fields.length; index += 2) {
    const key = fields[index] ?? '';
    const value = fields[index + 1] ?? '';
    if (key) {
      result[key] = value;
    }
  }
  return result;
};

/**
 * Convert a fields object with string values to proper types for Match schema.
 * This handles the case where fields are stored as individual stream fields (all strings).
 *
 * @param fields - Object with string values from Redis stream.
 * @returns Object with converted types.
 */
const convertFieldsToMatch = (fields: Record<string, string>): unknown => {
  const converted: Record<string, unknown> = { ...fields };

  // Convert numeric fields
  if (converted.rate !== undefined) {
    converted.rate = Number(converted.rate);
  }
  if (converted.maturity !== undefined) {
    converted.maturity = Number(converted.maturity);
  }
  if (converted.timestamp !== undefined) {
    converted.timestamp = Number(converted.timestamp);
  }

  // Convert boolean field
  if (converted.borrowerIsTaker !== undefined) {
    const value = String(converted.borrowerIsTaker).toLowerCase();
    converted.borrowerIsTaker = value === 'true' || value === '1';
  }

  return converted;
};

/**
 * Parse a Redis stream entry into a typed `Match` using `matchSchema`.
 *
 * This assumes the matching engine publishes entries where either:
 * - all fields of the match are stored directly as stream fields; or
 * - a single field `data` contains a JSON string of the full payload.
 */
const parseMatchEntry = (
  entry: StreamEntry,
): { id: string; value: Match } | null => {
  const [id, rawFields] = entry;
  const fieldsObject = fieldsArrayToObject(rawFields);

  let candidate: unknown = fieldsObject;

  if (fieldsObject.data) {
    // If there's a 'data' field, try to parse it as JSON
    try {
      candidate = JSON.parse(fieldsObject.data);
    } catch {
      // Fall back to using the field object as-is.
      candidate = fieldsObject;
    }
  } else {
    // If no 'data' field, convert string fields to proper types
    candidate = convertFieldsToMatch(fieldsObject);
  }

  const parsed = matchSchema.safeParse(candidate);
  if (!parsed.success) {
    return null;
  }

  return { id, value: parsed.data };
};

/**
 * Process a single entry from a stream.
 * Handles parsing, validation, and returns the match if valid.
 *
 * @param entry - Stream entry to process.
 * @param stream - Stream name.
 * @param consumerGroup - Consumer group name.
 * @param redis - Redis client.
 * @param onInvalid - Optional handler for invalid entries.
 * @returns MatchWithMeta if valid, null if invalid (and ACKed).
 */
const processEntry = async (
  entry: StreamEntry,
  stream: string,
  consumerGroup: string,
  redis: Redis,
  onInvalid?: ReadMatchesOptions['onInvalid'],
): Promise<MatchWithMeta | null> => {
  const parsed = parseMatchEntry(entry);
  const id = entry[0];
  const rawFields = fieldsArrayToObject(entry[1]);

  if (!parsed) {
    if (onInvalid) {
      await onInvalid({
        id,
        stream,
        raw: rawFields,
        error: new Error('Validation failed for match entry'),
      });
    } else {
      logger.error({ component: 'settlement-consumer', stream, id, raw: rawFields }, 'Invalid match entry');
    }

    // ACK invalid entries immediately to avoid blocking
    await redis.xack(stream, consumerGroup, id);
    return null;
  }

  const matchWithMeta: MatchWithMeta = {
    id: parsed.id,
    stream,
    payload: parsed.value,
  };

  // Return match (ACK will happen after successful batch processing)
  return matchWithMeta;
};

/**
 * Read matches from Redis stream using non-blocking XREADGROUP.
 * Entries are assigned to the consumer but not ACKed (ACK happens after successful batch processing).
 *
 * @param options - Options for reading matches.
 * @returns Array of valid matches (invalid entries are ACKed immediately).
 */
export const readMatches = async (
  options: ReadMatchesOptions,
): Promise<MatchWithMeta[]> => {
  const { redis, stream, consumerGroup, consumerName, readCount, onInvalid } =
    options;

  try {
    // Use XREADGROUP without BLOCK for truly non-blocking reads.
    // Polling is handled by BatchProcessor's setInterval.
    const result = (await (redis.xreadgroup as (
      ...args: (string | number)[]
    ) => Promise<StreamReadResult | null>)(
      'GROUP',
      consumerGroup,
      consumerName,
      'COUNT',
      readCount,
      'STREAMS',
      stream,
      '>',
    )) as StreamReadResult | null;

    if (!result || result.length === 0) {
      return [];
    }

    const matches: MatchWithMeta[] = [];

    for (const [streamName, entries] of result) {
      for (const entry of entries) {
        const parsed = parseMatchEntry(entry);
        const id = entry[0];
        const rawFields = fieldsArrayToObject(entry[1]);

        if (!parsed) {
          if (onInvalid) {
            await onInvalid({
              id,
              stream: streamName,
              raw: rawFields,
              error: new Error('Validation failed for match entry'),
            });
          } else {
            logger.error({ component: 'settlement-consumer', stream: streamName, id, raw: rawFields }, 'Invalid match entry');
          }

          // ACK invalid entries immediately to avoid blocking
          await redis.xack(streamName, consumerGroup, id);
          continue;
        }

        const matchWithMeta: MatchWithMeta = {
          id: parsed.id,
          stream: streamName,
          payload: parsed.value,
        };

        matches.push(matchWithMeta);
      }
    }

    return matches;
  } catch (error) {
    logger.error({ component: 'settlement-consumer', err: error }, 'Error reading matches');
    // Return empty array on error to allow graceful degradation
    return [];
  }
};

/**
 * Process pending entries for the current consumer and claim stale entries from other consumers.
 * This should be called before starting to process new entries.
 *
 * @param redis - Redis client.
 * @param stream - Stream name.
 * @param consumerGroup - Consumer group name.
 * @param consumerName - Current consumer name.
 * @param readCount - Maximum number of entries to read per batch.
 * @param onInvalid - Optional handler for invalid entries.
 * @param maxEntries - Maximum total entries to load (caps memory). Default: readCount * 3.
 * @param xclaimMinIdleMs - Minimum idle time in ms for XCLAIM. Default: 60000.
 * @returns Array of valid matches from pending entries.
 */
const processPendingEntries = async (
  redis: Redis,
  stream: string,
  consumerGroup: string,
  consumerName: string,
  readCount: number,
  onInvalid?: ReadMatchesOptions['onInvalid'],
  maxEntries?: number,
  xclaimMinIdleMs: number = 60000,
): Promise<MatchWithMeta[]> => {
  const matches: MatchWithMeta[] = [];
  const cap = maxEntries ?? readCount * 3;
  // First, process pending entries for the current consumer using XREADGROUP with '0'
  let hasMorePending = true;
  while (hasMorePending) {
    if (matches.length >= cap) break;
    try {
      // Read pending entries for this consumer (using '0' instead of '>')
      const result = (await (redis.xreadgroup as (
        ...args: (string | number)[]
      ) => Promise<StreamReadResult | null>)(
        'GROUP',
        consumerGroup,
        consumerName,
        'COUNT',
        readCount,
        'STREAMS',
        stream,
        '0',
      )) as StreamReadResult | null;

      if (!result || result.length === 0) {
        hasMorePending = false;
        break;
      }

      let processedAny = false;
      let totalEntriesProcessed = 0;

      for (const [streamName, entries] of result) {
        if (entries.length === 0) {
          continue;
        }
        processedAny = true;
        totalEntriesProcessed += entries.length;

        for (const entry of entries) {
          const match = await processEntry(
            entry,
            streamName,
            consumerGroup,
            redis,
            onInvalid,
          );
          if (match) {
            matches.push(match);
          }
        }
      }

      // If we didn't process any entries, or we got fewer than readCount, we're done
      if (!processedAny || totalEntriesProcessed < readCount) {
        hasMorePending = false;
      }
    } catch (error) {
      logger.error({ component: 'settlement-consumer', err: error }, 'Error processing pending entries');
      hasMorePending = false;
    }
  }

  // Second, claim and process stale entries from other consumers
  // Continue claiming in batches until no more stale entries are available
  let hasMoreStaleEntries = true;
  while (hasMoreStaleEntries && matches.length < cap) {
    try {
      // Get pending entry summary
      // XPENDING returns: [total, start, end, consumers]
      const pendingInfo = (await (redis.xpending as unknown as (
        ...args: (string | number)[]
      ) => Promise<[number, string, string, Array<[string, string]>] | null>)(
        stream,
        consumerGroup,
      )) as [number, string, string, Array<[string, string]>] | null;

      if (!pendingInfo || !Array.isArray(pendingInfo) || pendingInfo.length < 1) {
        hasMoreStaleEntries = false;
        break;
      }

      const totalPending = pendingInfo[0] as number;
      if (totalPending === 0) {
        hasMoreStaleEntries = false;
        break;
      }

      // Get detailed pending entries (up to readCount at a time)
      // XPENDING stream group - + count returns: [[id, consumer, idle, deliveries], ...]
      const pendingDetails = (await (redis.xpending as unknown as (
        ...args: (string | number)[]
      ) => Promise<Array<[string, string, number, number]>>)(
        stream,
        consumerGroup,
        '-',
        '+',
        readCount,
      )) as Array<[string, string, number, number]>;

      if (!pendingDetails || pendingDetails.length === 0) {
        hasMoreStaleEntries = false;
        break;
      }

      // Claim all pending entries (since we only claim at startup, they're likely abandoned)
      // Use 0ms min idle time to claim any pending entry regardless of how long it's been idle
      const entriesToClaim = pendingDetails.map((entry) => entry[0]);

      if (entriesToClaim.length === 0) {
        hasMoreStaleEntries = false;
        break;
      }

      // Claim only entries idle long enough (abandoned by other consumers)
      const claimedEntries = (await (redis.xclaim as (
        ...args: (string | number)[]
      ) => Promise<StreamEntry[]>)(
        stream,
        consumerGroup,
        consumerName,
        xclaimMinIdleMs,
        ...entriesToClaim,
      )) as StreamEntry[];

      // Process claimed entries
      for (const entry of claimedEntries) {
        const match = await processEntry(
          entry,
          stream,
          consumerGroup,
          redis,
          onInvalid,
        );
        if (match) {
          matches.push(match);
        }
      }

      // If we got fewer entries than readCount, we've processed all stale entries
      if (entriesToClaim.length < readCount) {
        hasMoreStaleEntries = false;
      }
    } catch (error) {
      logger.error({ component: 'settlement-consumer', err: error }, 'Error claiming stale entries');
      hasMoreStaleEntries = false;
    }
  }

  return matches;
};

/**
 * Process pending entries on startup and return them as matches.
 * This should be called before starting batch processing to handle any
 * pending entries from previous runs.
 *
 * @param options - Options for processing pending entries.
 * @returns Array of valid matches from pending entries.
 */
export const processPendingEntriesOnStartup = async (
  options: ReadMatchesOptions,
): Promise<MatchWithMeta[]> => {
  const {
    redis,
    stream,
    consumerGroup,
    consumerName,
    readCount,
    onInvalid,
    maxEntries,
    xclaimMinIdleMs,
  } = options;

  return processPendingEntries(
    redis,
    stream,
    consumerGroup,
    consumerName,
    readCount,
    onInvalid,
    maxEntries,
    xclaimMinIdleMs,
  );
};


