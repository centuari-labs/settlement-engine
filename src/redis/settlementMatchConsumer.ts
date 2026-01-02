import type Redis from 'ioredis';
import { matchSchema, type Match } from '../schemas/match';
import type { AppConfig } from '../config';

type StreamEntry = [string, string[]];
type StreamReadResult = [string, StreamEntry[]][];

export interface MatchWithMeta {
  readonly id: string;
  readonly stream: string;
  readonly payload: Match;
}

export interface SettlementMatchConsumerOptions {
  readonly redis: Redis;
  readonly config: AppConfig;
  /**
   * Handler invoked for each valid match.
   * For now this can log or perform simple side effects; later we will batch.
   */
  readonly onMatch: (match: MatchWithMeta) => Promise<void>;
  /**
   * Optional handler for invalid messages that fail validation.
   */
  readonly onInvalid?: (args: {
    readonly id: string;
    readonly stream: string;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    readonly raw: any;
    readonly error: unknown;
  }) => Promise<void>;
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
    // eslint-disable-next-line no-console
    console.log(`Created consumer group "${group}" on stream "${stream}"`);
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
 * Handles parsing, validation, and invokes appropriate handlers.
 *
 * @param entry - Stream entry to process.
 * @param stream - Stream name.
 * @param consumerGroup - Consumer group name.
 * @param redis - Redis client.
 * @param onMatch - Handler for valid matches.
 * @param onInvalid - Optional handler for invalid entries.
 */
const processEntry = async (
  entry: StreamEntry,
  stream: string,
  consumerGroup: string,
  redis: Redis,
  onMatch: (match: MatchWithMeta) => Promise<void>,
  onInvalid?: SettlementMatchConsumerOptions['onInvalid'],
): Promise<void> => {
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
      // eslint-disable-next-line no-console
      console.error(
        '[settlement-consumer] Invalid match entry',
        JSON.stringify({ stream, id, raw: rawFields }),
      );
    }

    await redis.xack(stream, consumerGroup, id);
    return;
  }

  const matchWithMeta: MatchWithMeta = {
    id: parsed.id,
    stream,
    payload: parsed.value,
  };

  await onMatch(matchWithMeta);
  await redis.xack(stream, consumerGroup, id);
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
 * @param onMatch - Handler for valid matches.
 * @param onInvalid - Optional handler for invalid entries.
 */
const processPendingEntries = async (
  redis: Redis,
  stream: string,
  consumerGroup: string,
  consumerName: string,
  readCount: number,
  onMatch: (match: MatchWithMeta) => Promise<void>,
  onInvalid?: SettlementMatchConsumerOptions['onInvalid'],
): Promise<void> => {
  // First, process pending entries for the current consumer using XREADGROUP with '0'
  let hasMorePending = true;
  while (hasMorePending) {
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
          await processEntry(entry, streamName, consumerGroup, redis, onMatch, onInvalid);
        }
      }

      // If we didn't process any entries, or we got fewer than readCount, we're done
      if (!processedAny || totalEntriesProcessed < readCount) {
        hasMorePending = false;
      }
    } catch (error) {
      // eslint-disable-next-line no-console
      console.error('[settlement-consumer] Error processing pending entries', error);
      hasMorePending = false;
    }
  }

  // Second, claim and process stale entries from other consumers
  // Continue claiming in batches until no more stale entries are available
  let hasMoreStaleEntries = true;
  while (hasMoreStaleEntries) {
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

      // Claim the stale entries with 0ms min idle time (claim all pending entries)
      const claimedEntries = (await (redis.xclaim as (
        ...args: (string | number)[]
      ) => Promise<StreamEntry[]>)(
        stream,
        consumerGroup,
        consumerName,
        0, // 0ms = claim any pending entry regardless of idle time
        ...entriesToClaim,
      )) as StreamEntry[];

      // Process claimed entries
      for (const entry of claimedEntries) {
        await processEntry(entry, stream, consumerGroup, redis, onMatch, onInvalid);
      }

      // If we got fewer entries than readCount, we've processed all stale entries
      if (entriesToClaim.length < readCount) {
        hasMoreStaleEntries = false;
      }
    } catch (error) {
      // eslint-disable-next-line no-console
      console.error('[settlement-consumer] Error claiming stale entries', error);
      hasMoreStaleEntries = false;
    }
  }
};

/**
 * Start a long-running loop that consumes settlement matches from Redis.
 * This uses XREADGROUP with BLOCK, and should be stopped via the returned
 * `stop` function.
 */
export const startSettlementMatchConsumer = (
  options: SettlementMatchConsumerOptions,
): (() => void) => {
  const { redis, config, onMatch, onInvalid } = options;
  const {
    consumerGroup,
    consumerName,
    settlementMatchesStream,
    readBlockMs,
    readCount,
  } = config;

  let isRunning = true;

  // Process pending entries before starting the main loop
  void (async (): Promise<void> => {
    try {
      await processPendingEntries(
        redis,
        settlementMatchesStream,
        consumerGroup,
        consumerName,
        readCount,
        onMatch,
        onInvalid,
      );
    } catch (error) {
      // eslint-disable-next-line no-console
      console.error('[settlement-consumer] Error during initial pending entry processing', error);
      // Continue to start the main loop even if pending processing fails
    }
  })();

  const loop = async (): Promise<void> => {
    // eslint-disable-next-line no-constant-condition
    while (isRunning) {
      try {
        // Use type assertion to work around ioredis v5 type overload resolution
        const result = (await (redis.xreadgroup as (
          ...args: (string | number)[]
        ) => Promise<StreamReadResult | null>)(
          'GROUP',
          consumerGroup,
          consumerName,
          'BLOCK',
          readBlockMs,
          'COUNT',
          readCount,
          'STREAMS',
          settlementMatchesStream,
          '>',
        )) as StreamReadResult | null;

        if (!result) {
          continue;
        }

        for (const [stream, entries] of result) {
          for (const entry of entries) {
            await processEntry(entry, stream, consumerGroup, redis, onMatch, onInvalid);
          }
        }
      } catch (error) {
        // eslint-disable-next-line no-console
        console.error('[settlement-consumer] Error in consumer loop', error);
        // Back off briefly on error to avoid tight error loops.
        // Also check if we should continue - the connection might be permanently closed
        if (!isRunning) {
          break;
        }
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }
    }
  };

  void loop();

  return () => {
    isRunning = false;
  };
};


