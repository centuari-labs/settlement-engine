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
 * Start a long-running loop that consumes settlement matches from Redis.
 * This uses XREADGROUP with BLOCK, and should be stopped via the returned
 * `stop` function.
 */
export const startSettlementMatchConsumer = (
  options: SettlementMatchConsumerOptions,
): (() => void) => {
  const { redis, config, onMatch, onInvalid } = options;
  const { consumerGroup, consumerName, settlementMatchesStream, readBlockMs, readCount } =
    config;

  let isRunning = true;

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
              continue;
            }

            const matchWithMeta: MatchWithMeta = {
              id: parsed.id,
              stream,
              payload: parsed.value,
            };

            await onMatch(matchWithMeta);
            await redis.xack(stream, consumerGroup, id);
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


