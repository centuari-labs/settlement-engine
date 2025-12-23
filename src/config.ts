import { z } from 'zod';
import { REDIS_CONSUMER_GROUPS, REDIS_STREAMS } from './schemas/match';

/**
 * Zod schema for environment configuration.
 */
const configSchema = z.object({
  REDIS_URL: z.string().default('redis://localhost:6379'),
  REDIS_STREAM_SETTLEMENT_MATCHES: z
    .string()
    .default(REDIS_STREAMS.SETTLEMENT_MATCHES),
  REDIS_CONSUMER_GROUP: z
    .string()
    .default(REDIS_CONSUMER_GROUPS.SETTLEMENT_ENGINE),
  REDIS_CONSUMER_NAME: z.string().default('settlement-engine-1'),
  REDIS_READ_BLOCK_MS: z
    .string()
    .transform((value) => Number(value || 5000))
    .pipe(z.number().int().positive())
    .default('5000'),
  REDIS_READ_COUNT: z
    .string()
    .transform((value) => Number(value || 10))
    .pipe(z.number().int().positive())
    .default('10'),
  REDIS_STREAM_MAXLEN: z
    .string()
    .transform((value) => Number(value || 10000))
    .pipe(z.number().int().positive())
    .default('10000'),
});

export type AppConfig = {
  readonly redisUrl: string;
  readonly settlementMatchesStream: string;
  readonly consumerGroup: string;
  readonly consumerName: string;
  readonly readBlockMs: number;
  readonly readCount: number;
  readonly streamMaxLen: number;
};

/**
 * Load and validate application configuration from environment variables.
 */
export const loadConfig = (): AppConfig => {
  const parsed = configSchema.parse(process.env);

  return {
    redisUrl: parsed.REDIS_URL,
    settlementMatchesStream: parsed.REDIS_STREAM_SETTLEMENT_MATCHES,
    consumerGroup: parsed.REDIS_CONSUMER_GROUP,
    consumerName: parsed.REDIS_CONSUMER_NAME,
    readBlockMs: parsed.REDIS_READ_BLOCK_MS,
    readCount: parsed.REDIS_READ_COUNT,
    streamMaxLen: parsed.REDIS_STREAM_MAXLEN,
  };
};


