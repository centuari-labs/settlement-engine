import { z } from 'zod';
import {
  REDIS_CONSUMER_GROUPS,
  REDIS_STREAMS,
  ethereumAddressSchema,
} from './schemas/match';

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
  SETTLEMENT_BATCH_SIZE: z
    .string()
    .transform((value) => Number(value || 10))
    .pipe(z.number().int().positive())
    .default('10'),
  SETTLEMENT_BATCH_INTERVAL_MS: z
    .string()
    .transform((value) => Number(value || 5000))
    .pipe(z.number().int().positive())
    .default('5000'),
  SETTLEMENT_POLL_INTERVAL_MS: z
    .string()
    .transform((value) => Number(value || 200))
    .pipe(z.number().int().positive())
    .default('200'),
  SETTLEMENT_PENDING_RECLAIM_INTERVAL_MS: z
    .string()
    .transform((value) => Number(value || 60000))
    .pipe(z.number().int().positive())
    .default('60000'),
  REDIS_XCLAIM_MIN_IDLE_MS: z
    .string()
    .transform((value) => Number(value || 60000))
    .pipe(z.number().int().positive())
    .default('60000'),
  SETTLEMENT_FAILURE_BACKOFF_BASE_MS: z
    .string()
    .transform((value) => Number(value || 1000))
    .pipe(z.number().int().positive())
    .default('1000'),
  SETTLEMENT_FAILURE_BACKOFF_MAX_MS: z
    .string()
    .transform((value) => Number(value || 60000))
    .pipe(z.number().int().positive())
    .default('60000'),
  SETTLEMENT_CONTRACT_ADDRESS: ethereumAddressSchema,
  ETHEREUM_RPC_URL: z.string().url('RPC URL must be a valid URL'),
  TURNKEY_API_PUBLIC_KEY: z.string().min(1, 'TURNKEY_API_PUBLIC_KEY is required'),
  TURNKEY_API_PRIVATE_KEY: z.string().min(1, 'TURNKEY_API_PRIVATE_KEY is required'),
  TURNKEY_ORGANIZATION_ID: z.string().min(1, 'TURNKEY_ORGANIZATION_ID is required'),
  TURNKEY_WALLET_ACCOUNT_ADDRESS: ethereumAddressSchema,
  ETHEREUM_CHAIN_ID: z
    .string()
    .transform((value) => Number(value || 1))
    .pipe(z.number().int().positive())
    .default('1'),
  NONCE_LOCK_TTL_MS: z
    .string()
    .transform((value) => Number(value || 30000))
    .pipe(z.number().int().positive())
    .default('30000'),
  TX_CONFIRMATION_TIMEOUT_MS: z
    .string()
    .transform((value) => Number(value || 120000))
    .pipe(z.number().int().positive())
    .default('120000'),
  NONCE_LOCK_RETRY_DELAY_MS: z
    .string()
    .transform((value) => Number(value || 500))
    .pipe(z.number().int().positive())
    .default('500'),
});

export type AppConfig = {
  readonly redisUrl: string;
  readonly settlementMatchesStream: string;
  readonly consumerGroup: string;
  readonly consumerName: string;
  readonly readBlockMs: number;
  readonly readCount: number;
  readonly streamMaxLen: number;
  readonly batchSize: number;
  readonly batchIntervalMs: number;
  readonly pollIntervalMs: number;
  readonly pendingReclaimIntervalMs: number;
  readonly xclaimMinIdleMs: number;
  readonly failureBackoffBaseMs: number;
  readonly failureBackoffMaxMs: number;
  readonly settlementContractAddress: string;
  readonly ethereumRpcUrl: string;
  readonly turnkeyApiPublicKey: string;
  readonly turnkeyApiPrivateKey: string;
  readonly turnkeyOrganizationId: string;
  readonly walletAddress: string;
  readonly ethereumChainId: number;
  readonly nonceLockTtlMs: number;
  readonly txConfirmationTimeoutMs: number;
  readonly nonceLockRetryDelayMs: number;
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
    batchSize: parsed.SETTLEMENT_BATCH_SIZE,
    batchIntervalMs: parsed.SETTLEMENT_BATCH_INTERVAL_MS,
    pollIntervalMs: parsed.SETTLEMENT_POLL_INTERVAL_MS,
    pendingReclaimIntervalMs: parsed.SETTLEMENT_PENDING_RECLAIM_INTERVAL_MS,
    xclaimMinIdleMs: parsed.REDIS_XCLAIM_MIN_IDLE_MS,
    failureBackoffBaseMs: parsed.SETTLEMENT_FAILURE_BACKOFF_BASE_MS,
    failureBackoffMaxMs: parsed.SETTLEMENT_FAILURE_BACKOFF_MAX_MS,
    settlementContractAddress: parsed.SETTLEMENT_CONTRACT_ADDRESS,
    ethereumRpcUrl: parsed.ETHEREUM_RPC_URL,
    turnkeyApiPublicKey: parsed.TURNKEY_API_PUBLIC_KEY,
    turnkeyApiPrivateKey: parsed.TURNKEY_API_PRIVATE_KEY,
    turnkeyOrganizationId: parsed.TURNKEY_ORGANIZATION_ID,
    walletAddress: parsed.TURNKEY_WALLET_ACCOUNT_ADDRESS,
    ethereumChainId: parsed.ETHEREUM_CHAIN_ID,
    nonceLockTtlMs: parsed.NONCE_LOCK_TTL_MS,
    txConfirmationTimeoutMs: parsed.TX_CONFIRMATION_TIMEOUT_MS,
    nonceLockRetryDelayMs: parsed.NONCE_LOCK_RETRY_DELAY_MS,
  };
};


