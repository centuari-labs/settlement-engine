import dotenv from 'dotenv';
import path from 'path';

// Load .env.contracts FIRST so its keys win over .env (dotenv default
// behavior: only sets unset keys, so first-wins gives priority to the
// auto-generated file synced from smart-contract-revamp/bin/sync-to-services.sh).
dotenv.config({ path: path.resolve(__dirname, '..', '.env.contracts') });
dotenv.config();

import { loadConfig } from './config';
import { getRedisClient, closeRedisClient } from './redis/client';
import {
  ensureConsumerGroup,
  processPendingEntriesOnStartup,
  type ReadMatchesOptions,
} from './redis/settlementMatchConsumer';
import { BatchAccumulator } from './settlement/batchAccumulator';
import { BatchProcessor } from './settlement/batchProcessor';
import { createNonceManager } from './settlement/nonceManager';
import { ensureTurnkeyPolicy } from './turnkey/policy';
import { logger } from './logger';

const main = async (): Promise<void> => {
  const config = loadConfig();
  const redis = getRedisClient(config);

  await ensureTurnkeyPolicy(config);

  // Ensure the consumer group exists before starting.
  await ensureConsumerGroup(
    redis,
    config.settlementMatchesStream,
    config.consumerGroup,
  );

  // Create batch accumulator
  const accumulator = new BatchAccumulator(
    config.batchSize,
    config.batchIntervalMs,
  );

  // Process pending entries on startup and add them to accumulator
  const readMatchesOptions: ReadMatchesOptions = {
    redis,
    stream: config.settlementMatchesStream,
    consumerGroup: config.consumerGroup,
    consumerName: config.consumerName,
    readCount: config.readCount,
    maxEntries: config.batchSize * 3,
    xclaimMinIdleMs: config.xclaimMinIdleMs,
  };

  try {
    const pendingMatches = await processPendingEntriesOnStartup(
      readMatchesOptions,
    );
    if (pendingMatches.length > 0) {
      accumulator.addMatches(pendingMatches);
      logger.info(
        { component: 'settlement-engine', count: pendingMatches.length },
        'Processed pending matches on startup',
      );
    }
  } catch (error) {
    logger.error(
      { component: 'settlement-engine', err: error },
      'Error processing pending entries on startup',
    );
    // Continue anyway - pending entries will be reclaimed later
  }

  // Create nonce manager for explicit nonce sequencing
  const nonceManager = createNonceManager(redis, config);

  // Create and start batch processor
  const batchProcessor = new BatchProcessor({
    redis,
    config,
    accumulator,
    nonceManager,
  });

  batchProcessor.start();

  logger.info(
    {
      component: 'settlement-engine',
      stream: config.settlementMatchesStream,
      group: config.consumerGroup,
      consumer: config.consumerName,
      batchSize: config.batchSize,
      batchIntervalMs: config.batchIntervalMs,
    },
    'Started',
  );

  const shutdown = async (signal: string): Promise<void> => {
    logger.info({ component: 'settlement-engine', signal }, 'Shutting down...');

    // Stop batch processor (will process pending batches)
    await batchProcessor.stop();

    // Release nonce manager lock
    await nonceManager.destroy();

    // Close Redis connection
    await closeRedisClient();

    logger.info({ component: 'settlement-engine' }, 'Shutdown complete');
    process.exit(0);
  };

  process.on('SIGINT', () => {
    void shutdown('SIGINT');
  });
  process.on('SIGTERM', () => {
    void shutdown('SIGTERM');
  });
};

void main().catch((error) => {
  logger.error({ component: 'settlement-engine', err: error }, 'Fatal error during startup');
  process.exit(1);
});
