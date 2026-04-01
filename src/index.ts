import 'dotenv/config';

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

const main = async (): Promise<void> => {
  const config = loadConfig();
  const redis = getRedisClient(config);

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
      // eslint-disable-next-line no-console
      console.log(
        `[settlement-engine] Processed ${pendingMatches.length} pending matches on startup`,
      );
    }
  } catch (error) {
    // eslint-disable-next-line no-console
    console.error(
      '[settlement-engine] Error processing pending entries on startup',
      error,
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

  // eslint-disable-next-line no-console
  console.log(
    '[settlement-engine] Started. Listening on stream:',
    config.settlementMatchesStream,
    'group:',
    config.consumerGroup,
    'consumer:',
    config.consumerName,
    'batch-size:',
    config.batchSize,
    'batch-interval-ms:',
    config.batchIntervalMs,
  );

  const shutdown = async (signal: string): Promise<void> => {
    // eslint-disable-next-line no-console
    console.log(`[settlement-engine] Received ${signal}, shutting down...`);

    // Stop batch processor (will process pending batches)
    await batchProcessor.stop();

    // Release nonce manager lock
    await nonceManager.destroy();

    // Close Redis connection
    await closeRedisClient();

    // eslint-disable-next-line no-console
    console.log('[settlement-engine] Shutdown complete');
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
  // eslint-disable-next-line no-console
  console.error('[settlement-engine] Fatal error during startup', error);
  process.exit(1);
});