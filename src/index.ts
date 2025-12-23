import 'dotenv/config';

import { loadConfig } from './config';
import { getRedisClient, closeRedisClient } from './redis/client';
import {
  ensureConsumerGroup,
  startSettlementMatchConsumer,
  type MatchWithMeta,
} from './redis/settlementMatchConsumer';
import { processSettlementBatch } from './settlement/processBatch';

const main = async (): Promise<void> => {
  const config = loadConfig();
  const redis = getRedisClient(config);

  // Ensure the consumer group exists before starting the loop.
  await ensureConsumerGroup(
    redis,
    config.settlementMatchesStream,
    config.consumerGroup,
  );

  // For now we process matches one-by-one but route them through a batch-aware API
  // so we can easily switch to true batching later. After a successful batch
  // (currently size 1), we delete the corresponding stream entries and apply
  // bounded stream trimming inside processSettlementBatch.
  const onMatch = async (match: MatchWithMeta): Promise<void> => {
    await processSettlementBatch([match], {
      redis,
      stream: config.settlementMatchesStream,
      streamMaxLen: config.streamMaxLen,
    });
  };

  const stopConsumer = startSettlementMatchConsumer({
    redis,
    config,
    onMatch,
  });

  // eslint-disable-next-line no-console
  console.log(
    '[settlement-engine] Started. Listening on stream:',
    config.settlementMatchesStream,
    'group:',
    config.consumerGroup,
    'consumer:',
    config.consumerName,
  );

  const shutdown = async (signal: string): Promise<void> => {
    // eslint-disable-next-line no-console
    console.log(`[settlement-engine] Received ${signal}, shutting down...`);
    stopConsumer();
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


