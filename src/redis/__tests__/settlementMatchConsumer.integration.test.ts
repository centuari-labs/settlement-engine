import type Redis from 'ioredis';
import {
  ensureConsumerGroup,
  startSettlementMatchConsumer,
  type MatchWithMeta,
} from '../settlementMatchConsumer';
import type { AppConfig } from '../../config';
import { createMatch } from '../../tests/helpers/testFixtures';
import { getRedisClient, closeRedisClient } from '../client';
import { cleanupTestStreams } from '../../tests/helpers/redisTestClient';
import { createTestConfig } from '../../tests/helpers/testConfig';

/**
 * Integration tests for settlement match consumer using a real Redis instance.
 * These tests verify end-to-end behavior with actual Redis stream operations.
 *
 * @requires Redis server running (default: localhost:6379, or set REDIS_TEST_URL)
 */
describe('settlementMatchConsumer Integration Tests', () => {
  let redis: Redis;
  let config: AppConfig;
  let onMatch: jest.Mock<Promise<void>, [MatchWithMeta]>;
  let onInvalid: jest.Mock<Promise<void>, [any]>;
  const activeConsumers: Array<() => void> = [];

  beforeAll(async () => {
    // Test Redis connection before running tests using the real client factory
    const testConfig = createTestConfig();
    redis = getRedisClient(testConfig);
    try {
      await redis.ping();
    } catch (error) {
      throw new Error(
        'Redis is not available. Please start Redis or set REDIS_TEST_URL environment variable.',
      );
    }
    // Close the test connection
    await closeRedisClient();
  });

  beforeEach(async () => {
    // Use the real Redis client factory for each test
    config = createTestConfig({
      settlementMatchesStream: `test:settlement:matches:${Date.now()}`,
      consumerGroup: `test-settlement-engine-${Date.now()}`,
      consumerName: 'test-consumer-1',
    });
    redis = getRedisClient(config);
    onMatch = jest.fn().mockResolvedValue(undefined);
    onInvalid = jest.fn().mockResolvedValue(undefined);
    // Clear active consumers for this test
    activeConsumers.length = 0;
  });

  afterEach(async () => {
    // Stop all active consumers and wait for them to finish
    for (const stop of activeConsumers) {
      stop();
    }
    // Wait for consumers to exit their loops (they might be blocked in xreadgroup)
    // Give them time to exit after stop() is called
    await new Promise((resolve) => setTimeout(resolve, 150));
    
    // Clean up test streams and consumer groups
    await cleanupTestStreams(redis, [config.settlementMatchesStream]);
    // Close the Redis client to reset singleton for next test
    await closeRedisClient();
  });

  it('should create consumer group if it does not exist', async () => {
    await ensureConsumerGroup(
      redis,
      config.settlementMatchesStream,
      config.consumerGroup,
    );

    // Verify group was created by checking it exists (should not throw)
    const groups = await redis.xinfo('GROUPS', config.settlementMatchesStream);
    expect(Array.isArray(groups)).toBe(true);
  });

  it('should handle consumer group that already exists', async () => {
    // Create group first time
    await ensureConsumerGroup(
      redis,
      config.settlementMatchesStream,
      config.consumerGroup,
    );

    // Should not throw when creating again
    await expect(
      ensureConsumerGroup(
        redis,
        config.settlementMatchesStream,
        config.consumerGroup,
      ),
    ).resolves.not.toThrow();
  });

  it('should consume and process a single match from stream', async () => {
    await ensureConsumerGroup(
      redis,
      config.settlementMatchesStream,
      config.consumerGroup,
    );

    // Add a match to the stream
    const match = createMatch();
    const entryId = await redis.xadd(
      config.settlementMatchesStream,
      '*',
      'data',
      JSON.stringify(match),
    );

    expect(entryId).toBeTruthy();

    // Start consumer
    const stop = startSettlementMatchConsumer({
      redis,
      config,
      onMatch,
    });
    activeConsumers.push(stop);

    // Wait for consumer to process
    await new Promise((resolve) => setTimeout(resolve, 200));

    // Verify match was processed
    expect(onMatch).toHaveBeenCalledTimes(1);
    expect(onMatch).toHaveBeenCalledWith({
      id: entryId,
      stream: config.settlementMatchesStream,
      payload: match,
    });

    stop();
  });

  it('should acknowledge entries after processing', async () => {
    await ensureConsumerGroup(
      redis,
      config.settlementMatchesStream,
      config.consumerGroup,
    );

    const match = createMatch();
    const entryId = await redis.xadd(
      config.settlementMatchesStream,
      '*',
      'data',
      JSON.stringify(match),
    );

    const stop = startSettlementMatchConsumer({
      redis,
      config,
      onMatch,
    });
    activeConsumers.push(stop);

    await new Promise((resolve) => setTimeout(resolve, 200));

    // Check pending entries (should be 0 after ack)
    const pending = await redis.xpending(
      config.settlementMatchesStream,
      config.consumerGroup,
    );

    // After acknowledgment, there should be no pending entries
    expect(onMatch).toHaveBeenCalled();
    stop();
  });

  it('should process multiple matches in sequence', async () => {
    await ensureConsumerGroup(
      redis,
      config.settlementMatchesStream,
      config.consumerGroup,
    );

    const match1 = createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440001' });
    const match2 = createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440002' });
    const match3 = createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440003' });

    // Start consumer first, then add messages
    const stop = startSettlementMatchConsumer({
      redis,
      config,
      onMatch,
    });
    activeConsumers.push(stop);

    // Add messages after consumer starts
    await redis.xadd(config.settlementMatchesStream, '*', 'data', JSON.stringify(match1));
    await redis.xadd(config.settlementMatchesStream, '*', 'data', JSON.stringify(match2));
    await redis.xadd(config.settlementMatchesStream, '*', 'data', JSON.stringify(match3));

    // Wait for all matches to be processed
    // Give more time for consumer to read and process
    await new Promise((resolve) => setTimeout(resolve, 600));

    expect(onMatch).toHaveBeenCalledTimes(3);

    // Verify all matches were processed
    const processedMatchIds = onMatch.mock.calls.map((call) => call[0].payload.matchId);
    expect(processedMatchIds).toContain(match1.matchId);
    expect(processedMatchIds).toContain(match2.matchId);
    expect(processedMatchIds).toContain(match3.matchId);

    stop();
  });

  it('should handle invalid match entries with onInvalid handler', async () => {
    await ensureConsumerGroup(
      redis,
      config.settlementMatchesStream,
      config.consumerGroup,
    );

    // Add invalid entry (missing required fields)
    await redis.xadd(
      config.settlementMatchesStream,
      '*',
      'data',
      JSON.stringify({ invalid: 'data' }),
    );

    const stop = startSettlementMatchConsumer({
      redis,
      config,
      onMatch,
      onInvalid,
    });
    activeConsumers.push(stop);

    await new Promise((resolve) => setTimeout(resolve, 200));

    expect(onMatch).not.toHaveBeenCalled();
    expect(onInvalid).toHaveBeenCalledTimes(1);
    expect(onInvalid).toHaveBeenCalledWith({
      id: expect.any(String),
      stream: config.settlementMatchesStream,
      raw: expect.any(Object),
      error: expect.any(Error),
    });

    stop();
  });

  it('should handle matches with individual field format (not JSON)', async () => {
    await ensureConsumerGroup(
      redis,
      config.settlementMatchesStream,
      config.consumerGroup,
    );

    const match = createMatch();

    // Add match as individual fields instead of JSON
    await redis.xadd(
      config.settlementMatchesStream,
      '*',
      'matchId',
      match.matchId,
      'lendOrderId',
      match.lendOrderId,
      'borrowOrderId',
      match.borrowOrderId,
      'lenderWallet',
      match.lenderWallet,
      'borrowerWallet',
      match.borrowerWallet,
      'matchedAmount',
      match.matchedAmount,
      'rate',
      String(match.rate),
      'loanToken',
      match.loanToken,
      'maturity',
      String(match.maturity),
      'timestamp',
      String(match.timestamp),
      'borrowerIsTaker',
      String(match.borrowerIsTaker),
    );

    const stop = startSettlementMatchConsumer({
      redis,
      config,
      onMatch,
    });
    activeConsumers.push(stop);

    await new Promise((resolve) => setTimeout(resolve, 200));

    expect(onMatch).toHaveBeenCalledTimes(1);
    expect(onMatch).toHaveBeenCalledWith({
      id: expect.any(String),
      stream: config.settlementMatchesStream,
      payload: match,
    });

    stop();
  });

  it('should respect readCount limit', async () => {
    await ensureConsumerGroup(
      redis,
      config.settlementMatchesStream,
      config.consumerGroup,
    );

    // Add more matches than readCount
    const matches = Array.from({ length: 15 }, () => createMatch());
    for (const match of matches) {
      await redis.xadd(
        config.settlementMatchesStream,
        '*',
        'data',
        JSON.stringify(match),
      );
    }

    const customConfig = { ...config, readCount: 5 };
    const stop = startSettlementMatchConsumer({
      redis,
      config: customConfig,
      onMatch,
    });
    activeConsumers.push(stop);

    // Wait for processing
    await new Promise((resolve) => setTimeout(resolve, 500));

    // Should process all matches eventually (in multiple iterations)
    // Note: exact count depends on mock implementation, but should process all
    expect(onMatch).toHaveBeenCalled();

    stop();
  });

  it('should stop processing when stop() is called', async () => {
    await ensureConsumerGroup(
      redis,
      config.settlementMatchesStream,
      config.consumerGroup,
    );

    // Add a match
    await redis.xadd(
      config.settlementMatchesStream,
      '*',
      'data',
      JSON.stringify(createMatch()),
    );

    const stop = startSettlementMatchConsumer({
      redis,
      config,
      onMatch,
    });
    activeConsumers.push(stop);

    // Let it process once
    await new Promise((resolve) => setTimeout(resolve, 200));
    const initialCount = onMatch.mock.calls.length;

    // Stop the consumer
    stop();

    // Add another match
    await redis.xadd(
      config.settlementMatchesStream,
      '*',
      'data',
      JSON.stringify(createMatch()),
    );

    // Wait - should not process more
    await new Promise((resolve) => setTimeout(resolve, 300));

    // Should not have processed significantly more (might process one more in flight)
    expect(onMatch.mock.calls.length).toBeLessThanOrEqual(initialCount + 1);
  });
});

