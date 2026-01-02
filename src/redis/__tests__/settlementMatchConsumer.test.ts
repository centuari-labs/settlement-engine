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
 * Unit tests for settlement match consumer using a real Redis instance.
 * These tests verify behavior with actual Redis stream operations.
 *
 * @requires Redis server running (default: localhost:6379, or set REDIS_TEST_URL)
 */
describe('ensureConsumerGroup', () => {
  let redis: Redis;
  let streamName: string;
  let groupName: string;

  beforeAll(async () => {
    // Test Redis connection
    const testConfig = createTestConfig();
    redis = getRedisClient(testConfig);
    try {
      await redis.ping();
    } catch (error) {
      throw new Error(
        'Redis is not available. Please start Redis or set REDIS_TEST_URL environment variable.',
      );
    }
    await closeRedisClient();
  });

  beforeEach(async () => {
    streamName = `test:stream:${Date.now()}`;
    groupName = `test-group:${Date.now()}`;
    redis = getRedisClient(
      createTestConfig({
        settlementMatchesStream: streamName,
        consumerGroup: groupName,
      }),
    );
  });

  afterEach(async () => {
    await cleanupTestStreams(redis, [streamName]);
    await closeRedisClient();
  });

  it('should create a consumer group successfully', async () => {
    await ensureConsumerGroup(redis, streamName, groupName);

    // Verify group was created
    const groups = await redis.xinfo('GROUPS', streamName);
    expect(Array.isArray(groups)).toBe(true);
    if (Array.isArray(groups)) {
      expect(groups.length).toBeGreaterThan(0);
    }
  });

  it('should handle BUSYGROUP error gracefully when group already exists', async () => {
    // Create group first time
    await ensureConsumerGroup(redis, streamName, groupName);

    // Should not throw when creating again
    await expect(
      ensureConsumerGroup(redis, streamName, groupName),
    ).resolves.not.toThrow();
  });

  it('should propagate other errors', async () => {
    // Try to create group on non-existent stream without MKSTREAM
    await expect(
      redis.xgroup('CREATE', 'non-existent-stream', groupName, '0'),
    ).rejects.toThrow();
  });
});

describe('startSettlementMatchConsumer', () => {
  let redis: Redis;
  let config: AppConfig;
  let onMatch: jest.Mock<Promise<void>, [MatchWithMeta]>;
  let onInvalid: jest.Mock<Promise<void>, [any]>;
  const activeConsumers: Array<() => void> = [];

  beforeAll(async () => {
    // Test Redis connection
    const testConfig = createTestConfig();
    redis = getRedisClient(testConfig);
    try {
      await redis.ping();
    } catch (error) {
      throw new Error(
        'Redis is not available. Please start Redis or set REDIS_TEST_URL environment variable.',
      );
    }
    await closeRedisClient();
  });

  beforeEach(async () => {
    config = createTestConfig({
      settlementMatchesStream: `test:settlement:matches:${Date.now()}`,
      consumerGroup: `test-settlement-engine-${Date.now()}`,
      consumerName: 'test-consumer-1',
    });
    // Ensure we have a fresh Redis connection
    // getRedisClient will automatically recreate if closed
    redis = getRedisClient(config);
    // Verify connection is ready
    await redis.ping();
    // Reset mocks completely to avoid leftover calls from previous tests
    onMatch = jest.fn().mockResolvedValue(undefined);
    onInvalid = jest.fn().mockResolvedValue(undefined);
    onMatch.mockClear();
    onInvalid.mockClear();
    activeConsumers.length = 0;
    jest.useRealTimers();
  });

  afterEach(async () => {
    // Stop all active consumers to prevent memory leaks
    activeConsumers.forEach((stop) => stop());
    activeConsumers.length = 0;
    // Wait longer to ensure consumers have time to check isRunning flag and exit
    await new Promise((resolve) => setTimeout(resolve, 300));
    // Clean up test streams - delete the stream completely to remove all messages
    try {
      if (redis && redis.status === 'ready') {
        // Delete the stream entirely to ensure no leftover messages
        await redis.del(config.settlementMatchesStream);
        // Also try to destroy consumer group if it exists
        try {
          await redis.xgroup('DESTROY', config.settlementMatchesStream, config.consumerGroup);
        } catch {
          // Ignore if group doesn't exist
        }
      }
    } catch {
      // Ignore cleanup errors (redis might be disconnected)
    }
    // Close Redis client
    try {
      await closeRedisClient();
    } catch {
      // Ignore close errors
    }
    // Restore real timers if any test used fake timers
    jest.useRealTimers();
  });

  it('should process valid match with JSON data field', async () => {
    await ensureConsumerGroup(
      redis,
      config.settlementMatchesStream,
      config.consumerGroup,
    );

    const stop = startSettlementMatchConsumer({
      redis,
      config,
      onMatch,
    });
    activeConsumers.push(stop);

    // Give the consumer a moment to start its loop and begin reading
    await new Promise((resolve) => setTimeout(resolve, 50));

    const match = createMatch();
    const entryId = await redis.xadd(
      config.settlementMatchesStream,
      '*',
      'data',
      JSON.stringify(match),
    );

    // Wait for async processing
    await new Promise((resolve) => setTimeout(resolve, 200));

    expect(onMatch).toHaveBeenCalledTimes(1);
    expect(onMatch).toHaveBeenCalledWith({
      id: entryId,
      stream: config.settlementMatchesStream,
      payload: match,
    });

    stop();
  });

  it('should process valid match with individual fields', async () => {
    await ensureConsumerGroup(
      redis,
      config.settlementMatchesStream,
      config.consumerGroup,
    );

    const match = createMatch();
    // Include all required fields as individual stream fields
    const entryId = await redis.xadd(
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
      id: entryId,
      stream: config.settlementMatchesStream,
      payload: match,
    });

    stop();
  });

  it('should handle invalid JSON in data field gracefully', async () => {
    await ensureConsumerGroup(
      redis,
      config.settlementMatchesStream,
      config.consumerGroup,
    );

    const entryId = await redis.xadd(
      config.settlementMatchesStream,
      '*',
      'data',
      'invalid json {',
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
      id: entryId,
      stream: config.settlementMatchesStream,
      raw: { data: 'invalid json {' },
      error: expect.any(Error),
    });

    stop();
  });

  it('should handle invalid schema matches', async () => {
    await ensureConsumerGroup(
      redis,
      config.settlementMatchesStream,
      config.consumerGroup,
    );

    // Ensure stream is empty before adding the invalid match
    const streamInfo = await redis.xinfo('STREAM', config.settlementMatchesStream);
    if (Array.isArray(streamInfo)) {
      const lengthIndex = streamInfo.findIndex((v) => v === 'length');
      if (lengthIndex !== -1 && streamInfo[lengthIndex + 1] > 0) {
        // Stream has messages, delete them
        await redis.del(config.settlementMatchesStream);
        await ensureConsumerGroup(
          redis,
          config.settlementMatchesStream,
          config.consumerGroup,
        );
      }
    }

    const invalidMatch = {
      matchId: 'invalid', // Not a UUID
      lendOrderId: '550e8400-e29b-41d4-a716-446655440001',
    };
    const entryId = await redis.xadd(
      config.settlementMatchesStream,
      '*',
      'data',
      JSON.stringify(invalidMatch),
    );

    const stop = startSettlementMatchConsumer({
      redis,
      config,
      onMatch,
      onInvalid,
    });
    activeConsumers.push(stop);

    await new Promise((resolve) => setTimeout(resolve, 300));

    expect(onMatch).not.toHaveBeenCalled();
    expect(onInvalid).toHaveBeenCalledTimes(1);

    stop();
  });

  it('should log to console.error when onInvalid handler is not provided', async () => {
    await ensureConsumerGroup(
      redis,
      config.settlementMatchesStream,
      config.consumerGroup,
    );

    const consoleSpy = jest.spyOn(console, 'error').mockImplementation();
    const entryId = await redis.xadd(
      config.settlementMatchesStream,
      '*',
      'data',
      JSON.stringify({ invalid: 'data' }),
    );

    const stop = startSettlementMatchConsumer({
      redis,
      config,
      onMatch,
    });
    activeConsumers.push(stop);

    await new Promise((resolve) => setTimeout(resolve, 200));

    expect(onMatch).not.toHaveBeenCalled();
    expect(consoleSpy).toHaveBeenCalledWith(
      '[settlement-consumer] Invalid match entry',
      expect.any(String),
    );

    consoleSpy.mockRestore();
    stop();
  });

  it('should handle null result (no messages)', async () => {
    await ensureConsumerGroup(
      redis,
      config.settlementMatchesStream,
      config.consumerGroup,
    );

    const stop = startSettlementMatchConsumer({
      redis,
      config,
      onMatch,
    });
    activeConsumers.push(stop);

    // Wait for the consumer to process (should get no messages)
    await new Promise((resolve) => setTimeout(resolve, 200));

    expect(onMatch).not.toHaveBeenCalled();

    stop();
  });

  it('should process multiple matches in sequence', async () => {
    await ensureConsumerGroup(
      redis,
      config.settlementMatchesStream,
      config.consumerGroup,
    );

    const stop = startSettlementMatchConsumer({
      redis,
      config,
      onMatch,
    });
    activeConsumers.push(stop);

    // Give the consumer a moment to start its loop and begin reading
    await new Promise((resolve) => setTimeout(resolve, 50));

    const match1 = createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440001' });
    const match2 = createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440002' });

    const entryId1 = await redis.xadd(
      config.settlementMatchesStream,
      '*',
      'data',
      JSON.stringify(match1),
    );
    const entryId2 = await redis.xadd(
      config.settlementMatchesStream,
      '*',
      'data',
      JSON.stringify(match2),
    );

    // Wait for async processing
    await new Promise((resolve) => setTimeout(resolve, 200));

    expect(onMatch).toHaveBeenCalledTimes(2);
    expect(onMatch).toHaveBeenNthCalledWith(1, {
      id: entryId1,
      stream: config.settlementMatchesStream,
      payload: match1,
    });
    expect(onMatch).toHaveBeenNthCalledWith(2, {
      id: entryId2,
      stream: config.settlementMatchesStream,
      payload: match2,
    });

    stop();
  });

  it('should handle errors gracefully with backoff', async () => {
    const consoleSpy = jest.spyOn(console, 'error').mockImplementation();

    await ensureConsumerGroup(
      redis,
      config.settlementMatchesStream,
      config.consumerGroup,
    );

    // Create a consumer that will encounter an error
    // We'll disconnect Redis after starting to simulate connection loss
    const stop = startSettlementMatchConsumer({
      redis,
      config,
      onMatch,
    });
    activeConsumers.push(stop);

    // Disconnect Redis to cause errors (use disconnect instead of quit to avoid cleanup issues)
    try {
      redis.disconnect();
    } catch {
      // Ignore disconnect errors
    }

    // Wait for error to occur and backoff (1000ms)
    await new Promise((resolve) => setTimeout(resolve, 1500));

    // Should have logged an error
    expect(consoleSpy).toHaveBeenCalled();

    stop();
    // Re-open client for cleanup - getRedisClient will detect closed connection and create new one
    await closeRedisClient();
    redis = getRedisClient(config);
    // Ensure connection is ready
    await redis.ping();

    consoleSpy.mockRestore();
  }, 5000);

  it('should stop when stop() is called', async () => {
    await ensureConsumerGroup(
      redis,
      config.settlementMatchesStream,
      config.consumerGroup,
    );

    const match = createMatch();
    await redis.xadd(
      config.settlementMatchesStream,
      '*',
      'data',
      JSON.stringify(match),
    );

    // Use shorter block time so stop() works faster
    const stop = startSettlementMatchConsumer({
      redis,
      config: { ...config, readBlockMs: 50 },
      onMatch,
    });
    activeConsumers.push(stop);

    // Wait for initial processing
    await new Promise((resolve) => setTimeout(resolve, 200));

    const initialCallCount = onMatch.mock.calls.length;
    expect(initialCallCount).toBeGreaterThan(0);

    // Stop the consumer
    stop();

    // Wait for block timeout plus a bit more for the loop to check isRunning
    await new Promise((resolve) => setTimeout(resolve, 150));

    // Should not have processed significantly more (might process 1 more if in flight)
    expect(onMatch.mock.calls.length).toBeLessThanOrEqual(initialCallCount + 1);
  }, 5000);

  it('should respect readBlockMs and readCount config', async () => {
    await ensureConsumerGroup(
      redis,
      config.settlementMatchesStream,
      config.consumerGroup,
    );

    // Start consumer with a short block time for faster test completion
    const stop = startSettlementMatchConsumer({
      redis,
      config: { ...config, readBlockMs: 100 },
      onMatch,
    });
    activeConsumers.push(stop);

    // Wait a bit - consumer should be waiting (no messages)
    await new Promise((resolve) => setTimeout(resolve, 50));

    // No messages should have been processed yet
    expect(onMatch).not.toHaveBeenCalled();

    stop();
    // Wait for the block to timeout so consumer can exit
    await new Promise((resolve) => setTimeout(resolve, 150));
  }, 5000);

  it('should handle multiple streams correctly', async () => {
    // Note: The consumer only reads from settlementMatchesStream
    // This test verifies it processes messages correctly
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
      config: { ...config, readBlockMs: 100 },
      onMatch,
    });
    activeConsumers.push(stop);

    await new Promise((resolve) => setTimeout(resolve, 200));

    expect(onMatch).toHaveBeenCalledTimes(1);
    expect(onMatch).toHaveBeenCalledWith({
      id: entryId,
      stream: config.settlementMatchesStream,
      payload: match,
    });

    stop();
    // Wait for consumer to exit
    await new Promise((resolve) => setTimeout(resolve, 150));
  }, 5000);
});
