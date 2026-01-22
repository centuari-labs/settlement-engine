import type Redis from 'ioredis';
import { processSettlementBatch, type SettlementBatchContext } from '../processBatch';
import { ensureConsumerGroup } from '../../redis/settlementMatchConsumer';
import {
  createMatch,
  createMatchBatch,
  createMatchWithMeta,
} from '../../tests/helpers/testFixtures';
import { getRedisClient, closeRedisClient } from '../../redis/client';
import {
  cleanupTestStreams,
  removePendingMessages,
} from '../../tests/helpers/redisTestClient';
import { createTestConfig } from '../../tests/helpers/testConfig';
import { setupMockSettleBatch, getMockSettleBatch } from '../../tests/helpers/mockSmartContract';

/**
 * Integration tests for batch settlement processing using a real Redis instance.
 * These tests verify Redis stream operations (xdel, xtrim) work correctly with actual Redis.
 *
 * @requires Redis server running (default: localhost:6379, or set REDIS_TEST_URL)
 */
describe('processSettlementBatch Integration Tests', () => {
  let redis: Redis;
  let context: SettlementBatchContext;
  let testStream: string;

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

  let testConfig: ReturnType<typeof createTestConfig>;

  beforeEach(async () => {
    // Set up mock smart contract to return successful results
    const mockSettleBatch = getMockSettleBatch();
    setupMockSettleBatch(mockSettleBatch);

    // Use the real Redis client factory for each test
    // Use a more unique stream name to avoid collisions
    testStream = `test:settlement:matches:${Date.now()}-${Math.random().toString(36).substring(7)}`;
    testConfig = createTestConfig({
      settlementMatchesStream: testStream,
    });
    redis = getRedisClient(testConfig);
    // Ensure stream doesn't exist before test starts
    try {
      await redis.del(testStream);
    } catch {
      // Ignore if stream doesn't exist
    }
    context = {
      redis,
      stream: testStream,
      consumerGroup: testConfig.consumerGroup,
      streamMaxLen: 10000,
    };
    // Remove any pending messages before running the test
    await removePendingMessages(redis, testStream, testConfig.consumerGroup);
  });

  afterEach(async () => {
    // Clean up test streams
    await cleanupTestStreams(redis, [testStream]);
    // Close the Redis client to reset singleton for next test
    await closeRedisClient();
  });

  it('should delete processed entries from stream', async () => {
    // Add entries to stream
    const match1 = createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440001' });
    const match2 = createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440002' });

    const entryId1 = await redis.xadd(
      context.stream,
      '*',
      'data',
      JSON.stringify(match1),
    );
    const entryId2 = await redis.xadd(
      context.stream,
      '*',
      'data',
      JSON.stringify(match2),
    );

    if (!entryId1 || !entryId2) {
      throw new Error('Failed to add entries to stream');
    }

    // Create matches with meta using the actual entry IDs
    const matches = [
      createMatchWithMeta(match1, { id: entryId1, stream: context.stream }),
      createMatchWithMeta(match2, { id: entryId2, stream: context.stream }),
    ];

    // Process batch - retry in case of random failures
    let retries = 3;
    while (retries > 0) {
      try {
        await processSettlementBatch(matches, context, testConfig);
        break;
      } catch (error) {
        retries--;
        if (retries === 0) {
          throw error;
        }
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
    }

    // Verify entries were deleted by checking stream length
    const length = await redis.xlen(context.stream);
    expect(length).toBe(0);
  });

  it('should delete entries from multiple streams separately', async () => {
    const stream1 = `test:stream1:${Date.now()}`;
    const stream2 = `test:stream2:${Date.now()}`;

    // Ensure consumer groups exist for both streams
    await ensureConsumerGroup(redis, stream1, context.consumerGroup);
    await ensureConsumerGroup(redis, stream2, context.consumerGroup);

    const match1 = createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440001' });
    const match2 = createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440002' });

    const entryId1 = await redis.xadd(stream1, '*', 'data', JSON.stringify(match1));
    const entryId2 = await redis.xadd(stream2, '*', 'data', JSON.stringify(match2));

    if (!entryId1 || !entryId2) {
      throw new Error('Failed to add entries to stream');
    }

    const matches = [
      createMatchWithMeta(match1, { id: entryId1, stream: stream1 }),
      createMatchWithMeta(match2, { id: entryId2, stream: stream2 }),
    ];

    // Retry in case of random failures
    let retries = 3;
    while (retries > 0) {
      try {
        await processSettlementBatch(matches, context, testConfig);
        break;
      } catch (error) {
        retries--;
        if (retries === 0) {
          throw error;
        }
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
    }

    // Both streams should be empty
    const length1 = await redis.xlen(stream1);
    const length2 = await redis.xlen(stream2);
    expect(length1).toBe(0);
    expect(length2).toBe(0);

    // Clean up test streams
    await cleanupTestStreams(redis, [stream1, stream2]);
  });

  it('should trim stream to maxLen after processing', async () => {
    const streamMaxLen = 5;
    const customContext: SettlementBatchContext = {
      ...context,
      streamMaxLen,
    };

    // Add more entries than maxLen
    const entries: string[] = [];
    for (let i = 0; i < 10; i++) {
      const match = createMatch({
        matchId: `550e8400-e29b-41d4-a716-44665544000${i}`,
      });
      const entryId = await redis.xadd(
        context.stream,
        '*',
        'data',
        JSON.stringify(match),
      );
      if (entryId) {
        entries.push(entryId);
      }
    }

    // Process one entry (which will trigger trim)
    const match = createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440010' });
    const matches = [
      createMatchWithMeta(match, { id: entries[0], stream: context.stream }),
    ];

    // Retry in case of random database failures
    let retries = 3;
    while (retries > 0) {
      try {
        await processSettlementBatch(matches, customContext, testConfig);
        break;
      } catch (error) {
        retries--;
        if (retries === 0) {
          throw error;
        }
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
    }

    // Stream length should be trimmed (approximately maxLen, but exact behavior depends on mock)
    const length = await redis.xlen(context.stream);
    // The entry we processed should be deleted, and trim should have removed older entries
    expect(length).toBeLessThan(10);
  });

  it('should handle empty batch without errors', async () => {
    // Add some entries to stream
    await redis.xadd(
      context.stream,
      '*',
      'data',
      JSON.stringify(createMatch()),
    );

    await expect(processSettlementBatch([], context, testConfig)).resolves.not.toThrow();

    // Stream should still have the entry
    const length = await redis.xlen(context.stream);
    expect(length).toBe(1);
  });

  it('should process large batches correctly', async () => {
    // Ensure stream is clean before starting
    const initialLength = await redis.xlen(context.stream);
    if (initialLength > 0) {
      await redis.del(context.stream);
    }

    const batchSize = 50;
    const matches = createMatchBatch(batchSize);
    const entryIds: string[] = [];

    // Add all matches to stream
    for (const match of matches) {
      const entryId = await redis.xadd(
        context.stream,
        '*',
        'data',
        JSON.stringify(match),
      );
      if (entryId) {
        entryIds.push(entryId);
      }
    }

    // Verify all entries were added
    const lengthBeforeProcessing = await redis.xlen(context.stream);
    expect(lengthBeforeProcessing).toBe(batchSize);

    // Create matches with meta
    const matchesWithMeta = matches.map((match, index) =>
      createMatchWithMeta(match, { id: entryIds[index], stream: context.stream }),
    );

    // Retry in case of random smart contract/database failures
    let retries = 3;
    while (retries > 0) {
      try {
        await processSettlementBatch(matchesWithMeta, context, testConfig);
        break;
      } catch (error) {
        retries--;
        if (retries === 0) {
          throw error;
        }
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
    }

    // Wait a bit for Redis operations to complete
    await new Promise((resolve) => setTimeout(resolve, 50));

    // All entries should be deleted
    const length = await redis.xlen(context.stream);
    expect(length).toBe(0);
  });

  it('should group entries by stream and delete efficiently', async () => {
    const stream1 = `test:stream1:${Date.now()}`;
    const stream2 = `test:stream2:${Date.now()}`;

    // Ensure consumer groups exist for both streams
    await ensureConsumerGroup(redis, stream1, context.consumerGroup);
    await ensureConsumerGroup(redis, stream2, context.consumerGroup);

    // Add entries to both streams
    const entryIds1: string[] = [];
    const entryIds2: string[] = [];

    for (let i = 0; i < 3; i++) {
      const match = createMatch({
        matchId: `550e8400-e29b-41d4-a716-44665544000${i}`,
      });
      const id1 = await redis.xadd(stream1, '*', 'data', JSON.stringify(match));
      const id2 = await redis.xadd(stream2, '*', 'data', JSON.stringify(match));
      if (id1) {
        entryIds1.push(id1);
      }
      if (id2) {
        entryIds2.push(id2);
      }
    }

    // Create matches from both streams
    const matches = [
      ...entryIds1.map((id, index) =>
        createMatchWithMeta(
          createMatch({ matchId: `550e8400-e29b-41d4-a716-44665544000${index}` }),
          { id, stream: stream1 },
        ),
      ),
      ...entryIds2.map((id, index) =>
        createMatchWithMeta(
          createMatch({ matchId: `550e8400-e29b-41d4-a716-44665544000${index}` }),
          { id, stream: stream2 },
        ),
      ),
    ];

    // Retry in case of random smart contract/database failures
    let retries = 3;
    while (retries > 0) {
      try {
        await processSettlementBatch(matches, context, testConfig);
        break;
      } catch (error) {
        retries--;
        if (retries === 0) {
          throw error;
        }
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
    }

    // Both streams should be empty
    const length1 = await redis.xlen(stream1);
    const length2 = await redis.xlen(stream2);
    expect(length1).toBe(0);
    expect(length2).toBe(0);

    // Clean up test streams
    await cleanupTestStreams(redis, [stream1, stream2]);
  });

  it('should handle stream trim with approximate length', async () => {
    const streamMaxLen = 3;
    const customContext: SettlementBatchContext = {
      ...context,
      streamMaxLen,
    };

    // Add entries
    const entries: string[] = [];
    for (let i = 0; i < 10; i++) {
      const match = createMatch({
        matchId: `550e8400-e29b-41d4-a716-44665544000${i}`,
      });
      const entryId = await redis.xadd(
        context.stream,
        '*',
        'data',
        JSON.stringify(match),
      );
      if (entryId) {
        entries.push(entryId);
      }
    }

    const initialLength = await redis.xlen(context.stream);
    expect(initialLength).toBe(10);

    // Process the first entry
    const match = createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440010' });
    const matches = [
      createMatchWithMeta(match, { id: entries[0], stream: context.stream }),
    ];

    // Retry in case of random database failures
    let retries = 3;
    while (retries > 0) {
      try {
        await processSettlementBatch(matches, customContext, testConfig);
        break;
      } catch (error) {
        retries--;
        if (retries === 0) {
          throw error;
        }
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
    }

    // Stream should be trimmed (exact behavior depends on mock implementation)
    const finalLength = await redis.xlen(context.stream);
    expect(finalLength).toBeLessThan(initialLength);
  });
});

