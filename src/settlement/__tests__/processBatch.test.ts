import type Redis from 'ioredis';
import { processSettlementBatch, type SettlementBatchContext } from '../processBatch';
import {
  createMatch,
  createMatchBatch,
  createMatchWithMeta,
} from '../../tests/helpers/testFixtures';
import { getRedisClient, closeRedisClient } from '../../redis/client';
import { cleanupTestStreams } from '../../tests/helpers/redisTestClient';
import { createTestConfig } from '../../tests/helpers/testConfig';

/**
 * Integration tests for batch settlement processing using a real Redis instance.
 * These tests verify Redis stream operations (xdel, xtrim) work correctly with actual Redis.
 *
 * @requires Redis server running (default: localhost:6379, or set REDIS_TEST_URL)
 */
describe('processSettlementBatch', () => {
  let redis: Redis;
  let context: SettlementBatchContext;
  let testStream: string;
  let consoleLogSpy: jest.SpyInstance;

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
    testStream = `test:settlement:matches:${Date.now()}`;
    const config = createTestConfig({
      settlementMatchesStream: testStream,
    });
    redis = getRedisClient(config);
    context = {
      redis,
      stream: testStream,
      streamMaxLen: 10000,
    };
    consoleLogSpy = jest.spyOn(console, 'log').mockImplementation();
  });

  afterEach(async () => {
    consoleLogSpy.mockRestore();
    // Clean up test streams
    await cleanupTestStreams(redis, [testStream]);
    // Close the Redis client to reset singleton for next test
    await closeRedisClient();
  });

  it('should return early for empty batch', async () => {
    // Add an entry to stream to verify it's not affected
    await redis.xadd(
      context.stream,
      '*',
      'data',
      JSON.stringify(createMatch()),
    );

    await processSettlementBatch([], context);

    expect(consoleLogSpy).toHaveBeenCalledWith(
      '[process-settlement-batch] Received batch of 0 matches',
      [],
    );

    // Stream should still have the entry (not deleted)
    const length = await redis.xlen(context.stream);
    expect(length).toBe(1);
  });

  it('should process a single match batch', async () => {
    const match = createMatch();
    const entryId = await redis.xadd(
      context.stream,
      '*',
      'data',
      JSON.stringify(match),
    );

    if (!entryId) {
      throw new Error('Failed to add entry to stream');
    }

    const matchWithMeta = createMatchWithMeta(match, {
      id: entryId,
      stream: context.stream,
    });

    await processSettlementBatch([matchWithMeta], context);

    expect(consoleLogSpy).toHaveBeenCalledWith(
      '[process-settlement-batch] Received batch of 1 matches',
      [
        {
          id: entryId,
          matchId: match.matchId,
          lendOrderId: match.lendOrderId,
          borrowOrderId: match.borrowOrderId,
        },
      ],
    );

    // Verify entry was deleted
    const length = await redis.xlen(context.stream);
    expect(length).toBe(0);
  });

  it('should process multiple matches from the same stream', async () => {
    const matches = [
      createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440001' }),
      createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440002' }),
      createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440003' }),
    ];

    const entryIds: string[] = [];
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

    const matchesWithMeta = matches.map((match, index) =>
      createMatchWithMeta(match, {
        id: entryIds[index],
        stream: context.stream,
      }),
    );

    await processSettlementBatch(matchesWithMeta, context);

    expect(consoleLogSpy).toHaveBeenCalledWith(
      '[process-settlement-batch] Received batch of 3 matches',
      expect.arrayContaining([
        expect.objectContaining({ id: entryIds[0] }),
        expect.objectContaining({ id: entryIds[1] }),
        expect.objectContaining({ id: entryIds[2] }),
      ]),
    );

    // All entries should be deleted
    const length = await redis.xlen(context.stream);
    expect(length).toBe(0);
  });

  it('should group matches by stream and delete separately', async () => {
    const stream1 = `test:stream1:${Date.now()}`;
    const stream2 = `test:stream2:${Date.now()}`;

    const match1 = createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440001' });
    const match2 = createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440002' });
    const match3 = createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440003' });

    const entryId1 = await redis.xadd(stream1, '*', 'data', JSON.stringify(match1));
    const entryId2 = await redis.xadd(stream1, '*', 'data', JSON.stringify(match2));
    const entryId3 = await redis.xadd(stream2, '*', 'data', JSON.stringify(match3));

    if (!entryId1 || !entryId2 || !entryId3) {
      throw new Error('Failed to add entries to streams');
    }

    const matches = [
      createMatchWithMeta(match1, { id: entryId1, stream: stream1 }),
      createMatchWithMeta(match2, { id: entryId2, stream: stream1 }),
      createMatchWithMeta(match3, { id: entryId3, stream: stream2 }),
    ];

    await processSettlementBatch(matches, context);

    // Both streams should be empty
    const length1 = await redis.xlen(stream1);
    const length2 = await redis.xlen(stream2);
    expect(length1).toBe(0);
    expect(length2).toBe(0);

    // Clean up test streams
    await cleanupTestStreams(redis, [stream1, stream2]);
  });

  it('should use correct streamMaxLen from context', async () => {
    const customContext: SettlementBatchContext = {
      ...context,
      streamMaxLen: 5,
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

    const initialLength = await redis.xlen(context.stream);
    expect(initialLength).toBe(10);

    // Process one entry (which will trigger trim)
    const match = createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440010' });
    const matchWithMeta = createMatchWithMeta(match, {
      id: entries[0],
      stream: context.stream,
    });

    await processSettlementBatch([matchWithMeta], customContext);

    // Stream should be trimmed (exact behavior depends on Redis implementation)
    const finalLength = await redis.xlen(context.stream);
    expect(finalLength).toBeLessThan(initialLength);
  });

  it('should handle large batches correctly', async () => {
    const batchSize = 100;
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

    const matchesWithMeta = matches.map((match, index) =>
      createMatchWithMeta(match, {
        id: entryIds[index],
        stream: context.stream,
      }),
    );

    await processSettlementBatch(matchesWithMeta, context);

    expect(consoleLogSpy).toHaveBeenCalledWith(
      '[process-settlement-batch] Received batch of 100 matches',
      expect.any(Array),
    );

    // All entries should be deleted
    const length = await redis.xlen(context.stream);
    expect(length).toBe(0);
  });

  it('should handle Redis operation failures gracefully', async () => {
    const match = createMatch();
    const entryId = await redis.xadd(
      context.stream,
      '*',
      'data',
      JSON.stringify(match),
    );

    if (!entryId) {
      throw new Error('Failed to add entry to stream');
    }

    const matchWithMeta = createMatchWithMeta(match, {
      id: entryId,
      stream: context.stream,
    });

    // Disconnect Redis to simulate connection failure
    await redis.disconnect();

    try {
      await expect(processSettlementBatch([matchWithMeta], context)).rejects.toThrow();
    } finally {
      // Reconnect for cleanup
      const config = createTestConfig({
        settlementMatchesStream: testStream,
      });
      redis = getRedisClient(config);
    }
  });

  it('should log match details correctly', async () => {
    const match = createMatch({
      matchId: '550e8400-e29b-41d4-a716-446655440100',
      lendOrderId: '550e8400-e29b-41d4-a716-446655440101',
      borrowOrderId: '550e8400-e29b-41d4-a716-446655440102',
    });

    const entryId = await redis.xadd(
      context.stream,
      '*',
      'data',
      JSON.stringify(match),
    );

    if (!entryId) {
      throw new Error('Failed to add entry to stream');
    }

    const matchWithMeta = createMatchWithMeta(match, {
      id: entryId,
      stream: context.stream,
    });

    await processSettlementBatch([matchWithMeta], context);

    expect(consoleLogSpy).toHaveBeenCalledWith(
      '[process-settlement-batch] Received batch of 1 matches',
      [
        {
          id: entryId,
          matchId: '550e8400-e29b-41d4-a716-446655440100',
          lendOrderId: '550e8400-e29b-41d4-a716-446655440101',
          borrowOrderId: '550e8400-e29b-41d4-a716-446655440102',
        },
      ],
    );

    // Verify entry was deleted
    const length = await redis.xlen(context.stream);
    expect(length).toBe(0);
  });
});

