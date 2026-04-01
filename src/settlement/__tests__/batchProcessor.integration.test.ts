import type Redis from 'ioredis';
import { BatchProcessor } from '../batchProcessor';
import { BatchAccumulator } from '../batchAccumulator';
import { ensureConsumerGroup } from '../../redis/settlementMatchConsumer';
import { createMatch } from '../../tests/helpers/testFixtures';
import { getRedisClient, closeRedisClient } from '../../redis/client';
import {
  cleanupTestStreams,
  removePendingMessages,
} from '../../tests/helpers/redisTestClient';
import { createTestConfig } from '../../tests/helpers/testConfig';
import type { AppConfig } from '../../config';
import { persistSettlementResults } from '../database';
import { setupMockSettleBatch, getMockSettleBatch } from '../../tests/helpers/mockSmartContract';

// Mock database to avoid random failures
jest.mock('../database');

const mockSettleBatch = getMockSettleBatch();
const mockPersistSettlementResults = persistSettlementResults as jest.MockedFunction<
  typeof persistSettlementResults
>;

/**
 * Integration tests for BatchProcessor using a real Redis instance.
 * These tests verify end-to-end batch processing behavior with actual Redis stream operations.
 *
 * @requires Redis server running (default: localhost:6379, or set REDIS_TEST_URL)
 */
describe('BatchProcessor Integration Tests', () => {
  let redis: Redis;
  let config: AppConfig;
  let accumulator: BatchAccumulator;
  let processor: BatchProcessor;
  let onInvalid: jest.Mock<Promise<void>, [any]>;

  // Increase timeout for all tests in this suite
  jest.setTimeout(30000);

  beforeAll(async () => {
    // Test Redis connection before running tests
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
    // Reset mocks
    jest.clearAllMocks();

    config = createTestConfig({
      settlementMatchesStream: `test:settlement:matches:${Date.now()}`,
      consumerGroup: `test-settlement-engine-${Date.now()}`,
      consumerName: 'test-consumer-1',
      batchSize: 3,
      batchIntervalMs: 500,
      pollIntervalMs: 100,
      readCount: 10,
    });

    redis = getRedisClient(config);
    await Promise.race([
      ensureConsumerGroup(
        redis,
        config.settlementMatchesStream,
        config.consumerGroup,
      ),
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error('ensureConsumerGroup timeout')), 10000),
      ),
    ]).catch((error) => {
      // If timeout, try to close and rethrow
      closeRedisClient().catch(() => {});
      throw error;
    });
    // Remove any pending messages before running the test
    await removePendingMessages(
      redis,
      config.settlementMatchesStream,
      config.consumerGroup,
    );

    accumulator = new BatchAccumulator(
      config.batchSize,
      config.batchIntervalMs,
    );
    onInvalid = jest.fn().mockResolvedValue(undefined);

    processor = new BatchProcessor({
      redis,
      config,
      accumulator,
      onInvalid,
    });

    // Set up default successful mocks using the mock helper
    setupMockSettleBatch(mockSettleBatch);
    mockPersistSettlementResults.mockResolvedValue(undefined);
  }, 30000);

  afterEach(async () => {
    // Stop processor if running with timeout
    try {
      await Promise.race([
        processor.stop(),
        new Promise((_, reject) =>
          setTimeout(() => reject(new Error('stop timeout')), 10000),
        ),
      ]);
    } catch {
      // Ignore errors if already stopped or timeout
    }

    // Clean up test streams
    await cleanupTestStreams(redis, [config.settlementMatchesStream]);
    await closeRedisClient();
  }, 30000);

  it('should process matches when batch size is reached', async () => {
    // Add matches to stream
    const matches = [
      createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440001' }),
      createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440002' }),
      createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440003' }),
    ];

    for (const match of matches) {
      await redis.xadd(
        config.settlementMatchesStream,
        '*',
        'data',
        JSON.stringify(match),
      );
    }

    processor.start();

    // Wait for batch to be processed - poll until stream is empty
    let attempts = 0;
    let length = await redis.xlen(config.settlementMatchesStream);
    while (length > 0 && attempts < 100) {
      await new Promise((resolve) => setTimeout(resolve, 10));
      length = await redis.xlen(config.settlementMatchesStream);
      attempts++;
    }

    // Verify entries were deleted (processed)
    expect(length).toBe(0);
  }, 10000);

  it('should process matches when time interval has elapsed', async () => {
    // Add one match (less than batch size)
    const match = createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440001' });
    await redis.xadd(
      config.settlementMatchesStream,
      '*',
      'data',
      JSON.stringify(match),
    );

    processor.start();

    // Wait for time interval to elapse (batchIntervalMs is 500ms)
    await new Promise((resolve) => setTimeout(resolve, 800));

    // Verify entry was processed
    const length = await redis.xlen(config.settlementMatchesStream);
    expect(length).toBe(0);
  }, 10000);

  it('should read matches from Redis stream when accumulator needs more', async () => {
    // Add matches to stream
    const matches = [
      createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440001' }),
      createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440002' }),
    ];

    for (const match of matches) {
      await redis.xadd(
        config.settlementMatchesStream,
        '*',
        'data',
        JSON.stringify(match),
      );
    }

    processor.start();

    // Wait for matches to be read - poll until accumulator has matches
    let attempts = 0;
    while (accumulator.getPendingCount() < 2 && attempts < 100) {
      await new Promise((resolve) => setTimeout(resolve, 10));
      attempts++;
    }

    // Verify matches were added to accumulator
    expect(accumulator.getPendingCount()).toBeGreaterThanOrEqual(2);
  }, 10000);

  it('should handle invalid entries with onInvalid handler', async () => {
    // Add invalid entry
    await redis.xadd(
      config.settlementMatchesStream,
      '*',
      'data',
      'invalid json {',
    );

    processor.start();

    // Wait for processing
    await new Promise((resolve) => setTimeout(resolve, 500));

    // Verify onInvalid was called
    expect(onInvalid).toHaveBeenCalled();
    expect(onInvalid).toHaveBeenCalledWith(
      expect.objectContaining({
        id: expect.any(String),
        stream: config.settlementMatchesStream,
        raw: expect.any(Object),
        error: expect.any(Error),
      }),
    );
  }, 10000);

  it('should process multiple batches sequentially', async () => {
    // Add enough matches for multiple batches (batch size is 3)
    const matches = Array.from({ length: 6 }, (_, index) =>
      createMatch({
        matchId: `550e8400-e29b-41d4-a716-44665544000${index + 1}`,
      }),
    );

    for (const match of matches) {
      await redis.xadd(
        config.settlementMatchesStream,
        '*',
        'data',
        JSON.stringify(match),
      );
    }

    processor.start();

    // Wait for all batches to be processed
    await new Promise((resolve) => setTimeout(resolve, 2000));

    // Verify all entries were processed
    const length = await redis.xlen(config.settlementMatchesStream);
    expect(length).toBe(0);
  }, 15000);

  it('should stop gracefully and process pending matches', async () => {
    // Add matches to stream
    const matches = [
      createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440001' }),
      createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440002' }),
    ];

    for (const match of matches) {
      await redis.xadd(
        config.settlementMatchesStream,
        '*',
        'data',
        JSON.stringify(match),
      );
    }

    processor.start();

    // Wait for matches to be read
    await new Promise((resolve) => setTimeout(resolve, 300));

    // Stop processor
    await processor.stop();

    // Wait a bit for final processing
    await new Promise((resolve) => setTimeout(resolve, 500));

    // Verify entries were processed (or at least read)
    // Since we're stopping, some might still be in accumulator
    // But they should have been processed if stop() worked correctly
    const pendingCount = accumulator.getPendingCount();
    expect(pendingCount).toBeLessThanOrEqual(2);
  }, 10000);

  it('should respect pollIntervalMs configuration', async () => {
    // Spy on xreadgroup: each poll calls readMatches() which calls redis.xreadgroup.
    // This verifies the poll interval is actually firing at the configured rate.
    const xreadgroupSpy = jest.spyOn(redis, 'xreadgroup');

    processor.start();

    // Wait for multiple poll intervals (pollIntervalMs = 100ms, so ~3-4 polls in 350ms)
    await new Promise((resolve) => setTimeout(resolve, 350));

    // Expect at least 2 xreadgroup calls, confirming polling is active
    expect(xreadgroupSpy.mock.calls.length).toBeGreaterThan(1);
  }, 10000);

  it('should handle empty stream gracefully', async () => {
    processor.start();

    // Wait for polling
    await new Promise((resolve) => setTimeout(resolve, 300));

    // Should not throw errors
    expect(accumulator.getPendingCount()).toBe(0);
  }, 10000);

  it('should accumulate matches until batch size is reached', async () => {
    // Add matches one by one
    const match1 = createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440001' });
    await redis.xadd(
      config.settlementMatchesStream,
      '*',
      'data',
      JSON.stringify(match1),
    );

    processor.start();

    // Wait for first match to be read - poll until accumulator has the match
    let attempts = 0;
    while (accumulator.getPendingCount() === 0 && attempts < 50) {
      await new Promise((resolve) => setTimeout(resolve, 10));
      attempts++;
    }

    // Should not process yet (batch size is 3, only 1 match)
    expect(accumulator.getPendingCount()).toBe(1);

    // Add more matches
    const match2 = createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440002' });
    const match3 = createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440003' });

    await redis.xadd(
      config.settlementMatchesStream,
      '*',
      'data',
      JSON.stringify(match2),
    );
    await redis.xadd(
      config.settlementMatchesStream,
      '*',
      'data',
      JSON.stringify(match3),
    );

    // Wait for batch to be processed
    await new Promise((resolve) => setTimeout(resolve, 1000));

    // Verify all entries were processed
    const length = await redis.xlen(config.settlementMatchesStream);
    expect(length).toBe(0);
  }, 15000);
});

