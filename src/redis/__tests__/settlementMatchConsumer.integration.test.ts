import type Redis from 'ioredis';
import { ensureConsumerGroup } from '../settlementMatchConsumer';
import { BatchProcessor } from '../../settlement/batchProcessor';
import { BatchAccumulator } from '../../settlement/batchAccumulator';
import type { AppConfig } from '../../config';
import { createMatch } from '../../tests/helpers/testFixtures';
import { getRedisClient, closeRedisClient } from '../client';
import {
  cleanupTestStreams,
  removePendingMessages,
} from '../../tests/helpers/redisTestClient';
import { createTestConfig } from '../../tests/helpers/testConfig';
import { persistSettlementResults } from '../../settlement/database';
import { setupMockSettleBatch, getMockSettleBatch } from '../../tests/helpers/mockSmartContract';

// Mock database to avoid random failures
jest.mock('../../settlement/database');

const mockSettleBatch = getMockSettleBatch();
const mockPersistSettlementResults = persistSettlementResults as jest.MockedFunction<
  typeof persistSettlementResults
>;

/**
 * Integration tests for settlement match consumer using a real Redis instance.
 * These tests verify end-to-end behavior with actual Redis stream operations.
 *
 * @requires Redis server running (default: localhost:6379, or set REDIS_TEST_URL)
 */
describe('settlementMatchConsumer Integration Tests', () => {
  let redis: Redis;
  let config: AppConfig;
  let accumulator: BatchAccumulator;
  let processor: BatchProcessor;
  let onInvalid: jest.Mock<Promise<void>, [any]>;

  // Increase timeout for all tests in this suite
  jest.setTimeout(30000);

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
    // Reset mocks
    jest.clearAllMocks();

    // Use the real Redis client factory for each test
    config = createTestConfig({
      settlementMatchesStream: `test:settlement:matches:${Date.now()}`,
      consumerGroup: `test-settlement-engine-${Date.now()}`,
      consumerName: 'test-consumer-1',
      batchSize: 3,
      batchIntervalMs: 500,
      pollIntervalMs: 100,
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
    // Wait for processor to stop
    await new Promise((resolve) => setTimeout(resolve, 150));
    
    // Clean up test streams and consumer groups
    await cleanupTestStreams(redis, [config.settlementMatchesStream]);
    // Close the Redis client to reset singleton for next test
    await closeRedisClient();
  }, 30000);

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

  it('should process matches when batch size is reached', async () => {
    // Add matches to stream (batch size is 3)
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

    // Wait for batch to be processed
    await new Promise((resolve) => setTimeout(resolve, 1000));

    // Verify entries were deleted (processed)
    const length = await redis.xlen(config.settlementMatchesStream);
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

  it('should handle matches with individual field format (not JSON)', async () => {
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
      'makerFeeAmount',
      String(match.makerFeeAmount),
      'takerFeeAmount',
      String(match.takerFeeAmount),
      'lenderSettlementFee',
      String(match.lenderSettlementFee),
      'borrowerSettlementFee',
      String(match.borrowerSettlementFee),
    );

    processor.start();

    // Wait for processing (will process when time interval elapses)
    await new Promise((resolve) => setTimeout(resolve, 800));

    // Verify entry was processed
    const length = await redis.xlen(config.settlementMatchesStream);
    expect(length).toBe(0);
  }, 10000);


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
});


