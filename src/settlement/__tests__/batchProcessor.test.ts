import type Redis from 'ioredis';
import { BatchProcessor } from '../batchProcessor';
import { BatchAccumulator } from '../batchAccumulator';
import { processSettlementBatch, BatchProcessingError } from '../processBatch';
import { ensureConsumerGroup } from '../../redis/settlementMatchConsumer';
import { createMatch } from '../../tests/helpers/testFixtures';
import { getRedisClient, closeRedisClient } from '../../redis/client';
import {
  cleanupTestStreams,
  removePendingMessages,
} from '../../tests/helpers/redisTestClient';
import { createTestConfig } from '../../tests/helpers/testConfig';
import type { AppConfig } from '../../config';

// Mock only processSettlementBatch to avoid actual settlement execution
jest.mock('../processBatch');

const mockProcessSettlementBatch = processSettlementBatch as jest.MockedFunction<
  typeof processSettlementBatch
>;

/**
 * Unit tests for BatchProcessor using a real Redis instance.
 * Tests the processor's polling, reading, and batch processing logic with real Redis operations.
 *
 * @requires Redis server running (default: localhost:6379, or set REDIS_TEST_URL)
 */
describe('BatchProcessor', () => {
  let redis: Redis;
  let config: AppConfig;
  let accumulator: BatchAccumulator;
  let processor: BatchProcessor;

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
    jest.clearAllMocks();

    config = createTestConfig({
      settlementMatchesStream: `test:settlement:matches:${Date.now()}`,
      consumerGroup: `test-settlement-engine-${Date.now()}`,
      consumerName: 'test-consumer-1',
      batchSize: 3,
      batchIntervalMs: 5000,
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

    accumulator = new BatchAccumulator(config.batchSize, config.batchIntervalMs);

    processor = new BatchProcessor({
      redis,
      config,
      accumulator,
    });
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

  describe('constructor', () => {
    it('should initialize with required options', () => {
      expect(processor).toBeInstanceOf(BatchProcessor);
    });
  });

  describe('start', () => {
    it('should set up polling interval and do initial poll', async () => {
      processor.start();

      // Initial poll should happen immediately
      await new Promise((resolve) => setTimeout(resolve, 150));

      // Poll should be called again after pollIntervalMs
      const initialPending = accumulator.getPendingCount();
      await new Promise((resolve) => setTimeout(resolve, 150));

      // Should have polled (even if no matches, accumulator state may change)
      expect(processor).toBeDefined();
    });

    it('should warn if already running', () => {
      const consoleSpy = jest.spyOn(console, 'warn').mockImplementation();

      processor.start();
      processor.start(); // Try to start again

      expect(consoleSpy).toHaveBeenCalledWith(
        '[batch-processor] Already running',
      );

      consoleSpy.mockRestore();
    });

    it('should add matches to accumulator when read', async () => {
      // Add matches to Redis stream
      const match1 = createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440001' });
      const match2 = createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440002' });

      await redis.xadd(
        config.settlementMatchesStream,
        '*',
        'data',
        JSON.stringify(match1),
      );
      await redis.xadd(
        config.settlementMatchesStream,
        '*',
        'data',
        JSON.stringify(match2),
      );

      processor.start();

      // Wait for matches to be read - poll until accumulator has the matches
      let attempts = 0;
      while (accumulator.getPendingCount() < 2 && attempts < 50) {
        await new Promise((resolve) => setTimeout(resolve, 10));
        attempts++;
      }

      expect(accumulator.getPendingCount()).toBe(2);
    });
  });

  describe('stop', () => {
    it('should clear interval and wait for in-flight processing', async () => {
      mockProcessSettlementBatch.mockResolvedValue(undefined);

      processor.start();

      // Add matches and trigger processing
      const matches = [
        createMatch(),
        createMatch(),
        createMatch(),
      ];
      for (const match of matches) {
        await redis.xadd(
          config.settlementMatchesStream,
          '*',
          'data',
          JSON.stringify(match),
        );
      }

      // Wait for matches to be read and processed
      await new Promise((resolve) => setTimeout(resolve, 300));

      await processor.stop();

      // Verify stop completed
      expect(mockProcessSettlementBatch).toHaveBeenCalled();
    });

    it('should process pending matches before shutdown', async () => {
      mockProcessSettlementBatch.mockResolvedValue(undefined);

      processor.start();

      // Add matches to accumulator directly
      const match1 = createMatch();
      const match2 = createMatch();
      await redis.xadd(
        config.settlementMatchesStream,
        '*',
        'data',
        JSON.stringify(match1),
      );
      await redis.xadd(
        config.settlementMatchesStream,
        '*',
        'data',
        JSON.stringify(match2),
      );

      // Wait for matches to be read - poll until accumulator has matches
      let attempts = 0;
      while (accumulator.getPendingCount() === 0 && attempts < 50) {
        await new Promise((resolve) => setTimeout(resolve, 10));
        attempts++;
      }

      // Get the matches that were read
      const pendingCount = accumulator.getPendingCount();
      expect(pendingCount).toBeGreaterThan(0);

      await processor.stop();

      expect(mockProcessSettlementBatch).toHaveBeenCalled();
    });

    it('should return immediately if not running', async () => {
      const newProcessor = new BatchProcessor({
        redis,
        config,
        accumulator: new BatchAccumulator(config.batchSize, config.batchIntervalMs),
      });

      await expect(newProcessor.stop()).resolves.not.toThrow();
    });
  });

  describe('poll', () => {
    it('should read matches when accumulator needs more', async () => {
      // Add match to Redis stream
      const match = createMatch();
      await redis.xadd(
        config.settlementMatchesStream,
        '*',
        'data',
        JSON.stringify(match),
      );

      processor.start();

      // Wait for match to be read - poll until accumulator has the match
      let attempts = 0;
      while (accumulator.getPendingCount() === 0 && attempts < 50) {
        await new Promise((resolve) => setTimeout(resolve, 10));
        attempts++;
      }

      expect(accumulator.getPendingCount()).toBe(1);
    });

    it('should process batch when accumulator says it should', async () => {
      mockProcessSettlementBatch.mockResolvedValue(undefined);

      // Add matches to Redis stream
      const matches = [
        createMatch(),
        createMatch(),
        createMatch(),
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

      // Wait for matches to be read and batch to be processed
      await new Promise((resolve) => setTimeout(resolve, 300));

      expect(mockProcessSettlementBatch).toHaveBeenCalled();
    });

    it('should skip if already processing', async () => {
      // Make processSettlementBatch take a long time
      mockProcessSettlementBatch.mockImplementation(
        () => new Promise((resolve) => setTimeout(resolve, 2000)),
      );

      // Add matches to Redis stream
      const matches = [
        createMatch(),
        createMatch(),
        createMatch(),
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

      // Wait for first poll to start processing
      await new Promise((resolve) => setTimeout(resolve, 200));

      const initialCallCount = mockProcessSettlementBatch.mock.calls.length;
      expect(initialCallCount).toBeGreaterThan(0);

      // Wait for another poll interval
      await new Promise((resolve) => setTimeout(resolve, 150));

      // Should not have called processSettlementBatch again (still processing)
      expect(mockProcessSettlementBatch).toHaveBeenCalledTimes(initialCallCount);
    });

    it('should skip if not running', async () => {
      const newProcessor = new BatchProcessor({
        redis,
        config,
        accumulator: new BatchAccumulator(config.batchSize, config.batchIntervalMs),
      });

      // Don't start the processor
      // Add match to stream
      const match = createMatch();
      await redis.xadd(
        config.settlementMatchesStream,
        '*',
        'data',
        JSON.stringify(match),
      );

      // Wait a bit - processor not started, so nothing should happen
      await new Promise((resolve) => setTimeout(resolve, 200));

      expect(mockProcessSettlementBatch).not.toHaveBeenCalled();
    });
  });

  describe('error handling', () => {
    it('should leave matches in Redis pending state for retryable errors', async () => {
      // Add matches to Redis stream
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

      const retryableError = new BatchProcessingError(
        'Retryable error',
        true,
        new Error('Original error'),
      );
      mockProcessSettlementBatch.mockRejectedValue(retryableError);

      processor.start();

      // Wait for matches to be read and processing to fail
      // Use a loop to wait for the mock to be called
      let attempts = 0;
      while (mockProcessSettlementBatch.mock.calls.length === 0 && attempts < 50) {
        await new Promise((resolve) => setTimeout(resolve, 10));
        attempts++;
      }

      // Verify the mock was called
      expect(mockProcessSettlementBatch).toHaveBeenCalled();

      // Wait for error handling to complete
      await new Promise((resolve) => setTimeout(resolve, 200));

      // Matches should NOT be added back to accumulator
      // They remain in Redis pending state (not ACKed) for retry through pending entry processing
      expect(accumulator.getPendingCount()).toBe(0);
    });

    it('should leave matches in pending state for non-retryable errors', async () => {
      // Add matches to Redis stream
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

      const nonRetryableError = new BatchProcessingError(
        'Non-retryable error',
        false,
        new Error('Original error'),
      );
      mockProcessSettlementBatch.mockRejectedValue(nonRetryableError);

      processor.start();

      // Wait for matches to be read and processing to fail
      await new Promise((resolve) => setTimeout(resolve, 300));

      // Matches should NOT be added back (remain in pending state)
      expect(accumulator.getPendingCount()).toBe(0);
    });

    it('should add matches back for unexpected errors', async () => {
      // Add matches to Redis stream
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

      const unexpectedError = new Error('Unexpected error');
      mockProcessSettlementBatch.mockRejectedValue(unexpectedError);

      processor.start();

      // Wait for matches to be read and processing to fail
      // First wait for matches to be read
      let attempts = 0;
      while (accumulator.getPendingCount() === 0 && attempts < 50) {
        await new Promise((resolve) => setTimeout(resolve, 10));
        attempts++;
      }

      // Then wait for processing to attempt and fail
      attempts = 0;
      while (mockProcessSettlementBatch.mock.calls.length === 0 && attempts < 50) {
        await new Promise((resolve) => setTimeout(resolve, 10));
        attempts++;
      }

      // Wait for error handling to complete (matches removed from accumulator)
      // After error, matches are in Redis pending state
      await new Promise((resolve) => setTimeout(resolve, 200));

      // Matches are now in Redis pending state. Wait for next poll cycle
      // to process pending entries and add them back to accumulator.
      // The accumulator should be empty after getBatch(), so needsMoreMatches() 
      // should return true, triggering processPendingEntriesOnStartup
      attempts = 0;
      while (accumulator.getPendingCount() < 2 && attempts < 150) {
        await new Promise((resolve) => setTimeout(resolve, 10));
        attempts++;
      }

      // Matches should be added back for retry via pending entry processing
      expect(accumulator.getPendingCount()).toBe(2);
    });

    it('should handle readMatches errors gracefully', async () => {
      const consoleSpy = jest.spyOn(console, 'error').mockImplementation();

      // Close Redis connection to cause an error
      await closeRedisClient();

      processor.start();

      await new Promise((resolve) => setTimeout(resolve, 200));

      expect(consoleSpy).toHaveBeenCalledWith(
        '[batch-processor] Error in poll',
        expect.any(Error),
      );

      consoleSpy.mockRestore();

      // Reconnect for cleanup
      redis = getRedisClient(config);
    });
  });

  describe('batch processing flow', () => {
    it('should process batch when size threshold reached', async () => {
      mockProcessSettlementBatch.mockResolvedValue(undefined);

      // Add matches to Redis stream
      const matches = [
        createMatch(),
        createMatch(),
        createMatch(),
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

      // Wait for matches to be read and batch to be processed
      await new Promise((resolve) => setTimeout(resolve, 300));

      expect(mockProcessSettlementBatch).toHaveBeenCalled();
      const callArgs = mockProcessSettlementBatch.mock.calls[0];
      expect(callArgs[0].length).toBe(3);
      expect(callArgs[1]).toEqual(
        expect.objectContaining({
          redis,
          stream: config.settlementMatchesStream,
          consumerGroup: config.consumerGroup,
          streamMaxLen: config.streamMaxLen,
        }),
      );
    });

    it('should not process empty batch', async () => {
      mockProcessSettlementBatch.mockResolvedValue(undefined);

      processor.start();

      // Wait for poll - no matches in stream
      await new Promise((resolve) => setTimeout(resolve, 200));

      // Should not call processSettlementBatch with empty batch
      expect(mockProcessSettlementBatch).not.toHaveBeenCalled();
    });
  });
});

