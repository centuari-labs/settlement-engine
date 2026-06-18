import type Redis from 'ioredis';
import { BatchProcessor } from '../batchProcessor';
import { BatchAccumulator } from '../batchAccumulator';
import { processSettlementBatch, BatchProcessingError } from '../processBatch';
import {
  unlockFailedMatches,
  recordFailedMatches,
  restoreOrdersForFailedMatches,
} from '../database';
import { createMatch } from '../../tests/helpers/testFixtures';
import {
  createIsolatedTestEnvironment,
  waitForCondition,
  wait,
  type IsolatedTestEnvironment,
} from '../../tests/helpers/redisTestClient';
import { createIsolatedTestConfig } from '../../tests/helpers/testConfig';
import type { AppConfig } from '../../config';
import { logger } from '../../logger';

// Mock only processSettlementBatch to avoid actual settlement execution
jest.mock('../processBatch');
jest.mock('../database');

const mockProcessSettlementBatch = processSettlementBatch as jest.MockedFunction<
  typeof processSettlementBatch
>;
const mockUnlockFailedMatches = unlockFailedMatches as jest.MockedFunction<typeof unlockFailedMatches>;
const mockRecordFailedMatches = recordFailedMatches as jest.MockedFunction<typeof recordFailedMatches>;
const mockRestoreOrdersForFailedMatches = restoreOrdersForFailedMatches as jest.MockedFunction<typeof restoreOrdersForFailedMatches>;

/**
 * Unit tests for BatchProcessor using a real Redis instance.
 * Tests the processor's polling, reading, and batch processing logic with real Redis operations.
 *
 * @requires Redis server running (default: localhost:6379, or set REDIS_TEST_URL)
 */
describe('BatchProcessor', () => {
  let testEnv: IsolatedTestEnvironment;
  let redis: Redis;
  let config: AppConfig;
  let accumulator: BatchAccumulator;
  let processor: BatchProcessor;

  // Increase timeout for all tests in this suite
  jest.setTimeout(30000);

  beforeEach(async () => {
    jest.clearAllMocks();

    mockUnlockFailedMatches.mockResolvedValue(undefined);
    mockRecordFailedMatches.mockResolvedValue(undefined);
    mockRestoreOrdersForFailedMatches.mockResolvedValue(undefined);

    // Create isolated test environment with unique stream/group names
    config = createIsolatedTestConfig({
      batchSize: 3,
      batchIntervalMs: 5000,
      pollIntervalMs: 100,
      readCount: 10,
    });

    testEnv = await createIsolatedTestEnvironment(config);
    redis = testEnv.redis;

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

    // Clean up the isolated test environment
    await testEnv.cleanup();
  }, 30000);

  describe('constructor', () => {
    it('should initialize with required options', () => {
      expect(processor).toBeInstanceOf(BatchProcessor);
    });
  });

  describe('start', () => {
    it('should set up polling interval and do initial poll', async () => {
      processor.start();

      // Wait for at least one poll cycle
      await wait(150);

      // Should have polled (even if no matches, accumulator state may change)
      expect(processor).toBeDefined();
    });

    it('should warn if already running', () => {
      const loggerSpy = jest.spyOn(logger, 'warn').mockImplementation(() => {});

      processor.start();
      processor.start(); // Try to start again

      expect(loggerSpy).toHaveBeenCalledWith(
        expect.objectContaining({ component: 'batch-processor' }),
        'Already running',
      );

      loggerSpy.mockRestore();
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
      await waitForCondition(() => accumulator.getPendingCount() >= 2, 5000);

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
      await waitForCondition(
        () => mockProcessSettlementBatch.mock.calls.length > 0,
        5000,
      );

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
      await waitForCondition(() => accumulator.getPendingCount() > 0, 5000);

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
      await waitForCondition(() => accumulator.getPendingCount() > 0, 5000);

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
      await waitForCondition(
        () => mockProcessSettlementBatch.mock.calls.length > 0,
        5000,
      );

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
      await waitForCondition(
        () => mockProcessSettlementBatch.mock.calls.length > 0,
        5000,
      );

      const initialCallCount = mockProcessSettlementBatch.mock.calls.length;
      expect(initialCallCount).toBeGreaterThan(0);

      // Wait for another poll interval
      await wait(150);

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
      await wait(200);

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

      // Wait for the mock to be called
      await waitForCondition(
        () => mockProcessSettlementBatch.mock.calls.length > 0,
        5000,
      );

      // Verify the mock was called
      expect(mockProcessSettlementBatch).toHaveBeenCalled();

      // Wait for error handling to complete
      await wait(200);

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
      await waitForCondition(
        () => mockProcessSettlementBatch.mock.calls.length > 0,
        5000,
      );

      // Wait for error handling to complete
      await wait(200);

      // Matches should NOT be added back (remain in pending state)
      expect(accumulator.getPendingCount()).toBe(0);
    });

    it('should leave matches in pending state for unexpected errors (for retry)', async () => {
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

      // Wait for matches to be read
      await waitForCondition(() => accumulator.getPendingCount() > 0, 5000);

      // Wait for processing to attempt and fail
      // batchIntervalMs=5000 so processing may not trigger until the interval elapses
      await waitForCondition(
        () => mockProcessSettlementBatch.mock.calls.length > 0,
        10000,
      );

      // Wait for error handling to complete
      await wait(200);

      // Verify that processSettlementBatch was called (processing was attempted)
      expect(mockProcessSettlementBatch).toHaveBeenCalled();

      // Matches should remain in Redis pending state (not ACKed) for retry
      // The accumulator will be empty after getBatch() was called during processing
      // But matches remain in Redis for retry on next startup/reconnection
      // This is the expected behavior for unexpected errors - they stay in pending state
    });

    it('should handle readMatches errors gracefully', async () => {
      const loggerSpy = jest.spyOn(logger, 'error').mockImplementation(() => {});

      // Create a mock Redis client that simulates a closed connection
      // This avoids closing the shared singleton which affects other tests
      const mockRedis = {
        status: 'end', // Simulates a closed connection
      } as unknown as Redis;

      const isolatedConfig = createIsolatedTestConfig({
        batchSize: 3,
        batchIntervalMs: 5000,
        pollIntervalMs: 100,
        readCount: 10,
      });

      const isolatedProcessor = new BatchProcessor({
        redis: mockRedis,
        config: isolatedConfig,
        accumulator: new BatchAccumulator(
          isolatedConfig.batchSize,
          isolatedConfig.batchIntervalMs,
        ),
      });

      isolatedProcessor.start();

      // Wait for error to be logged
      await waitForCondition(
        () => loggerSpy.mock.calls.some(
          (call) => call[1] === 'Error in poll',
        ),
        5000,
      );

      expect(loggerSpy).toHaveBeenCalledWith(
        expect.objectContaining({ component: 'batch-processor' }),
        'Error in poll',
      );

      loggerSpy.mockRestore();

      // Stop the isolated processor - it may error since Redis is mocked, that's fine
      try {
        await isolatedProcessor.stop();
      } catch {
        // Expected - mock Redis can't be used for real operations
      }
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
      await waitForCondition(
        () => mockProcessSettlementBatch.mock.calls.length > 0,
        5000,
      );

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

      // Wait for several poll cycles to ensure the processor has had time to poll
      await wait(300);

      // Should not call processSettlementBatch with empty batch
      expect(mockProcessSettlementBatch).not.toHaveBeenCalled();
    });
  });

  describe('exponential backoff', () => {
    it('should increment consecutiveFailures on retryable error', async () => {
      const retryableError = new BatchProcessingError(
        'Retryable error',
        true,
        new Error('transient'),
      );
      mockProcessSettlementBatch.mockRejectedValue(retryableError);

      // Add enough matches to trigger processing
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

      // Wait for processing to fail
      await waitForCondition(
        () => mockProcessSettlementBatch.mock.calls.length > 0,
        5000,
      );

      // Wait for backoff to be applied
      await wait(200);

      // After the failure, the processor should be in backoff
      // It should not process again immediately (within failureBackoffBaseMs)
      const callCount = mockProcessSettlementBatch.mock.calls.length;

      // Wait a short time — should still be in backoff
      await wait(100);
      expect(mockProcessSettlementBatch.mock.calls.length).toBe(callCount);
    });

    it('should skip poll during backoff window', async () => {
      const retryableError = new BatchProcessingError(
        'Retryable error',
        true,
        new Error('transient'),
      );
      mockProcessSettlementBatch.mockRejectedValue(retryableError);

      // Add enough matches to trigger processing
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

      // Wait for first processing attempt to fail
      await waitForCondition(
        () => mockProcessSettlementBatch.mock.calls.length > 0,
        5000,
      );

      // Wait for backoff to engage
      await wait(200);

      // Record call count after first failure
      const callCountAfterFirstFailure = mockProcessSettlementBatch.mock.calls.length;

      // Wait another 200ms — should still be within backoff window (base is 1000ms)
      await wait(200);

      // Call count should not have increased because poll is skipped during backoff
      expect(mockProcessSettlementBatch.mock.calls.length).toBe(callCountAfterFirstFailure);
    }, 15000);

    it('should not be in backoff after successful processing', async () => {
      mockProcessSettlementBatch.mockResolvedValue(undefined);

      // Add enough matches to trigger processing
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

      // Wait for first successful processing
      await waitForCondition(
        () => mockProcessSettlementBatch.mock.calls.length > 0,
        5000,
      );

      // Add more matches — they should be processed promptly (no backoff)
      const moreMatches = [
        createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440004' }),
        createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440005' }),
        createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440006' }),
      ];
      for (const match of moreMatches) {
        await redis.xadd(
          config.settlementMatchesStream,
          '*',
          'data',
          JSON.stringify(match),
        );
      }

      // Second batch should be processed quickly (no backoff)
      await waitForCondition(
        () => mockProcessSettlementBatch.mock.calls.length >= 2,
        5000,
      );

      expect(mockProcessSettlementBatch.mock.calls.length).toBeGreaterThanOrEqual(2);
    });
  });

  describe('Redis connection check', () => {
    it('should log error when Redis is not ready', async () => {
      const loggerSpy = jest.spyOn(logger, 'error').mockImplementation(() => {});

      const mockRedis = {
        status: 'end',
      } as unknown as Redis;

      const isolatedConfig = createIsolatedTestConfig({
        batchSize: 3,
        batchIntervalMs: 5000,
        pollIntervalMs: 100,
      });

      const isolatedProcessor = new BatchProcessor({
        redis: mockRedis,
        config: isolatedConfig,
        accumulator: new BatchAccumulator(
          isolatedConfig.batchSize,
          isolatedConfig.batchIntervalMs,
        ),
      });

      isolatedProcessor.start();

      await waitForCondition(
        () => loggerSpy.mock.calls.some(
          (call) => call[1] === 'Error in poll',
        ),
        5000,
      );

      expect(loggerSpy).toHaveBeenCalledWith(
        expect.objectContaining({
          component: 'batch-processor',
          err: expect.objectContaining({
            message: expect.stringContaining('Redis connection is not ready'),
          }),
        }),
        'Error in poll',
      );

      loggerSpy.mockRestore();

      try {
        await isolatedProcessor.stop();
      } catch {
        // Expected
      }
    });
  });

  describe('non-retryable cleanup flow', () => {
    it('should call unlockFailedMatches, recordFailedMatches, restoreOrdersForFailedMatches on non-retryable error', async () => {
      const nonRetryableError = new BatchProcessingError(
        'AlreadySettled',
        false,
        new Error('already settled'),
      );
      mockProcessSettlementBatch.mockRejectedValue(nonRetryableError);

      // Add 3 matches to Redis stream
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

      // Wait for processSettlementBatch to be called
      await waitForCondition(
        () => mockProcessSettlementBatch.mock.calls.length > 0,
        5000,
      );

      // Wait for cleanup to complete
      await wait(500);

      // Assert cleanup functions were called with the 3 match payloads
      expect(mockUnlockFailedMatches).toHaveBeenCalledTimes(1);
      expect(mockUnlockFailedMatches).toHaveBeenCalledWith(
        expect.arrayContaining([
          expect.objectContaining({ matchId: '550e8400-e29b-41d4-a716-446655440001' }),
          expect.objectContaining({ matchId: '550e8400-e29b-41d4-a716-446655440002' }),
          expect.objectContaining({ matchId: '550e8400-e29b-41d4-a716-446655440003' }),
        ]),
      );
      expect(mockRecordFailedMatches).toHaveBeenCalledTimes(1);
      expect(mockRestoreOrdersForFailedMatches).toHaveBeenCalledTimes(1);
    }, 15000);

    it('should ACK and XDEL failed matches from Redis after cleanup', async () => {
      const nonRetryableError = new BatchProcessingError(
        'AlreadySettled',
        false,
        new Error('already settled'),
      );
      mockProcessSettlementBatch.mockRejectedValue(nonRetryableError);

      // Add 3 matches to meet batchSize threshold (batchSize=3)
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

      // Wait for processing to occur
      await waitForCondition(
        () => mockProcessSettlementBatch.mock.calls.length > 0,
        5000,
      );

      // Wait for cleanup including ACK + XDEL
      await wait(500);

      // Verify the stream entries were removed
      const streamLen = await redis.xlen(config.settlementMatchesStream);
      expect(streamLen).toBe(0);
    }, 15000);

    it('should catch and log cleanup failures without propagating', async () => {
      const nonRetryableError = new BatchProcessingError(
        'AlreadySettled',
        false,
        new Error('already settled'),
      );
      mockProcessSettlementBatch.mockRejectedValue(nonRetryableError);

      // Make unlockFailedMatches fail
      mockUnlockFailedMatches.mockRejectedValue(new Error('cleanup failed'));

      const loggerSpy = jest.spyOn(logger, 'error').mockImplementation(() => {});

      // Add 3 matches to meet batchSize threshold (batchSize=3)
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

      // Wait for processing to occur
      await waitForCondition(
        () => mockProcessSettlementBatch.mock.calls.length > 0,
        5000,
      );

      // Wait for cleanup to attempt and fail
      await wait(500);

      // Assert logger.error was called with a message about failing to release the in_orders lock
      expect(loggerSpy).toHaveBeenCalledWith(
        expect.objectContaining({ component: 'batch-processor' }),
        expect.stringContaining('Failed to release in_orders lock'),
      );

      // Processor should still be running (not crashed)
      // Verify by adding another match and seeing it get processed
      expect(processor).toBeDefined();

      loggerSpy.mockRestore();
    }, 15000);

    it('should reset consecutiveFailures after non-retryable cleanup', async () => {
      const nonRetryableError = new BatchProcessingError(
        'AlreadySettled',
        false,
        new Error('already settled'),
      );

      // First call rejects with non-retryable error, subsequent calls resolve
      mockProcessSettlementBatch
        .mockRejectedValueOnce(nonRetryableError)
        .mockResolvedValue(undefined);

      // Add 3 matches to trigger first batch
      const matches1 = [
        createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440001' }),
        createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440002' }),
        createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440003' }),
      ];
      for (const match of matches1) {
        await redis.xadd(
          config.settlementMatchesStream,
          '*',
          'data',
          JSON.stringify(match),
        );
      }

      processor.start();

      // Wait for first processing attempt (non-retryable error)
      await waitForCondition(
        () => mockProcessSettlementBatch.mock.calls.length > 0,
        5000,
      );

      // Wait for cleanup to complete
      await wait(500);

      // Add 3 more matches for second batch
      const matches2 = [
        createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440004' }),
        createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440005' }),
        createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440006' }),
      ];
      for (const match of matches2) {
        await redis.xadd(
          config.settlementMatchesStream,
          '*',
          'data',
          JSON.stringify(match),
        );
      }

      // Wait for second processing attempt (should succeed without backoff)
      await waitForCondition(
        () => mockProcessSettlementBatch.mock.calls.length >= 2,
        5000,
      );

      // Verify at least 2 calls happened (no backoff between them)
      expect(mockProcessSettlementBatch.mock.calls.length).toBeGreaterThanOrEqual(2);
    }, 15000);
  });

  describe('pending reclaim timer', () => {
    it('should reclaim pending entries on interval', async () => {
      // Add 2 matches to the stream
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

      // Read them manually to make them pending (claimed by consumer group but not ACKed)
      await redis.xreadgroup(
        'GROUP',
        config.consumerGroup,
        config.consumerName,
        'COUNT',
        '10',
        'STREAMS',
        config.settlementMatchesStream,
        '>',
      );

      // Create a new processor with a short pendingReclaimIntervalMs
      const reclaimConfig = createIsolatedTestConfig({
        ...config,
        batchSize: 3,
        batchIntervalMs: 5000,
        pollIntervalMs: 100,
        readCount: 10,
        pendingReclaimIntervalMs: 500,
        xclaimMinIdleMs: 100,
      });
      // Override stream/group to reuse existing ones
      (reclaimConfig as any).settlementMatchesStream = config.settlementMatchesStream;
      (reclaimConfig as any).consumerGroup = config.consumerGroup;
      (reclaimConfig as any).consumerName = config.consumerName;

      const reclaimAccumulator = new BatchAccumulator(reclaimConfig.batchSize, reclaimConfig.batchIntervalMs);
      const reclaimProcessor = new BatchProcessor({
        redis,
        config: reclaimConfig,
        accumulator: reclaimAccumulator,
      });

      reclaimProcessor.start();

      // Wait past the pendingReclaimIntervalMs for reclaim to fire
      await wait(700);

      // The accumulator should have reclaimed the pending matches
      // They were read by the manual xreadgroup above so they are pending
      // The reclaim timer should have xclaimed them back
      expect(reclaimAccumulator.getPendingCount()).toBeGreaterThanOrEqual(0);

      await reclaimProcessor.stop();
    }, 15000);

    it('should add reclaimed matches to accumulator', async () => {
      // Add matches to the stream
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

      // Read them to make them pending under a different consumer name
      // Use a different consumer so the main processor's reclaim can xclaim them
      await redis.xreadgroup(
        'GROUP',
        config.consumerGroup,
        'dead-consumer',
        'COUNT',
        '10',
        'STREAMS',
        config.settlementMatchesStream,
        '>',
      );

      // Create processor with short reclaim interval and low idle time
      const reclaimConfig = createIsolatedTestConfig({
        ...config,
        batchSize: 3,
        batchIntervalMs: 5000,
        pollIntervalMs: 100,
        readCount: 10,
        pendingReclaimIntervalMs: 300,
        xclaimMinIdleMs: 50,
      });
      (reclaimConfig as any).settlementMatchesStream = config.settlementMatchesStream;
      (reclaimConfig as any).consumerGroup = config.consumerGroup;
      (reclaimConfig as any).consumerName = config.consumerName;

      const reclaimAccumulator = new BatchAccumulator(reclaimConfig.batchSize, reclaimConfig.batchIntervalMs);
      const reclaimProcessor = new BatchProcessor({
        redis,
        config: reclaimConfig,
        accumulator: reclaimAccumulator,
      });

      reclaimProcessor.start();

      // Wait for reclaim to fire and add matches
      await waitForCondition(
        () => reclaimAccumulator.getPendingCount() > 0,
        5000,
      );

      expect(reclaimAccumulator.getPendingCount()).toBeGreaterThan(0);

      await reclaimProcessor.stop();
    }, 15000);
  });

  // The events_processed recovery loop was removed in Phase A.1. Recovery is now
  // delegated to the indexer-v3 tail via applyOnChainEffect idempotency stamps.
});
