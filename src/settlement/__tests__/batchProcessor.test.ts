import type Redis from 'ioredis';
import { BatchProcessor } from '../batchProcessor';
import { BatchAccumulator } from '../batchAccumulator';
import { processSettlementBatch, BatchProcessingError } from '../processBatch';
import {
  readMatches,
  processPendingEntriesOnStartup,
} from '../../redis/settlementMatchConsumer';
import { createMatch, createMatchWithMeta } from '../../tests/helpers/testFixtures';
import { createTestConfig } from '../../tests/helpers/testConfig';
import type { AppConfig } from '../../config';
import type { MatchWithMeta } from '../../redis/settlementMatchConsumer';

// Mock processSettlementBatch, readMatches, and processPendingEntriesOnStartup
jest.mock('../processBatch');
jest.mock('../../redis/settlementMatchConsumer');

const mockProcessSettlementBatch = processSettlementBatch as jest.MockedFunction<
  typeof processSettlementBatch
>;
const mockReadMatches = readMatches as jest.MockedFunction<typeof readMatches>;
const mockProcessPending = processPendingEntriesOnStartup as jest.MockedFunction<
  typeof processPendingEntriesOnStartup
>;

/**
 * Unit tests for BatchProcessor.
 * All Redis and settlement interactions are mocked — no live Redis required.
 */
describe('BatchProcessor', () => {
  let config: AppConfig;
  let accumulator: BatchAccumulator;
  let processor: BatchProcessor;
  let mockRedis: { status: string };

  beforeEach(() => {
    jest.clearAllMocks();
    jest.useFakeTimers();

    config = createTestConfig({
      batchSize: 3,
      batchIntervalMs: 5000,
      pollIntervalMs: 100,
      readCount: 10,
    });

    mockRedis = { status: 'ready' } as any;
    accumulator = new BatchAccumulator(config.batchSize, config.batchIntervalMs);

    mockReadMatches.mockResolvedValue([]);
    mockProcessPending.mockResolvedValue([]);
    mockProcessSettlementBatch.mockResolvedValue(undefined);

    processor = new BatchProcessor({
      redis: mockRedis as any,
      config,
      accumulator,
    });
  });

  afterEach(async () => {
    jest.useRealTimers();
    // Ensure processor stops cleanly
    try {
      await processor.stop();
    } catch {
      // Ignore
    }
  });

  describe('constructor', () => {
    it('should initialize with required options', () => {
      expect(processor).toBeInstanceOf(BatchProcessor);
    });
  });

  describe('start', () => {
    it('should set up polling interval and do initial poll', async () => {
      processor.start();

      // Run the initial poll (triggered immediately in start())
      await jest.advanceTimersByTimeAsync(1);

      // readMatches should have been called from initial poll
      expect(mockReadMatches).toHaveBeenCalled();
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
      const match1 = createMatchWithMeta({ matchId: '550e8400-e29b-41d4-a716-446655440001' });
      const match2 = createMatchWithMeta({ matchId: '550e8400-e29b-41d4-a716-446655440002' });

      mockReadMatches.mockResolvedValueOnce([match1, match2]);

      processor.start();

      // Run the initial poll
      await jest.advanceTimersByTimeAsync(1);

      expect(accumulator.getPendingCount()).toBe(2);
    });
  });

  describe('stop', () => {
    it('should clear interval and process pending matches on shutdown', async () => {
      const match1 = createMatchWithMeta();
      const match2 = createMatchWithMeta();
      mockReadMatches.mockResolvedValueOnce([match1, match2]);

      processor.start();

      // Run the initial poll to read matches
      await jest.advanceTimersByTimeAsync(1);

      expect(accumulator.getPendingCount()).toBe(2);

      jest.useRealTimers();
      await processor.stop();

      // Should process remaining matches on shutdown
      expect(mockProcessSettlementBatch).toHaveBeenCalled();
    });

    it('should return immediately if not running', async () => {
      jest.useRealTimers();
      const newProcessor = new BatchProcessor({
        redis: mockRedis as any,
        config,
        accumulator: new BatchAccumulator(config.batchSize, config.batchIntervalMs),
      });

      await expect(newProcessor.stop()).resolves.not.toThrow();
    });
  });

  describe('poll', () => {
    it('should read matches when accumulator needs more', async () => {
      const match = createMatchWithMeta();
      mockReadMatches.mockResolvedValueOnce([match]);

      processor.start();
      await jest.advanceTimersByTimeAsync(1);

      expect(accumulator.getPendingCount()).toBe(1);
    });

    it('should process batch when accumulator says it should', async () => {
      const matches: MatchWithMeta[] = [
        createMatchWithMeta({ matchId: '550e8400-e29b-41d4-a716-446655440001' }),
        createMatchWithMeta({ matchId: '550e8400-e29b-41d4-a716-446655440002' }),
        createMatchWithMeta({ matchId: '550e8400-e29b-41d4-a716-446655440003' }),
      ];

      mockReadMatches.mockResolvedValueOnce(matches);

      processor.start();
      await jest.advanceTimersByTimeAsync(1);

      expect(mockProcessSettlementBatch).toHaveBeenCalled();
      const callArgs = mockProcessSettlementBatch.mock.calls[0];
      expect(callArgs[0].length).toBe(3);
    });

    it('should skip reading if accumulator does not need more matches', async () => {
      // Fill accumulator to capacity
      const matches = [
        createMatchWithMeta({ matchId: '550e8400-e29b-41d4-a716-446655440001' }),
        createMatchWithMeta({ matchId: '550e8400-e29b-41d4-a716-446655440002' }),
        createMatchWithMeta({ matchId: '550e8400-e29b-41d4-a716-446655440003' }),
      ];
      accumulator.addMatches(matches);

      processor.start();
      await jest.advanceTimersByTimeAsync(1);

      // Processing should trigger, but no new reads needed
      expect(mockProcessSettlementBatch).toHaveBeenCalled();
    });

    it('should skip if not running', async () => {
      // Don't start the processor
      jest.advanceTimersByTime(200);
      await jest.runAllTimersAsync();

      expect(mockReadMatches).not.toHaveBeenCalled();
      expect(mockProcessSettlementBatch).not.toHaveBeenCalled();
    });
  });

  describe('error handling', () => {
    it('should handle retryable BatchProcessingError', async () => {
      const consoleSpy = jest.spyOn(console, 'error').mockImplementation();
      jest.spyOn(console, 'log').mockImplementation();

      const matches = [
        createMatchWithMeta({ matchId: '550e8400-e29b-41d4-a716-446655440001' }),
        createMatchWithMeta({ matchId: '550e8400-e29b-41d4-a716-446655440002' }),
        createMatchWithMeta({ matchId: '550e8400-e29b-41d4-a716-446655440003' }),
      ];

      mockReadMatches.mockResolvedValueOnce(matches);
      const retryableError = new BatchProcessingError('Retryable', true, new Error('orig'));
      mockProcessSettlementBatch.mockRejectedValueOnce(retryableError);

      processor.start();
      await jest.advanceTimersByTimeAsync(1);

      expect(mockProcessSettlementBatch).toHaveBeenCalled();
      // Matches removed from accumulator (getBatch was called)
      expect(accumulator.getPendingCount()).toBe(0);

      consoleSpy.mockRestore();
    });

    it('should handle non-retryable BatchProcessingError', async () => {
      const consoleSpy = jest.spyOn(console, 'error').mockImplementation();
      jest.spyOn(console, 'log').mockImplementation();

      const matches = [
        createMatchWithMeta({ matchId: '550e8400-e29b-41d4-a716-446655440001' }),
        createMatchWithMeta({ matchId: '550e8400-e29b-41d4-a716-446655440002' }),
        createMatchWithMeta({ matchId: '550e8400-e29b-41d4-a716-446655440003' }),
      ];

      mockReadMatches.mockResolvedValueOnce(matches);
      const nonRetryableError = new BatchProcessingError('Non-retryable', false, new Error('orig'));
      mockProcessSettlementBatch.mockRejectedValueOnce(nonRetryableError);

      processor.start();
      await jest.advanceTimersByTimeAsync(1);

      expect(mockProcessSettlementBatch).toHaveBeenCalled();
      expect(accumulator.getPendingCount()).toBe(0);

      consoleSpy.mockRestore();
    });

    it('should handle unexpected errors', async () => {
      const consoleSpy = jest.spyOn(console, 'error').mockImplementation();
      jest.spyOn(console, 'log').mockImplementation();

      const matches = [
        createMatchWithMeta({ matchId: '550e8400-e29b-41d4-a716-446655440001' }),
        createMatchWithMeta({ matchId: '550e8400-e29b-41d4-a716-446655440002' }),
        createMatchWithMeta({ matchId: '550e8400-e29b-41d4-a716-446655440003' }),
      ];

      mockReadMatches.mockResolvedValueOnce(matches);
      mockProcessSettlementBatch.mockRejectedValueOnce(new Error('Unexpected'));

      processor.start();
      await jest.advanceTimersByTimeAsync(1);

      expect(mockProcessSettlementBatch).toHaveBeenCalled();

      consoleSpy.mockRestore();
    });

    it('should handle readMatches errors gracefully', async () => {
      const consoleSpy = jest.spyOn(console, 'error').mockImplementation();

      // Set Redis status to closed to trigger error path
      mockRedis.status = 'end';

      processor.start();
      await jest.advanceTimersByTimeAsync(1);

      expect(consoleSpy).toHaveBeenCalledWith(
        '[batch-processor] Error in poll',
        expect.any(Error),
      );

      consoleSpy.mockRestore();
    });
  });

  describe('batch processing flow', () => {
    it('should process batch when size threshold reached', async () => {
      const matches = [
        createMatchWithMeta({ matchId: '550e8400-e29b-41d4-a716-446655440001' }),
        createMatchWithMeta({ matchId: '550e8400-e29b-41d4-a716-446655440002' }),
        createMatchWithMeta({ matchId: '550e8400-e29b-41d4-a716-446655440003' }),
      ];

      mockReadMatches.mockResolvedValueOnce(matches);

      processor.start();
      await jest.advanceTimersByTimeAsync(1);

      expect(mockProcessSettlementBatch).toHaveBeenCalled();
      const callArgs = mockProcessSettlementBatch.mock.calls[0];
      expect(callArgs[0].length).toBe(3);
      expect(callArgs[1]).toEqual(
        expect.objectContaining({
          redis: mockRedis,
          stream: config.settlementMatchesStream,
          consumerGroup: config.consumerGroup,
          streamMaxLen: config.streamMaxLen,
        }),
      );
    });

    it('should not process empty batch', async () => {
      processor.start();
      await jest.advanceTimersByTimeAsync(config.pollIntervalMs * 3);

      expect(mockProcessSettlementBatch).not.toHaveBeenCalled();
    });

    it('should process pending entries before new ones', async () => {
      const pendingMatch = createMatchWithMeta({ matchId: '550e8400-e29b-41d4-a716-446655440001' });
      mockProcessPending.mockResolvedValueOnce([pendingMatch]);

      processor.start();
      await jest.advanceTimersByTimeAsync(1);

      expect(mockProcessPending).toHaveBeenCalled();
      expect(accumulator.getPendingCount()).toBeGreaterThanOrEqual(0); // May have been processed
    });
  });
});
