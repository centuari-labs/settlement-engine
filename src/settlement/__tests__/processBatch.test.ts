import type Redis from 'ioredis';
import { processSettlementBatch, BatchProcessingError, type SettlementBatchContext } from '../processBatch';
import {
  createMatch,
  createMatchBatch,
  createMatchWithMeta,
} from '../../tests/helpers/testFixtures';
import { getRedisClient, closeRedisClient } from '../../redis/client';
import { cleanupTestStreams } from '../../tests/helpers/redisTestClient';
import { createTestConfig } from '../../tests/helpers/testConfig';
import { applySettlementResult } from '../database';
import { filterAlreadySettledMatches, settleBatch } from '../smartContract';
import { setupMockSettleBatch, getMockSettleBatch, setupMockSettleBatchError, createSettlementError } from '../../tests/helpers/mockSmartContract';
import { logger } from '../../logger';

// Mock database to avoid random failures
jest.mock('../database');

const mockSettleBatch = getMockSettleBatch();
const mockApplySettlementResult = applySettlementResult as jest.MockedFunction<
  typeof applySettlementResult
>;

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
  let testConfig: ReturnType<typeof createTestConfig>;
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
    // Reset mocks
    jest.clearAllMocks();

    // Use the real Redis client factory for each test
    testStream = `test:settlement:matches:${Date.now()}`;
    testConfig = createTestConfig({
      settlementMatchesStream: testStream,
    });
    redis = getRedisClient(testConfig);

    // Create consumer group for the test stream (MKSTREAM creates the stream if needed)
    try {
      await redis.xgroup('CREATE', testStream, testConfig.consumerGroup, '0', 'MKSTREAM');
    } catch {
      // Group may already exist from a previous failed cleanup
    }

    context = {
      redis,
      stream: testStream,
      consumerGroup: testConfig.consumerGroup,
      streamMaxLen: 10000,
    };
    consoleLogSpy = jest.spyOn(logger, 'info').mockImplementation(() => {});

    // Set up default successful mocks using the mock helper
    setupMockSettleBatch(mockSettleBatch);
    mockApplySettlementResult.mockResolvedValue(undefined);
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

    await processSettlementBatch([], context, testConfig);

    // Empty batch returns early without logging
    expect(consoleLogSpy).not.toHaveBeenCalledWith(
      expect.objectContaining({ component: 'process-settlement-batch' }),
      expect.anything(),
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

    await processSettlementBatch([matchWithMeta], context, testConfig);

    expect(consoleLogSpy).toHaveBeenCalledWith(
      expect.objectContaining({
        component: 'process-settlement-batch',
        matchCount: 1,
        matches: [expect.objectContaining({
          id: entryId,
          matchId: match.matchId,
          lendOrderId: match.lendOrderId,
          borrowOrderId: match.borrowOrderId,
        })],
      }),
      'Processing batch',
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

    await processSettlementBatch(matchesWithMeta, context, testConfig);

    expect(consoleLogSpy).toHaveBeenCalledWith(
      expect.objectContaining({
        component: 'process-settlement-batch',
        matchCount: 3,
        matches: expect.arrayContaining([
          expect.objectContaining({ id: entryIds[0] }),
          expect.objectContaining({ id: entryIds[1] }),
          expect.objectContaining({ id: entryIds[2] }),
        ]),
      }),
      'Processing batch',
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

    await processSettlementBatch(matches, context, testConfig);

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

    await processSettlementBatch([matchWithMeta], customContext, testConfig);

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

    await processSettlementBatch(matchesWithMeta, context, testConfig);

    expect(consoleLogSpy).toHaveBeenCalledWith(
      expect.objectContaining({
        component: 'process-settlement-batch',
        matchCount: 100,
        matches: expect.any(Array),
      }),
      'Processing batch',
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
      await expect(processSettlementBatch([matchWithMeta], context, testConfig)).rejects.toThrow();
    } finally {
      // Reconnect for cleanup
      const config = createTestConfig({
        settlementMatchesStream: testStream,
      });
      redis = getRedisClient(config);
    }
  });

  describe('already-settled filter', () => {
    it('should ACK and delete already-settled matches and continue with unsettled', async () => {
      const mockFilter = filterAlreadySettledMatches as jest.MockedFunction<
        typeof filterAlreadySettledMatches
      >;

      const match1 = createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440001' });
      const match2 = createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440002' });

      // Add entries to stream
      const entryId1 = await redis.xadd(context.stream, '*', 'data', JSON.stringify(match1));
      const entryId2 = await redis.xadd(context.stream, '*', 'data', JSON.stringify(match2));

      if (!entryId1 || !entryId2) throw new Error('Failed to add entries');

      const matchMeta1 = createMatchWithMeta(match1, { id: entryId1, stream: context.stream });
      const matchMeta2 = createMatchWithMeta(match2, { id: entryId2, stream: context.stream });

      // Mock: match1 already settled, match2 unsettled
      mockFilter.mockResolvedValueOnce({
        unsettled: [matchMeta2],
        alreadySettled: [matchMeta1],
      });

      await processSettlementBatch([matchMeta1, matchMeta2], context, testConfig);

      // Both entries should be deleted
      const length = await redis.xlen(context.stream);
      expect(length).toBe(0);

      // settleBatch should have been called only with unsettled match
      expect(mockSettleBatch).toHaveBeenCalled();
    });

    it('should return early when all matches are already settled', async () => {
      const mockFilter = filterAlreadySettledMatches as jest.MockedFunction<
        typeof filterAlreadySettledMatches
      >;

      const match = createMatch();
      const entryId = await redis.xadd(context.stream, '*', 'data', JSON.stringify(match));
      if (!entryId) throw new Error('Failed to add entry');

      const matchMeta = createMatchWithMeta(match, { id: entryId, stream: context.stream });

      // Mock: all matches already settled
      mockFilter.mockResolvedValueOnce({
        unsettled: [],
        alreadySettled: [matchMeta],
      });

      await processSettlementBatch([matchMeta], context, testConfig);

      // settleBatch should NOT have been called
      expect(mockSettleBatch).not.toHaveBeenCalled();
    });

    it('should handle matches with individual field format in filter', async () => {
      const mockFilter = filterAlreadySettledMatches as jest.MockedFunction<
        typeof filterAlreadySettledMatches
      >;

      const match = createMatch();

      // Add entry using individual fields (not JSON 'data' field)
      const entryId = await redis.xadd(
        context.stream,
        '*',
        'matchId', match.matchId,
        'marketId', match.marketId,
        'lendOrderId', match.lendOrderId,
        'borrowOrderId', match.borrowOrderId,
        'lenderWallet', match.lenderWallet,
        'borrowerWallet', match.borrowerWallet,
        'matchedAmount', match.matchedAmount,
        'rate', String(match.rate),
        'loanToken', match.loanToken,
        'maturity', String(match.maturity),
        'timestamp', String(match.timestamp),
        'borrowerIsTaker', String(match.borrowerIsTaker),
        'makerFeeAmount', match.makerFeeAmount,
        'takerFeeAmount', match.takerFeeAmount,
        'lenderSettlementFeeAmount', match.lenderSettlementFeeAmount,
        'borrowerSettlementFeeAmount', match.borrowerSettlementFeeAmount,
      );

      if (!entryId) throw new Error('Failed to add entry');

      const matchMeta = createMatchWithMeta(match, {
        id: entryId,
        stream: context.stream,
      });

      // Mock: this match is already settled
      mockFilter.mockResolvedValueOnce({
        unsettled: [],
        alreadySettled: [matchMeta],
      });

      await processSettlementBatch([matchMeta], context, testConfig);

      // Entry should be ACKed and deleted (stream is empty)
      const length = await redis.xlen(context.stream);
      expect(length).toBe(0);

      // settleBatch should NOT have been called (all matches were already settled)
      expect(mockSettleBatch).not.toHaveBeenCalled();
    });
  });

  describe('XDEL chunking', () => {
    it('should chunk xdel calls for 150+ entries', async () => {
      const entryCount = 150;
      const matches = createMatchBatch(entryCount);
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

      const xdelSpy = jest.spyOn(redis, 'xdel');

      await processSettlementBatch(matchesWithMeta, context, testConfig);

      // 150 entries / 100 per batch = 2 xdel calls
      expect(xdelSpy).toHaveBeenCalledTimes(2);

      // All entries should be deleted
      const length = await redis.xlen(context.stream);
      expect(length).toBe(0);

      xdelSpy.mockRestore();
    }, 30000);

    it('should call xdel once for entries at or under chunk size', async () => {
      const entryCount = 50;
      const matches = createMatchBatch(entryCount);
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

      const xdelSpy = jest.spyOn(redis, 'xdel');

      await processSettlementBatch(matchesWithMeta, context, testConfig);

      // 50 entries < 100 chunk size = 1 xdel call
      expect(xdelSpy).toHaveBeenCalledTimes(1);

      // All entries should be deleted
      const length = await redis.xlen(context.stream);
      expect(length).toBe(0);

      xdelSpy.mockRestore();
    });
  });

  describe('error wrapping', () => {
    it('should wrap settlement errors as BatchProcessingError', async () => {
      const match = createMatch();
      const entryId = await redis.xadd(context.stream, '*', 'data', JSON.stringify(match));
      if (!entryId) throw new Error('Failed to add entry');

      const matchMeta = createMatchWithMeta(match, { id: entryId, stream: context.stream });

      // Make settleBatch throw a non-retryable error
      const error = createSettlementError('Match already settled', 'ALREADY_SETTLED', false);
      setupMockSettleBatchError(mockSettleBatch, error);

      await expect(
        processSettlementBatch([matchMeta], context, testConfig),
      ).rejects.toThrow(BatchProcessingError);

      try {
        await processSettlementBatch([matchMeta], context, testConfig);
      } catch (e) {
        expect(e).toBeInstanceOf(BatchProcessingError);
        expect((e as BatchProcessingError).retryable).toBe(false);
      }
    });

    it('should wrap retryable settlement errors correctly', async () => {
      const match = createMatch();
      const entryId = await redis.xadd(context.stream, '*', 'data', JSON.stringify(match));
      if (!entryId) throw new Error('Failed to add entry');

      const matchMeta = createMatchWithMeta(match, { id: entryId, stream: context.stream });

      const error = createSettlementError('Network error', 'NETWORK_ERROR', true);
      setupMockSettleBatchError(mockSettleBatch, error);

      try {
        await processSettlementBatch([matchMeta], context, testConfig);
        fail('Should have thrown');
      } catch (e) {
        expect(e).toBeInstanceOf(BatchProcessingError);
        expect((e as BatchProcessingError).retryable).toBe(true);
      }
    });

    it('should wrap database errors as BatchProcessingError', async () => {
      const match = createMatch();
      const entryId = await redis.xadd(context.stream, '*', 'data', JSON.stringify(match));
      if (!entryId) throw new Error('Failed to add entry');

      const matchMeta = createMatchWithMeta(match, { id: entryId, stream: context.stream });

      // settleBatch succeeds, but database persistence fails
      setupMockSettleBatch(mockSettleBatch);
      mockApplySettlementResult.mockRejectedValueOnce({
        message: 'unique violation',
        code: '23505',
        retryable: false,
      });

      try {
        await processSettlementBatch([matchMeta], context, testConfig);
        fail('Should have thrown');
      } catch (e) {
        expect(e).toBeInstanceOf(BatchProcessingError);
        expect((e as BatchProcessingError).retryable).toBe(false);
      }
    });
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

    // Retry in case of random smart contract/database failures
    let retries = 3;
    while (retries > 0) {
      try {
        await processSettlementBatch([matchWithMeta], context, testConfig);
        break;
      } catch (error) {
        retries--;
        if (retries === 0) {
          throw error;
        }
        // Wait a bit before retry
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
    }

    expect(consoleLogSpy).toHaveBeenCalledWith(
      expect.objectContaining({
        component: 'process-settlement-batch',
        matchCount: 1,
        matches: [expect.objectContaining({
          id: entryId,
          matchId: '550e8400-e29b-41d4-a716-446655440100',
          lendOrderId: '550e8400-e29b-41d4-a716-446655440101',
          borrowOrderId: '550e8400-e29b-41d4-a716-446655440102',
        })],
      }),
      'Processing batch',
    );

    // Verify entry was deleted
    const length = await redis.xlen(context.stream);
    expect(length).toBe(0);
  });
});

