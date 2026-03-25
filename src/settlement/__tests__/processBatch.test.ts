import { processSettlementBatch, BatchProcessingError, type SettlementBatchContext } from '../processBatch';
import {
  createMatch,
  createMatchBatch,
  createMatchWithMeta,
} from '../../tests/helpers/testFixtures';
import { persistSettlementResults } from '../database';
import { setupMockSettleBatch, setupMockSettleBatchError, createSettlementError, getMockSettleBatch } from '../../tests/helpers/mockSmartContract';
import type { AppConfig } from '../../config';
import { createTestConfig } from '../../tests/helpers/testConfig';

// Mock database to avoid actual DB calls
jest.mock('../database');

const mockSettleBatch = getMockSettleBatch();
const mockPersistSettlementResults = persistSettlementResults as jest.MockedFunction<
  typeof persistSettlementResults
>;

/**
 * Unit tests for processSettlementBatch.
 * All Redis interactions are mocked — no live Redis required.
 */
describe('processSettlementBatch', () => {
  let context: SettlementBatchContext;
  let testConfig: AppConfig;
  let mockRedis: {
    xack: jest.Mock;
    xdel: jest.Mock;
    xtrim: jest.Mock;
  };
  let consoleLogSpy: jest.SpyInstance;
  let consoleErrorSpy: jest.SpyInstance;

  beforeEach(() => {
    jest.clearAllMocks();

    mockRedis = {
      xack: jest.fn().mockResolvedValue(1),
      xdel: jest.fn().mockResolvedValue(1),
      xtrim: jest.fn().mockResolvedValue(0),
    };

    const testStream = 'test:settlement:matches';
    testConfig = createTestConfig({ settlementMatchesStream: testStream });

    context = {
      redis: mockRedis as any,
      stream: testStream,
      consumerGroup: 'test-group',
      streamMaxLen: 10000,
    };

    consoleLogSpy = jest.spyOn(console, 'log').mockImplementation();
    consoleErrorSpy = jest.spyOn(console, 'error').mockImplementation();

    setupMockSettleBatch(mockSettleBatch);
    mockPersistSettlementResults.mockResolvedValue(undefined);
  });

  afterEach(() => {
    consoleLogSpy.mockRestore();
    consoleErrorSpy.mockRestore();
  });

  it('should return early for empty batch', async () => {
    await processSettlementBatch([], context, testConfig);

    expect(mockSettleBatch).not.toHaveBeenCalled();
    expect(mockPersistSettlementResults).not.toHaveBeenCalled();
    expect(mockRedis.xack).not.toHaveBeenCalled();
    expect(mockRedis.xdel).not.toHaveBeenCalled();
  });

  it('should process a single match batch', async () => {
    const match = createMatch();
    const matchWithMeta = createMatchWithMeta(match, {
      id: '1234-0',
      stream: context.stream,
    });

    await processSettlementBatch([matchWithMeta], context, testConfig);

    expect(mockSettleBatch).toHaveBeenCalledTimes(1);
    expect(mockPersistSettlementResults).toHaveBeenCalledTimes(1);
    expect(mockRedis.xack).toHaveBeenCalledWith(context.stream, context.consumerGroup, '1234-0');
    expect(mockRedis.xdel).toHaveBeenCalledWith(context.stream, '1234-0');
    expect(mockRedis.xtrim).toHaveBeenCalledWith(context.stream, 'MAXLEN', '~', context.streamMaxLen);
  });

  it('should process multiple matches from the same stream', async () => {
    const matches = [
      createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440001' }),
      createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440002' }),
      createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440003' }),
    ];

    const matchesWithMeta = matches.map((match, i) =>
      createMatchWithMeta(match, {
        id: `entry-${i}`,
        stream: context.stream,
      }),
    );

    await processSettlementBatch(matchesWithMeta, context, testConfig);

    expect(mockSettleBatch).toHaveBeenCalledTimes(1);
    expect(mockRedis.xack).toHaveBeenCalledTimes(3);
    expect(mockRedis.xdel).toHaveBeenCalledTimes(1); // batch delete
    expect(mockRedis.xtrim).toHaveBeenCalledTimes(1);
  });

  it('should group matches by stream and delete separately', async () => {
    const stream1 = 'test:stream1';
    const stream2 = 'test:stream2';

    const match1 = createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440001' });
    const match2 = createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440002' });
    const match3 = createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440003' });

    const matchesWithMeta = [
      createMatchWithMeta(match1, { id: 'e1', stream: stream1 }),
      createMatchWithMeta(match2, { id: 'e2', stream: stream1 }),
      createMatchWithMeta(match3, { id: 'e3', stream: stream2 }),
    ];

    await processSettlementBatch(matchesWithMeta, context, testConfig);

    // xack called per entry
    expect(mockRedis.xack).toHaveBeenCalledWith(stream1, context.consumerGroup, 'e1');
    expect(mockRedis.xack).toHaveBeenCalledWith(stream1, context.consumerGroup, 'e2');
    expect(mockRedis.xack).toHaveBeenCalledWith(stream2, context.consumerGroup, 'e3');

    // xdel called per stream batch
    expect(mockRedis.xdel).toHaveBeenCalledWith(stream1, 'e1', 'e2');
    expect(mockRedis.xdel).toHaveBeenCalledWith(stream2, 'e3');
  });

  it('should use correct streamMaxLen from context', async () => {
    const customContext: SettlementBatchContext = {
      ...context,
      streamMaxLen: 5,
    };

    const match = createMatch();
    const matchWithMeta = createMatchWithMeta(match, {
      id: 'e1',
      stream: context.stream,
    });

    await processSettlementBatch([matchWithMeta], customContext, testConfig);

    expect(mockRedis.xtrim).toHaveBeenCalledWith(context.stream, 'MAXLEN', '~', 5);
  });

  it('should handle large batches correctly', async () => {
    const batchSize = 100;
    const matches = createMatchBatch(batchSize);

    const matchesWithMeta = matches.map((match, i) =>
      createMatchWithMeta(match, {
        id: `entry-${i}`,
        stream: context.stream,
      }),
    );

    await processSettlementBatch(matchesWithMeta, context, testConfig);

    expect(consoleLogSpy).toHaveBeenCalledWith(
      '[process-settlement-batch] Processing batch of 100 matches',
      expect.any(Array),
    );
    expect(mockRedis.xack).toHaveBeenCalledTimes(100);
  });

  it('should throw BatchProcessingError when smart contract fails with retryable error', async () => {
    const error = createSettlementError('Contract is paused', 'CONTRACT_PAUSED', true, []);
    setupMockSettleBatchError(mockSettleBatch, error);

    const match = createMatch();
    const matchWithMeta = createMatchWithMeta(match, { id: 'e1', stream: context.stream });

    await expect(
      processSettlementBatch([matchWithMeta], context, testConfig),
    ).rejects.toThrow(BatchProcessingError);

    // Should not ACK or delete on failure
    expect(mockRedis.xack).not.toHaveBeenCalled();
    expect(mockRedis.xdel).not.toHaveBeenCalled();
  });

  it('should throw BatchProcessingError when smart contract fails with non-retryable error', async () => {
    const error = createSettlementError('Match already settled', 'ALREADY_SETTLED', false, []);
    setupMockSettleBatchError(mockSettleBatch, error);

    const match = createMatch();
    const matchWithMeta = createMatchWithMeta(match, { id: 'e1', stream: context.stream });

    try {
      await processSettlementBatch([matchWithMeta], context, testConfig);
      fail('Should have thrown');
    } catch (err) {
      expect(err).toBeInstanceOf(BatchProcessingError);
      expect((err as BatchProcessingError).retryable).toBe(false);
    }
  });

  it('should throw BatchProcessingError when database persistence fails', async () => {
    mockPersistSettlementResults.mockRejectedValue({
      message: 'Connection refused',
      code: '08006',
      retryable: true,
    });

    const match = createMatch();
    const matchWithMeta = createMatchWithMeta(match, { id: 'e1', stream: context.stream });

    try {
      await processSettlementBatch([matchWithMeta], context, testConfig);
      fail('Should have thrown');
    } catch (err) {
      expect(err).toBeInstanceOf(BatchProcessingError);
      expect((err as BatchProcessingError).retryable).toBe(true);
      expect((err as BatchProcessingError).message).toContain('Database persistence failed');
    }

    // Should not ACK or delete on DB failure
    expect(mockRedis.xack).not.toHaveBeenCalled();
  });

  it('should log match details correctly', async () => {
    const match = createMatch({
      matchId: '550e8400-e29b-41d4-a716-446655440100',
      lendOrderId: '550e8400-e29b-41d4-a716-446655440101',
      borrowOrderId: '550e8400-e29b-41d4-a716-446655440102',
    });

    const matchWithMeta = createMatchWithMeta(match, {
      id: 'entry-1',
      stream: context.stream,
    });

    await processSettlementBatch([matchWithMeta], context, testConfig);

    expect(consoleLogSpy).toHaveBeenCalledWith(
      '[process-settlement-batch] Processing batch of 1 matches',
      [
        {
          id: 'entry-1',
          matchId: '550e8400-e29b-41d4-a716-446655440100',
          lendOrderId: '550e8400-e29b-41d4-a716-446655440101',
          borrowOrderId: '550e8400-e29b-41d4-a716-446655440102',
        },
      ],
    );
  });

  it('should pass match payloads to settleBatch', async () => {
    const match1 = createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440001' });
    const match2 = createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440002' });

    const matchesWithMeta = [
      createMatchWithMeta(match1, { id: 'e1', stream: context.stream }),
      createMatchWithMeta(match2, { id: 'e2', stream: context.stream }),
    ];

    await processSettlementBatch(matchesWithMeta, context, testConfig);

    expect(mockSettleBatch).toHaveBeenCalledWith(
      expect.objectContaining({
        matches: [match1, match2],
        config: testConfig,
      }),
    );
  });

  it('should pass settlement result to persistSettlementResults', async () => {
    const match = createMatch();
    const matchWithMeta = createMatchWithMeta(match, { id: 'e1', stream: context.stream });

    await processSettlementBatch([matchWithMeta], context, testConfig);

    expect(mockPersistSettlementResults).toHaveBeenCalledWith({
      results: [
        expect.objectContaining({
          transactionHash: expect.any(String),
          blockNumber: expect.any(Number),
          gasUsed: expect.any(Number),
          timestamp: expect.any(Number),
          settledMatchIds: [match.matchId],
        }),
      ],
    });
  });

  it('should log batch processing completion', async () => {
    const match = createMatch();
    const matchWithMeta = createMatchWithMeta(match, { id: 'e1', stream: context.stream });

    await processSettlementBatch([matchWithMeta], context, testConfig);

    expect(consoleLogSpy).toHaveBeenCalledWith(
      '[process-settlement-batch] Batch processing complete',
      expect.objectContaining({
        matchCount: 1,
        transactionHash: expect.any(String),
      }),
    );
  });

  it('should handle error without retryable property as retryable', async () => {
    mockSettleBatch.mockRejectedValue({ message: 'Unknown error' });

    const match = createMatch();
    const matchWithMeta = createMatchWithMeta(match, { id: 'e1', stream: context.stream });

    try {
      await processSettlementBatch([matchWithMeta], context, testConfig);
      fail('Should have thrown');
    } catch (err) {
      expect(err).toBeInstanceOf(BatchProcessingError);
      // When retryable is undefined, defaults to true
      expect((err as BatchProcessingError).retryable).toBe(true);
    }
  });

  it('should handle database error without retryable property as retryable', async () => {
    mockPersistSettlementResults.mockRejectedValue({ message: 'DB error' });

    const match = createMatch();
    const matchWithMeta = createMatchWithMeta(match, { id: 'e1', stream: context.stream });

    try {
      await processSettlementBatch([matchWithMeta], context, testConfig);
      fail('Should have thrown');
    } catch (err) {
      expect(err).toBeInstanceOf(BatchProcessingError);
      expect((err as BatchProcessingError).retryable).toBe(true);
    }
  });
});
