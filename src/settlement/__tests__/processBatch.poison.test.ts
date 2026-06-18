import type Redis from 'ioredis';
import {
  processSettlementBatch,
  BatchProcessingError,
  type SettlementBatchContext,
} from '../processBatch';
import { createMatch, createMatchWithMeta } from '../../tests/helpers/testFixtures';
import type { MatchWithMeta } from '../../redis/settlementMatchConsumer';
import { getRedisClient, closeRedisClient } from '../../redis/client';
import { cleanupTestStreams } from '../../tests/helpers/redisTestClient';
import { createTestConfig } from '../../tests/helpers/testConfig';
import { applySettlementResult, quarantineFailedMatch } from '../database';
import {
  setupMockSettleBatch,
  getMockSettleBatch,
  createSettlementError,
} from '../../tests/helpers/mockSmartContract';
import {
  simulateSettleBatch,
  simulateMatchesForPoison,
} from '../poisonIsolation';
import { logger } from '../../logger';

// Mock database + the (non-auto-mocked) poison-isolation module. smartContract
// is globally mocked in setup.ts.
jest.mock('../database');
jest.mock('../poisonIsolation');

const mockSettleBatch = getMockSettleBatch();
const mockApplySettlementResult = applySettlementResult as jest.MockedFunction<
  typeof applySettlementResult
>;
const mockQuarantine = quarantineFailedMatch as jest.MockedFunction<
  typeof quarantineFailedMatch
>;
const mockSimulateBatch = simulateSettleBatch as jest.MockedFunction<
  typeof simulateSettleBatch
>;
const mockSimulatePoison = simulateMatchesForPoison as jest.MockedFunction<
  typeof simulateMatchesForPoison
>;

/**
 * Poison-match isolation (Track C8) path in processSettlementBatch.
 * Uses a real Redis instance to assert ACK/XDEL of quarantined entries.
 *
 * @requires Redis server running (default: localhost:6379, or set REDIS_TEST_URL)
 */
describe('processSettlementBatch — poison isolation', () => {
  let redis: Redis;
  let context: SettlementBatchContext;
  let testStream: string;
  let testConfig: ReturnType<typeof createTestConfig>;
  let infoSpy: jest.SpyInstance;
  let warnSpy: jest.SpyInstance;

  beforeAll(async () => {
    const cfg = createTestConfig();
    redis = getRedisClient(cfg);
    try {
      await redis.ping();
    } catch {
      throw new Error(
        'Redis is not available. Please start Redis or set REDIS_TEST_URL.',
      );
    }
    await closeRedisClient();
  });

  beforeEach(async () => {
    jest.clearAllMocks();

    testStream = `test:settlement:matches:poison:${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    testConfig = createTestConfig({
      settlementMatchesStream: testStream,
    });
    redis = getRedisClient(testConfig);

    try {
      await redis.xgroup('CREATE', testStream, testConfig.consumerGroup, '0', 'MKSTREAM');
    } catch {
      // group may already exist
    }

    context = {
      redis,
      stream: testStream,
      consumerGroup: testConfig.consumerGroup,
      streamMaxLen: 10000,
    };
    infoSpy = jest.spyOn(logger, 'info').mockImplementation(() => {});
    warnSpy = jest.spyOn(logger, 'warn').mockImplementation(() => {});

    setupMockSettleBatch(mockSettleBatch);
    mockApplySettlementResult.mockResolvedValue(undefined);
    mockQuarantine.mockResolvedValue(true);
  });

  afterEach(async () => {
    infoSpy.mockRestore();
    warnSpy.mockRestore();
    await cleanupTestStreams(redis, [testStream]);
    await closeRedisClient();
  });

  /** Add a match to the stream and return its MatchWithMeta wired to the entry id. */
  const enqueue = async (overrides?: Parameters<typeof createMatch>[0]): Promise<MatchWithMeta> => {
    const match = createMatch(overrides);
    const entryId = await redis.xadd(testStream, '*', 'data', JSON.stringify(match));
    if (!entryId) throw new Error('Failed to add entry to stream');
    return createMatchWithMeta(match, { id: entryId, stream: testStream });
  };

  it('settles the whole batch when the dry-run is clean', async () => {
    mockSimulateBatch.mockResolvedValue(null);
    const m1 = await enqueue({ matchId: '550e8400-e29b-41d4-a716-446655440001' });
    const m2 = await enqueue({ matchId: '550e8400-e29b-41d4-a716-446655440002' });

    await processSettlementBatch([m1, m2], context, testConfig);

    expect(mockSimulateBatch).toHaveBeenCalledTimes(1);
    expect(mockSimulatePoison).not.toHaveBeenCalled();
    expect(mockQuarantine).not.toHaveBeenCalled();
    expect(mockSettleBatch).toHaveBeenCalledTimes(1);
    expect(mockSettleBatch.mock.calls[0][0].matches).toHaveLength(2);
  });

  it('quarantines the poison match and settles the survivors', async () => {
    const m1 = await enqueue({ matchId: '550e8400-e29b-41d4-a716-446655440001' });
    const poison = await enqueue({ matchId: '550e8400-e29b-41d4-a716-446655440002' });
    const m3 = await enqueue({ matchId: '550e8400-e29b-41d4-a716-446655440003' });

    mockSimulateBatch.mockResolvedValue(
      createSettlementError('Invalid match data', 'INVALID_MATCH_DATA', false),
    );
    mockSimulatePoison.mockResolvedValue({
      survivors: [m1, m3],
      poison: [poison],
      poisonReasons: new Map([[poison.id, 'INSUFFICIENT_FUNDS']]),
      survivorsSimulateClean: true,
    });

    await processSettlementBatch([m1, poison, m3], context, testConfig);

    // Poison quarantined with the sentinel + decoded code.
    expect(mockQuarantine).toHaveBeenCalledTimes(1);
    expect(mockQuarantine.mock.calls[0][0]).toEqual(poison.payload);
    expect(mockQuarantine.mock.calls[0][1]).toBe('POISON_PREFLIGHT_REVERT:INSUFFICIENT_FUNDS');

    // Survivors settled.
    expect(mockSettleBatch).toHaveBeenCalledTimes(1);
    expect(mockSettleBatch.mock.calls[0][0].matches).toHaveLength(2);

    // All three entries removed from the stream (poison via quarantine, survivors via settle).
    expect(await redis.xlen(testStream)).toBe(0);
  });

  it('returns early without settling when every match is poison', async () => {
    const p1 = await enqueue({ matchId: '550e8400-e29b-41d4-a716-446655440001' });
    const p2 = await enqueue({ matchId: '550e8400-e29b-41d4-a716-446655440002' });

    mockSimulateBatch.mockResolvedValue(
      createSettlementError('Invalid match data', 'INVALID_MATCH_DATA', false),
    );
    mockSimulatePoison.mockResolvedValue({
      survivors: [],
      poison: [p1, p2],
      poisonReasons: new Map([
        [p1.id, 'INVALID_MATCH_DATA'],
        [p2.id, 'INVALID_MATCH_DATA'],
      ]),
      survivorsSimulateClean: true,
    });

    await processSettlementBatch([p1, p2], context, testConfig);

    expect(mockQuarantine).toHaveBeenCalledTimes(2);
    expect(mockSettleBatch).not.toHaveBeenCalled();
    expect(await redis.xlen(testStream)).toBe(0);
  });

  it('throws a retryable error and quarantines nothing on a transient dry-run failure', async () => {
    const m = await enqueue();
    mockSimulateBatch.mockResolvedValue(
      createSettlementError('Contract is paused', 'CONTRACT_PAUSED', true),
    );

    await expect(
      processSettlementBatch([m], context, testConfig),
    ).rejects.toMatchObject({ retryable: true });

    expect(mockSimulatePoison).not.toHaveBeenCalled();
    expect(mockQuarantine).not.toHaveBeenCalled();
    expect(mockSettleBatch).not.toHaveBeenCalled();
    // Entry left in the stream for retry.
    expect(await redis.xlen(testStream)).toBe(1);
  });

  it('throws a non-retryable error and does not settle when survivors still revert', async () => {
    const m1 = await enqueue({ matchId: '550e8400-e29b-41d4-a716-446655440001' });
    const poison = await enqueue({ matchId: '550e8400-e29b-41d4-a716-446655440002' });

    mockSimulateBatch.mockResolvedValue(
      createSettlementError('Invalid match data', 'INVALID_MATCH_DATA', false),
    );
    mockSimulatePoison.mockResolvedValue({
      survivors: [m1],
      poison: [poison],
      poisonReasons: new Map([[poison.id, 'INSUFFICIENT_FUNDS']]),
      survivorsSimulateClean: false,
    });

    await expect(
      processSettlementBatch([m1, poison], context, testConfig),
    ).rejects.toMatchObject({ retryable: false });

    // Poison was quarantined; survivors were NOT settled.
    expect(mockQuarantine).toHaveBeenCalledTimes(1);
    expect(mockSettleBatch).not.toHaveBeenCalled();
  });

  it('throws a retryable error when the isolation probe itself is flaky', async () => {
    const m = await enqueue();
    mockSimulateBatch.mockResolvedValue(
      createSettlementError('Invalid match data', 'INVALID_MATCH_DATA', false),
    );
    mockSimulatePoison.mockRejectedValue(
      createSettlementError('network timeout', 'NETWORK_ERROR', true),
    );

    await expect(
      processSettlementBatch([m], context, testConfig),
    ).rejects.toMatchObject({ retryable: true });

    expect(mockQuarantine).not.toHaveBeenCalled();
    expect(mockSettleBatch).not.toHaveBeenCalled();
    expect(await redis.xlen(testStream)).toBe(1);
  });

  it('wraps isolation errors as BatchProcessingError', async () => {
    const m = await enqueue();
    mockSimulateBatch.mockResolvedValue(
      createSettlementError('Invalid match data', 'INVALID_MATCH_DATA', false),
    );
    mockSimulatePoison.mockRejectedValue(
      createSettlementError('boom', 'UNKNOWN_ERROR', true),
    );

    await expect(
      processSettlementBatch([m], context, testConfig),
    ).rejects.toBeInstanceOf(BatchProcessingError);
  });
});
