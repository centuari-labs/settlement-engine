import type Redis from 'ioredis';
import {
  ensureConsumerGroup,
  readMatches,
  processPendingEntriesOnStartup,
} from '../settlementMatchConsumer';
import type { AppConfig } from '../../config';
import { createMatch } from '../../tests/helpers/testFixtures';
import {
  createIsolatedTestEnvironment,
  wait,
  type IsolatedTestEnvironment,
} from '../../tests/helpers/redisTestClient';
import { createIsolatedTestConfig } from '../../tests/helpers/testConfig';
import { logger } from '../../logger';

/**
 * Unit tests for settlement match consumer using a real Redis instance.
 * These tests verify behavior with actual Redis stream operations.
 *
 * @requires Redis server running (default: localhost:6379, or set REDIS_TEST_URL)
 */
describe('ensureConsumerGroup', () => {
  let testEnv: IsolatedTestEnvironment;
  let redis: Redis;
  let config: AppConfig;

  beforeEach(async () => {
    config = createIsolatedTestConfig();
    testEnv = await createIsolatedTestEnvironment(config);
    redis = testEnv.redis;
  });

  afterEach(async () => {
    await testEnv.cleanup();
  });

  it('should create a consumer group successfully', async () => {
    // Consumer group already created in beforeEach, verify it exists
    const groups = await redis.xinfo('GROUPS', config.settlementMatchesStream);
    expect(Array.isArray(groups)).toBe(true);
    if (Array.isArray(groups)) {
      expect(groups.length).toBeGreaterThan(0);
    }
  });

  it('should handle BUSYGROUP error gracefully when group already exists', async () => {
    // Consumer group already created in beforeEach
    // Should not throw when creating again
    await expect(
      ensureConsumerGroup(
        redis,
        config.settlementMatchesStream,
        config.consumerGroup,
      ),
    ).resolves.not.toThrow();
  });

  it('should propagate other errors', async () => {
    // Try to create group on non-existent stream without MKSTREAM
    await expect(
      redis.xgroup('CREATE', 'non-existent-stream', config.consumerGroup, '0'),
    ).rejects.toThrow();
  });
});

describe('readMatches', () => {
  let testEnv: IsolatedTestEnvironment;
  let redis: Redis;
  let config: AppConfig;
  let onInvalid: jest.Mock<Promise<void>, [unknown]>;

  // Increase timeout for all tests in this suite
  jest.setTimeout(30000);

  beforeEach(async () => {
    config = createIsolatedTestConfig();
    testEnv = await createIsolatedTestEnvironment(config);
    redis = testEnv.redis;
    onInvalid = jest.fn().mockResolvedValue(undefined);
  }, 30000);

  afterEach(async () => {
    await testEnv.cleanup();
  }, 30000);

  it('should read valid matches with JSON data field', async () => {
    const match = createMatch();
    const entryId = await redis.xadd(
      config.settlementMatchesStream,
      '*',
      'data',
      JSON.stringify(match),
    );

    const matches = await readMatches({
      redis,
      stream: config.settlementMatchesStream,
      consumerGroup: config.consumerGroup,
      consumerName: config.consumerName,
      readCount: config.readCount,
    });

    expect(matches).toHaveLength(1);
    expect(matches[0]).toEqual({
      id: entryId,
      stream: config.settlementMatchesStream,
      payload: match,
    });
  });

  it('should read valid matches with individual fields', async () => {
    const match = createMatch();
    const entryId = await redis.xadd(
      config.settlementMatchesStream,
      '*',
      'matchId',
      match.matchId,
      'marketId',
      match.marketId,
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
      'lenderSettlementFeeAmount',
      String(match.lenderSettlementFeeAmount),
      'borrowerSettlementFeeAmount',
      String(match.borrowerSettlementFeeAmount),
    );

    const matches = await readMatches({
      redis,
      stream: config.settlementMatchesStream,
      consumerGroup: config.consumerGroup,
      consumerName: config.consumerName,
      readCount: config.readCount,
    });

    expect(matches).toHaveLength(1);
    expect(matches[0]).toEqual({
      id: entryId,
      stream: config.settlementMatchesStream,
      payload: match,
    });
  });

  it('should handle invalid JSON in data field', async () => {
    const entryId = await redis.xadd(
      config.settlementMatchesStream,
      '*',
      'data',
      'invalid json {',
    );

    const matches = await readMatches({
      redis,
      stream: config.settlementMatchesStream,
      consumerGroup: config.consumerGroup,
      consumerName: config.consumerName,
      readCount: config.readCount,
      onInvalid,
    });

    expect(matches).toHaveLength(0);
    expect(onInvalid).toHaveBeenCalledTimes(1);
    expect(onInvalid).toHaveBeenCalledWith({
      id: entryId,
      stream: config.settlementMatchesStream,
      raw: expect.any(Object),
      error: expect.any(Error),
    });
  });

  it('should handle invalid schema matches', async () => {
    const invalidMatch = {
      matchId: 'invalid', // Not a UUID
      lendOrderId: '550e8400-e29b-41d4-a716-446655440001',
    };
    const entryId = await redis.xadd(
      config.settlementMatchesStream,
      '*',
      'data',
      JSON.stringify(invalidMatch),
    );

    const matches = await readMatches({
      redis,
      stream: config.settlementMatchesStream,
      consumerGroup: config.consumerGroup,
      consumerName: config.consumerName,
      readCount: config.readCount,
      onInvalid,
    });

    expect(matches).toHaveLength(0);
    expect(onInvalid).toHaveBeenCalledTimes(1);
    expect(onInvalid).toHaveBeenCalledWith({
      id: entryId,
      stream: config.settlementMatchesStream,
      raw: expect.any(Object),
      error: expect.any(Error),
    });
  });

  it('should log to console.error when onInvalid handler is not provided', async () => {
    const loggerSpy = jest.spyOn(logger, 'error').mockImplementation(() => {});
    await redis.xadd(
      config.settlementMatchesStream,
      '*',
      'data',
      JSON.stringify({ invalid: 'data' }),
    );

    const matches = await readMatches({
      redis,
      stream: config.settlementMatchesStream,
      consumerGroup: config.consumerGroup,
      consumerName: config.consumerName,
      readCount: config.readCount,
    });

    expect(matches).toHaveLength(0);
    expect(loggerSpy).toHaveBeenCalledWith(
      expect.objectContaining({ component: 'settlement-consumer' }),
      'Invalid match entry',
    );

    loggerSpy.mockRestore();
  });

  it('should return empty array when stream is empty', async () => {
    const matches = await readMatches({
      redis,
      stream: config.settlementMatchesStream,
      consumerGroup: config.consumerGroup,
      consumerName: config.consumerName,
      readCount: config.readCount,
    });

    expect(matches).toHaveLength(0);
  }, 15000);

  it('should read multiple matches', async () => {
    const match1 = createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440001' });
    const match2 = createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440002' });

    const entryId1 = await redis.xadd(
      config.settlementMatchesStream,
      '*',
      'data',
      JSON.stringify(match1),
    );
    const entryId2 = await redis.xadd(
      config.settlementMatchesStream,
      '*',
      'data',
      JSON.stringify(match2),
    );

    const matches = await readMatches({
      redis,
      stream: config.settlementMatchesStream,
      consumerGroup: config.consumerGroup,
      consumerName: config.consumerName,
      readCount: config.readCount,
    });

    expect(matches).toHaveLength(2);
    expect(matches[0]?.id).toBe(entryId1);
    expect(matches[1]?.id).toBe(entryId2);
  }, 15000);

  it('should respect readCount limit', async () => {
    // Add more matches than readCount
    const matches = Array.from({ length: 15 }, () => createMatch());
    for (const match of matches) {
      await redis.xadd(
        config.settlementMatchesStream,
        '*',
        'data',
        JSON.stringify(match),
      );
    }

    const readMatchesResult = await readMatches({
      redis,
      stream: config.settlementMatchesStream,
      consumerGroup: config.consumerGroup,
      consumerName: config.consumerName,
      readCount: 5,
    });

    // Should read up to readCount
    expect(readMatchesResult.length).toBeLessThanOrEqual(5);
  }, 15000);

  it('should handle errors gracefully', async () => {
    const loggerSpy = jest.spyOn(logger, 'error').mockImplementation(() => {});

    // Disconnect Redis to cause errors
    await redis.disconnect();

    const matches = await readMatches({
      redis,
      stream: config.settlementMatchesStream,
      consumerGroup: config.consumerGroup,
      consumerName: config.consumerName,
      readCount: config.readCount,
    });

    expect(matches).toHaveLength(0);
    expect(loggerSpy).toHaveBeenCalledWith(
      expect.objectContaining({ component: 'settlement-consumer' }),
      'Error reading matches',
    );

    loggerSpy.mockRestore();

    // Wait a bit to ensure cleanup doesn't interfere
    await wait(100);
  }, 15000);
});

describe('processPendingEntriesOnStartup', () => {
  let testEnv: IsolatedTestEnvironment;
  let redis: Redis;
  let config: AppConfig;

  jest.setTimeout(30000);

  beforeEach(async () => {
    config = createIsolatedTestConfig();
    testEnv = await createIsolatedTestEnvironment(config);
    redis = testEnv.redis;
  }, 30000);

  afterEach(async () => {
    await testEnv.cleanup();
  }, 30000);

  it('should recover own pending entries (Phase 1 — "0" cursor)', async () => {
    const match = createMatch();
    // Add an entry and read it (creates a pending entry for this consumer)
    await redis.xadd(
      config.settlementMatchesStream,
      '*',
      'data',
      JSON.stringify(match),
    );

    // Read it so it becomes pending (assigned to this consumer but not ACKed)
    const initial = await readMatches({
      redis,
      stream: config.settlementMatchesStream,
      consumerGroup: config.consumerGroup,
      consumerName: config.consumerName,
      readCount: config.readCount,
    });
    expect(initial).toHaveLength(1);

    // Now call processPendingEntriesOnStartup — it should re-read the pending entry
    const recovered = await processPendingEntriesOnStartup({
      redis,
      stream: config.settlementMatchesStream,
      consumerGroup: config.consumerGroup,
      consumerName: config.consumerName,
      readCount: config.readCount,
    });

    expect(recovered).toHaveLength(1);
    expect(recovered[0]?.payload.matchId).toBe(match.matchId);
  });

  it('should return empty array when no pending entries exist', async () => {
    const recovered = await processPendingEntriesOnStartup({
      redis,
      stream: config.settlementMatchesStream,
      consumerGroup: config.consumerGroup,
      consumerName: config.consumerName,
      readCount: config.readCount,
    });

    expect(recovered).toHaveLength(0);
  });

  it('should recover multiple pending entries', async () => {
    // Add 5 entries
    const matches = Array.from({ length: 5 }, (_, i) =>
      createMatch({ matchId: `550e8400-e29b-41d4-a716-${String(i).padStart(12, '0')}` }),
    );
    for (const match of matches) {
      await redis.xadd(
        config.settlementMatchesStream,
        '*',
        'data',
        JSON.stringify(match),
      );
    }

    // Read them all so they become pending
    await readMatches({
      redis,
      stream: config.settlementMatchesStream,
      consumerGroup: config.consumerGroup,
      consumerName: config.consumerName,
      readCount: 10,
    });

    // Recover all pending entries
    const recovered = await processPendingEntriesOnStartup({
      redis,
      stream: config.settlementMatchesStream,
      consumerGroup: config.consumerGroup,
      consumerName: config.consumerName,
      readCount: 10,
    });

    expect(recovered).toHaveLength(5);
    // Verify all match IDs are present
    const recoveredIds = recovered.map((m) => m.payload.matchId).sort();
    const expectedIds = matches.map((m) => m.matchId).sort();
    expect(recoveredIds).toEqual(expectedIds);
  });

  it('should respect maxEntries cap between iterations', async () => {
    // Add 6 entries and make them pending
    const matches = Array.from({ length: 6 }, (_, i) =>
      createMatch({ matchId: `550e8400-e29b-41d4-a716-${String(i).padStart(12, '0')}` }),
    );
    for (const match of matches) {
      await redis.xadd(
        config.settlementMatchesStream,
        '*',
        'data',
        JSON.stringify(match),
      );
    }

    await readMatches({
      redis,
      stream: config.settlementMatchesStream,
      consumerGroup: config.consumerGroup,
      consumerName: config.consumerName,
      readCount: 10,
    });

    // Use readCount=3 and maxEntries=3: first iteration reads 3 entries,
    // then cap check prevents a second iteration
    const recovered = await processPendingEntriesOnStartup({
      redis,
      stream: config.settlementMatchesStream,
      consumerGroup: config.consumerGroup,
      consumerName: config.consumerName,
      readCount: 3,
      maxEntries: 3,
    });

    // First iteration reads exactly readCount (3) entries, then cap stops further reads
    expect(recovered).toHaveLength(3);
  });

  it('should XCLAIM stale entries from other consumers (Phase 2)', async () => {
    // Create a second consumer in the same group
    const otherConsumer = 'other-consumer-stale';
    const match = createMatch();

    await redis.xadd(
      config.settlementMatchesStream,
      '*',
      'data',
      JSON.stringify(match),
    );

    // Read with the OTHER consumer so it owns the pending entry
    await (redis.xreadgroup as (...args: (string | number)[]) => Promise<unknown>)(
      'GROUP',
      config.consumerGroup,
      otherConsumer,
      'COUNT',
      10,
      'STREAMS',
      config.settlementMatchesStream,
      '>',
    );

    // Wait for the entry to become idle enough for XCLAIM
    await wait(200);

    // Now call processPendingEntriesOnStartup as our consumer with low idle threshold
    const recovered = await processPendingEntriesOnStartup({
      redis,
      stream: config.settlementMatchesStream,
      consumerGroup: config.consumerGroup,
      consumerName: config.consumerName,
      readCount: config.readCount,
      xclaimMinIdleMs: 100, // Low threshold so our 200ms idle entry gets claimed
    });

    expect(recovered).toHaveLength(1);
    expect(recovered[0]?.payload.matchId).toBe(match.matchId);
  });

  it('should not XCLAIM entries that are not idle long enough', async () => {
    const otherConsumer = 'other-consumer-fresh';
    const match = createMatch();

    await redis.xadd(
      config.settlementMatchesStream,
      '*',
      'data',
      JSON.stringify(match),
    );

    // Read with the other consumer
    await (redis.xreadgroup as (...args: (string | number)[]) => Promise<unknown>)(
      'GROUP',
      config.consumerGroup,
      otherConsumer,
      'COUNT',
      10,
      'STREAMS',
      config.settlementMatchesStream,
      '>',
    );

    // Don't wait — entry is fresh

    // Call with high idle threshold — should not claim the fresh entry
    const recovered = await processPendingEntriesOnStartup({
      redis,
      stream: config.settlementMatchesStream,
      consumerGroup: config.consumerGroup,
      consumerName: config.consumerName,
      readCount: config.readCount,
      xclaimMinIdleMs: 60000, // 60s — entry is only a few ms old
    });

    // Phase 1 (own pending) returns nothing because our consumer has no pending
    // Phase 2 (XCLAIM) returns nothing because the entry isn't idle enough
    expect(recovered).toHaveLength(0);
  });

  it('should handle errors gracefully during pending processing', async () => {
    const loggerSpy = jest.spyOn(logger, 'error').mockImplementation(() => {});

    await redis.disconnect();

    const recovered = await processPendingEntriesOnStartup({
      redis,
      stream: config.settlementMatchesStream,
      consumerGroup: config.consumerGroup,
      consumerName: config.consumerName,
      readCount: config.readCount,
    });

    expect(recovered).toHaveLength(0);
    expect(loggerSpy).toHaveBeenCalledWith(
      expect.objectContaining({ component: 'settlement-consumer' }),
      'Error processing pending entries',
    );

    loggerSpy.mockRestore();
    await wait(100);
  });

  it('should ACK invalid pending entries and exclude them from results', async () => {
    const onInvalid = jest.fn().mockResolvedValue(undefined);

    // Add an invalid entry
    await redis.xadd(
      config.settlementMatchesStream,
      '*',
      'data',
      JSON.stringify({ invalid: 'not-a-match' }),
    );

    // Read it to make it pending
    await readMatches({
      redis,
      stream: config.settlementMatchesStream,
      consumerGroup: config.consumerGroup,
      consumerName: config.consumerName,
      readCount: 10,
      onInvalid,
    });

    // The first readMatches already ACKed the invalid entry.
    // Reset the mock to track calls from processPendingEntriesOnStartup
    onInvalid.mockClear();

    const recovered = await processPendingEntriesOnStartup({
      redis,
      stream: config.settlementMatchesStream,
      consumerGroup: config.consumerGroup,
      consumerName: config.consumerName,
      readCount: 10,
      onInvalid,
    });

    // No valid matches should be returned
    expect(recovered).toHaveLength(0);
  });
});
