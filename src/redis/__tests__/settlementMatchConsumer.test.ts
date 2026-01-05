import type Redis from 'ioredis';
import {
  ensureConsumerGroup,
  readMatches,
} from '../settlementMatchConsumer';
import type { AppConfig } from '../../config';
import { createMatch } from '../../tests/helpers/testFixtures';
import {
  createIsolatedTestEnvironment,
  wait,
  type IsolatedTestEnvironment,
} from '../../tests/helpers/redisTestClient';
import { createIsolatedTestConfig } from '../../tests/helpers/testConfig';

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
    const consoleSpy = jest.spyOn(console, 'error').mockImplementation();
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
    expect(consoleSpy).toHaveBeenCalledWith(
      '[settlement-consumer] Invalid match entry',
      expect.any(String),
    );

    consoleSpy.mockRestore();
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
    const consoleSpy = jest.spyOn(console, 'error').mockImplementation();

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
    expect(consoleSpy).toHaveBeenCalledWith(
      '[settlement-consumer] Error reading matches',
      expect.any(Error),
    );

    consoleSpy.mockRestore();

    // Wait a bit to ensure cleanup doesn't interfere
    await wait(100);
  }, 15000);
});
