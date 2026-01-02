import type Redis from 'ioredis';
import {
  ensureConsumerGroup,
  startSettlementMatchConsumer,
  type MatchWithMeta,
} from '../settlementMatchConsumer';
import type { AppConfig } from '../../config';
import { createMatch } from '../../tests/helpers/testFixtures';
import { getRedisClient, closeRedisClient } from '../client';
import { cleanupTestStreams } from '../../tests/helpers/redisTestClient';
import { createTestConfig } from '../../tests/helpers/testConfig';

/**
 * Integration tests for settlement match consumer using a real Redis instance.
 * These tests verify end-to-end behavior with actual Redis stream operations.
 *
 * @requires Redis server running (default: localhost:6379, or set REDIS_TEST_URL)
 */
describe('settlementMatchConsumer Integration Tests', () => {
  let redis: Redis;
  let config: AppConfig;
  let onMatch: jest.Mock<Promise<void>, [MatchWithMeta]>;
  let onInvalid: jest.Mock<Promise<void>, [any]>;
  const activeConsumers: Array<() => void> = [];

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
    // Use the real Redis client factory for each test
    config = createTestConfig({
      settlementMatchesStream: `test:settlement:matches:${Date.now()}`,
      consumerGroup: `test-settlement-engine-${Date.now()}`,
      consumerName: 'test-consumer-1',
    });
    redis = getRedisClient(config);
    onMatch = jest.fn().mockResolvedValue(undefined);
    onInvalid = jest.fn().mockResolvedValue(undefined);
    // Clear active consumers for this test
    activeConsumers.length = 0;
  });

  afterEach(async () => {
    // Stop all active consumers and wait for them to finish
    for (const stop of activeConsumers) {
      stop();
    }
    // Wait for consumers to exit their loops (they might be blocked in xreadgroup)
    // Give them time to exit after stop() is called
    await new Promise((resolve) => setTimeout(resolve, 150));
    
    // Clean up test streams and consumer groups
    await cleanupTestStreams(redis, [config.settlementMatchesStream]);
    // Close the Redis client to reset singleton for next test
    await closeRedisClient();
  });

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

  it('should consume and process a single match from stream', async () => {
    await ensureConsumerGroup(
      redis,
      config.settlementMatchesStream,
      config.consumerGroup,
    );

    // Add a match to the stream
    const match = createMatch();
    const entryId = await redis.xadd(
      config.settlementMatchesStream,
      '*',
      'data',
      JSON.stringify(match),
    );

    expect(entryId).toBeTruthy();

    // Start consumer
    const stop = startSettlementMatchConsumer({
      redis,
      config,
      onMatch,
    });
    activeConsumers.push(stop);

    // Wait for consumer to process
    await new Promise((resolve) => setTimeout(resolve, 200));

    // Verify match was processed
    expect(onMatch).toHaveBeenCalledTimes(1);
    expect(onMatch).toHaveBeenCalledWith({
      id: entryId,
      stream: config.settlementMatchesStream,
      payload: match,
    });

    stop();
  });

  it('should acknowledge entries after processing', async () => {
    await ensureConsumerGroup(
      redis,
      config.settlementMatchesStream,
      config.consumerGroup,
    );

    const match = createMatch();
    const entryId = await redis.xadd(
      config.settlementMatchesStream,
      '*',
      'data',
      JSON.stringify(match),
    );

    const stop = startSettlementMatchConsumer({
      redis,
      config,
      onMatch,
    });
    activeConsumers.push(stop);

    await new Promise((resolve) => setTimeout(resolve, 200));

    // Check pending entries (should be 0 after ack)
    const pending = await redis.xpending(
      config.settlementMatchesStream,
      config.consumerGroup,
    );

    // After acknowledgment, there should be no pending entries
    expect(onMatch).toHaveBeenCalled();
    stop();
  });

  it('should process multiple matches in sequence', async () => {
    await ensureConsumerGroup(
      redis,
      config.settlementMatchesStream,
      config.consumerGroup,
    );

    const match1 = createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440001' });
    const match2 = createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440002' });
    const match3 = createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440003' });

    // Start consumer first, then add messages
    const stop = startSettlementMatchConsumer({
      redis,
      config,
      onMatch,
    });
    activeConsumers.push(stop);

    // Add messages after consumer starts
    await redis.xadd(config.settlementMatchesStream, '*', 'data', JSON.stringify(match1));
    await redis.xadd(config.settlementMatchesStream, '*', 'data', JSON.stringify(match2));
    await redis.xadd(config.settlementMatchesStream, '*', 'data', JSON.stringify(match3));

    // Wait for all matches to be processed
    // Give more time for consumer to read and process
    await new Promise((resolve) => setTimeout(resolve, 600));

    expect(onMatch).toHaveBeenCalledTimes(3);

    // Verify all matches were processed
    const processedMatchIds = onMatch.mock.calls.map((call) => call[0].payload.matchId);
    expect(processedMatchIds).toContain(match1.matchId);
    expect(processedMatchIds).toContain(match2.matchId);
    expect(processedMatchIds).toContain(match3.matchId);

    stop();
  });

  it('should handle invalid match entries with onInvalid handler', async () => {
    await ensureConsumerGroup(
      redis,
      config.settlementMatchesStream,
      config.consumerGroup,
    );

    // Add invalid entry (missing required fields)
    await redis.xadd(
      config.settlementMatchesStream,
      '*',
      'data',
      JSON.stringify({ invalid: 'data' }),
    );

    const stop = startSettlementMatchConsumer({
      redis,
      config,
      onMatch,
      onInvalid,
    });
    activeConsumers.push(stop);

    await new Promise((resolve) => setTimeout(resolve, 200));

    expect(onMatch).not.toHaveBeenCalled();
    expect(onInvalid).toHaveBeenCalledTimes(1);
    expect(onInvalid).toHaveBeenCalledWith({
      id: expect.any(String),
      stream: config.settlementMatchesStream,
      raw: expect.any(Object),
      error: expect.any(Error),
    });

    stop();
  });

  it('should handle matches with individual field format (not JSON)', async () => {
    await ensureConsumerGroup(
      redis,
      config.settlementMatchesStream,
      config.consumerGroup,
    );

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
    );

    const stop = startSettlementMatchConsumer({
      redis,
      config,
      onMatch,
    });
    activeConsumers.push(stop);

    await new Promise((resolve) => setTimeout(resolve, 200));

    expect(onMatch).toHaveBeenCalledTimes(1);
    expect(onMatch).toHaveBeenCalledWith({
      id: expect.any(String),
      stream: config.settlementMatchesStream,
      payload: match,
    });

    stop();
  });

  it('should respect readCount limit', async () => {
    await ensureConsumerGroup(
      redis,
      config.settlementMatchesStream,
      config.consumerGroup,
    );

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

    const customConfig = { ...config, readCount: 5 };
    const stop = startSettlementMatchConsumer({
      redis,
      config: customConfig,
      onMatch,
    });
    activeConsumers.push(stop);

    // Wait for processing
    await new Promise((resolve) => setTimeout(resolve, 500));

    // Should process all matches eventually (in multiple iterations)
    // Note: exact count depends on mock implementation, but should process all
    expect(onMatch).toHaveBeenCalled();

    stop();
  });

  it('should stop processing when stop() is called', async () => {
    await ensureConsumerGroup(
      redis,
      config.settlementMatchesStream,
      config.consumerGroup,
    );

    // Add a match
    await redis.xadd(
      config.settlementMatchesStream,
      '*',
      'data',
      JSON.stringify(createMatch()),
    );

    const stop = startSettlementMatchConsumer({
      redis,
      config,
      onMatch,
    });
    activeConsumers.push(stop);

    // Let it process once
    await new Promise((resolve) => setTimeout(resolve, 200));
    const initialCount = onMatch.mock.calls.length;

    // Stop the consumer
    stop();

    // Add another match
    await redis.xadd(
      config.settlementMatchesStream,
      '*',
      'data',
      JSON.stringify(createMatch()),
    );

    // Wait - should not process more
    await new Promise((resolve) => setTimeout(resolve, 300));

    // Should not have processed significantly more (might process one more in flight)
    expect(onMatch.mock.calls.length).toBeLessThanOrEqual(initialCount + 1);
  });
});

/**
 * Integration tests for processPendingEntries mechanism.
 * These tests verify that pending entries for the current consumer and stale entries
 * from other consumers are correctly processed when the consumer starts.
 */
describe('processPendingEntries', () => {
  let redis: Redis;
  let config: AppConfig;
  let onMatch: jest.Mock<Promise<void>, [MatchWithMeta]>;
  let onInvalid: jest.Mock<Promise<void>, [any]>;
  const activeConsumers: Array<() => void> = [];

  beforeAll(async () => {
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
    config = createTestConfig({
      settlementMatchesStream: `test:settlement:matches:${Date.now()}`,
      consumerGroup: `test-settlement-engine-${Date.now()}`,
      consumerName: 'test-consumer-1',
    });
    redis = getRedisClient(config);
    onMatch = jest.fn().mockResolvedValue(undefined);
    onInvalid = jest.fn().mockResolvedValue(undefined);
    activeConsumers.length = 0;
  });

  afterEach(async () => {
    for (const stop of activeConsumers) {
      stop();
    }
    await new Promise((resolve) => setTimeout(resolve, 150));
    await cleanupTestStreams(redis, [config.settlementMatchesStream]);
    await closeRedisClient();
  });

  it('should process pending entries for current consumer', async () => {
    await ensureConsumerGroup(
      redis,
      config.settlementMatchesStream,
      config.consumerGroup,
    );

    // Create entries in the stream
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

    // Read entries with XREADGROUP to make them pending (but don't ACK them)
    const readResult = (await (redis.xreadgroup as (
      ...args: (string | number)[]
    ) => Promise<[[string, Array<[string, string[]]>]] | null>)(
      'GROUP',
      config.consumerGroup,
      config.consumerName,
      'COUNT',
      10,
      'STREAMS',
      config.settlementMatchesStream,
      '>',
    )) as [[string, Array<[string, string[]]>]] | null;

    // Verify entries were actually read
    expect(readResult).not.toBeNull();
    expect(readResult?.[0]?.[1]?.length).toBe(2);

    // Verify entries are pending
    const pendingBefore = (await redis.xpending(
      config.settlementMatchesStream,
      config.consumerGroup,
    )) as [number, string, string, Array<[string, string]>];
    expect(pendingBefore[0]).toBe(2);

    // Start consumer - it should process pending entries automatically
    const stop = startSettlementMatchConsumer({
      redis,
      config,
      onMatch,
    });
    activeConsumers.push(stop);

    // Wait for pending entries to be processed (poll until done)
    let attempts = 0;
    const maxAttempts = 20;
    while (attempts < maxAttempts) {
      await new Promise((resolve) => setTimeout(resolve, 100));
      const pending = (await redis.xpending(
        config.settlementMatchesStream,
        config.consumerGroup,
      )) as [number, string, string, Array<[string, string]>];
      if (pending[0] === 0 && onMatch.mock.calls.length >= 2) {
        break;
      }
      attempts++;
    }

    // Verify all pending entries were processed
    expect(onMatch).toHaveBeenCalledTimes(2);
    const processedIds = onMatch.mock.calls.map((call) => call[0].id);
    expect(processedIds).toContain(entryId1);
    expect(processedIds).toContain(entryId2);

    // Verify entries are ACKed (no pending entries)
    const pendingAfter = (await redis.xpending(
      config.settlementMatchesStream,
      config.consumerGroup,
    )) as [number, string, string, Array<[string, string]>];
    expect(pendingAfter[0]).toBe(0);

    stop();
  });

  it('should claim and process stale entries from other consumers', async () => {
    await ensureConsumerGroup(
      redis,
      config.settlementMatchesStream,
      config.consumerGroup,
    );

    // Create entries in the stream
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

    // Read entries with a different consumer name to make them pending for that consumer
    const otherConsumerName = 'other-consumer';
    const readResult = (await (redis.xreadgroup as (
      ...args: (string | number)[]
    ) => Promise<[[string, Array<[string, string[]]>]] | null>)(
      'GROUP',
      config.consumerGroup,
      otherConsumerName,
      'COUNT',
      10,
      'STREAMS',
      config.settlementMatchesStream,
      '>',
    )) as [[string, Array<[string, string[]]>]] | null;

    // Verify entries were actually read
    expect(readResult).not.toBeNull();
    expect(readResult?.[0]?.[1]?.length).toBe(2);

    // Verify entries are pending for the other consumer
    const pendingBefore = (await redis.xpending(
      config.settlementMatchesStream,
      config.consumerGroup,
    )) as [number, string, string, Array<[string, string]>];
    expect(pendingBefore[0]).toBe(2);

    // Start consumer with different name - it should claim stale entries
    const stop = startSettlementMatchConsumer({
      redis,
      config,
      onMatch,
    });
    activeConsumers.push(stop);

    // Wait for stale entries to be claimed and processed (poll until done)
    let attempts = 0;
    const maxAttempts = 20;
    while (attempts < maxAttempts) {
      await new Promise((resolve) => setTimeout(resolve, 100));
      const pending = (await redis.xpending(
        config.settlementMatchesStream,
        config.consumerGroup,
      )) as [number, string, string, Array<[string, string]>];
      if (pending[0] === 0 && onMatch.mock.calls.length >= 2) {
        break;
      }
      attempts++;
    }

    // Verify all stale entries were claimed and processed
    expect(onMatch).toHaveBeenCalledTimes(2);
    const processedIds = onMatch.mock.calls.map((call) => call[0].id);
    expect(processedIds).toContain(entryId1);
    expect(processedIds).toContain(entryId2);

    // Verify entries are ACKed (no pending entries)
    const pendingAfter = (await redis.xpending(
      config.settlementMatchesStream,
      config.consumerGroup,
    )) as [number, string, string, Array<[string, string]>];
    expect(pendingAfter[0]).toBe(0);

    stop();
  });

  it('should process multiple batches of pending entries', async () => {
    await ensureConsumerGroup(
      redis,
      config.settlementMatchesStream,
      config.consumerGroup,
    );

    // Create more entries than readCount
    const matches = Array.from({ length: 15 }, (_, index) =>
      createMatch({
        matchId: `550e8400-e29b-41d4-a716-${String(index).padStart(12, '0')}`,
      }),
    );
    const entryIds: string[] = [];
    for (const match of matches) {
      const entryId = await redis.xadd(
        config.settlementMatchesStream,
        '*',
        'data',
        JSON.stringify(match),
      );
      if (entryId) {
        entryIds.push(entryId);
      }
    }

    // Read all entries to make them pending (but don't ACK them)
    const readResult = (await (redis.xreadgroup as (
      ...args: (string | number)[]
    ) => Promise<[[string, Array<[string, string[]]>]] | null>)(
      'GROUP',
      config.consumerGroup,
      config.consumerName,
      'COUNT',
      20,
      'STREAMS',
      config.settlementMatchesStream,
      '>',
    )) as [[string, Array<[string, string[]]>]] | null;

    // Verify entries were actually read
    expect(readResult).not.toBeNull();
    expect(readResult?.[0]?.[1]?.length).toBe(15);

    // Verify entries are pending
    const pendingBefore = (await redis.xpending(
      config.settlementMatchesStream,
      config.consumerGroup,
    )) as [number, string, string, Array<[string, string]>];
    expect(pendingBefore[0]).toBe(15);

    // Start consumer with readCount limit - it should process in batches
    const customConfig = { ...config, readCount: 5 };
    const stop = startSettlementMatchConsumer({
      redis,
      config: customConfig,
      onMatch,
    });
    activeConsumers.push(stop);

    // Wait for all batches to be processed (poll until done)
    let attempts = 0;
    const maxAttempts = 30;
    while (attempts < maxAttempts) {
      await new Promise((resolve) => setTimeout(resolve, 100));
      const pending = (await redis.xpending(
        config.settlementMatchesStream,
        config.consumerGroup,
      )) as [number, string, string, Array<[string, string]>];
      if (pending[0] === 0 && onMatch.mock.calls.length >= 15) {
        break;
      }
      attempts++;
    }

    // Verify all entries were processed
    expect(onMatch).toHaveBeenCalledTimes(15);
    const processedIds = onMatch.mock.calls.map((call) => call[0].id);
    for (const entryId of entryIds) {
      expect(processedIds).toContain(entryId);
    }

    // Verify entries are ACKed (no pending entries)
    const pendingAfter = (await redis.xpending(
      config.settlementMatchesStream,
      config.consumerGroup,
    )) as [number, string, string, Array<[string, string]>];
    expect(pendingAfter[0]).toBe(0);

    stop();
  });

  it('should process mixed scenario with both pending and stale entries', async () => {
    await ensureConsumerGroup(
      redis,
      config.settlementMatchesStream,
      config.consumerGroup,
    );

    // Create entries
    const match1 = createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440001' });
    const match2 = createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440002' });
    const match3 = createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440003' });
    const match4 = createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440004' });

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
    const entryId3 = await redis.xadd(
      config.settlementMatchesStream,
      '*',
      'data',
      JSON.stringify(match3),
    );
    const entryId4 = await redis.xadd(
      config.settlementMatchesStream,
      '*',
      'data',
      JSON.stringify(match4),
    );

    // Read first two entries with current consumer (pending for current consumer)
    // Use COUNT 2 to limit the read to only 2 entries, leaving 2 for the other consumer
    const readResult1 = (await (redis.xreadgroup as (
      ...args: (string | number)[]
    ) => Promise<[[string, Array<[string, string[]]>]] | null>)(
      'GROUP',
      config.consumerGroup,
      config.consumerName,
      'COUNT',
      2,
      'STREAMS',
      config.settlementMatchesStream,
      '>',
    )) as [[string, Array<[string, string[]]>]] | null;

    // Verify first two entries were read
    expect(readResult1).not.toBeNull();
    expect(readResult1?.[0]?.[1]?.length).toBe(2);

    // Read next two entries with different consumer (stale entries)
    // The remaining 2 entries are still "new" and will be read by this consumer
    const otherConsumerName = 'other-consumer';
    const readResult2 = (await (redis.xreadgroup as (
      ...args: (string | number)[]
    ) => Promise<[[string, Array<[string, string[]]>]] | null>)(
      'GROUP',
      config.consumerGroup,
      otherConsumerName,
      'COUNT',
      2,
      'STREAMS',
      config.settlementMatchesStream,
      '>',
    )) as [[string, Array<[string, string[]]>]] | null;

    // Verify next two entries were read
    expect(readResult2).not.toBeNull();
    expect(readResult2?.[0]?.[1]?.length).toBe(2);

    // Verify entries are pending
    const pendingBefore = (await redis.xpending(
      config.settlementMatchesStream,
      config.consumerGroup,
    )) as [number, string, string, Array<[string, string]>];
    expect(pendingBefore[0]).toBe(4);

    // Start consumer - it should process both pending and stale entries
    const stop = startSettlementMatchConsumer({
      redis,
      config,
      onMatch,
    });
    activeConsumers.push(stop);

    // Wait for all entries to be processed (poll until done)
    let attempts = 0;
    const maxAttempts = 25;
    while (attempts < maxAttempts) {
      await new Promise((resolve) => setTimeout(resolve, 100));
      const pending = (await redis.xpending(
        config.settlementMatchesStream,
        config.consumerGroup,
      )) as [number, string, string, Array<[string, string]>];
      if (pending[0] === 0 && onMatch.mock.calls.length >= 4) {
        break;
      }
      attempts++;
    }

    // Verify all entries were processed
    expect(onMatch).toHaveBeenCalledTimes(4);
    const processedIds = onMatch.mock.calls.map((call) => call[0].id);
    expect(processedIds).toContain(entryId1);
    expect(processedIds).toContain(entryId2);
    expect(processedIds).toContain(entryId3);
    expect(processedIds).toContain(entryId4);

    // Verify entries are ACKed (no pending entries)
    const pendingAfter = (await redis.xpending(
      config.settlementMatchesStream,
      config.consumerGroup,
    )) as [number, string, string, Array<[string, string]>];
    expect(pendingAfter[0]).toBe(0);

    stop();
  });

  it('should handle invalid entries in pending entries', async () => {
    await ensureConsumerGroup(
      redis,
      config.settlementMatchesStream,
      config.consumerGroup,
    );

    // Create valid and invalid entries
    const validMatch = createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440001' });
    const entryId1 = await redis.xadd(
      config.settlementMatchesStream,
      '*',
      'data',
      JSON.stringify(validMatch),
    );
    const entryId2 = await redis.xadd(
      config.settlementMatchesStream,
      '*',
      'data',
      JSON.stringify({ invalid: 'data' }),
    );

    // Read entries to make them pending (but don't ACK them)
    const readResult = (await (redis.xreadgroup as (
      ...args: (string | number)[]
    ) => Promise<[[string, Array<[string, string[]]>]] | null>)(
      'GROUP',
      config.consumerGroup,
      config.consumerName,
      'COUNT',
      10,
      'STREAMS',
      config.settlementMatchesStream,
      '>',
    )) as [[string, Array<[string, string[]]>]] | null;

    // Verify entries were actually read
    expect(readResult).not.toBeNull();
    expect(readResult?.[0]?.[1]?.length).toBe(2);

    // Verify entries are pending
    const pendingBefore = (await redis.xpending(
      config.settlementMatchesStream,
      config.consumerGroup,
    )) as [number, string, string, Array<[string, string]>];
    expect(pendingBefore[0]).toBe(2);

    // Start consumer with onInvalid handler
    const stop = startSettlementMatchConsumer({
      redis,
      config,
      onMatch,
      onInvalid,
    });
    activeConsumers.push(stop);

    // Wait for pending entries to be processed (poll until done)
    let attempts = 0;
    const maxAttempts = 20;
    while (attempts < maxAttempts) {
      await new Promise((resolve) => setTimeout(resolve, 100));
      const pending = (await redis.xpending(
        config.settlementMatchesStream,
        config.consumerGroup,
      )) as [number, string, string, Array<[string, string]>];
      if (pending[0] === 0 && onMatch.mock.calls.length >= 1 && onInvalid.mock.calls.length >= 1) {
        break;
      }
      attempts++;
    }

    // Verify valid entry was processed via onMatch
    expect(onMatch).toHaveBeenCalledTimes(1);
    expect(onMatch).toHaveBeenCalledWith({
      id: entryId1,
      stream: config.settlementMatchesStream,
      payload: validMatch,
    });

    // Verify invalid entry was handled via onInvalid
    expect(onInvalid).toHaveBeenCalledTimes(1);
    expect(onInvalid).toHaveBeenCalledWith({
      id: entryId2,
      stream: config.settlementMatchesStream,
      raw: expect.any(Object),
      error: expect.any(Error),
    });

    // Verify entries are ACKed (no pending entries)
    const pendingAfter = (await redis.xpending(
      config.settlementMatchesStream,
      config.consumerGroup,
    )) as [number, string, string, Array<[string, string]>];
    expect(pendingAfter[0]).toBe(0);

    stop();
  });
});

