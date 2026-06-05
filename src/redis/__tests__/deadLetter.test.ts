import type Redis from 'ioredis';
import { readMatches } from '../settlementMatchConsumer';
import { createInvalidEntryHandler } from '../deadLetter';
import type { AppConfig } from '../../config';
import {
  createIsolatedTestEnvironment,
  type IsolatedTestEnvironment,
} from '../../tests/helpers/redisTestClient';
import { createIsolatedTestConfig } from '../../tests/helpers/testConfig';
import { REDIS_STREAMS } from '../../schemas/match';

/**
 * L5 — invalid stream entries must be dead-lettered, not silently dropped.
 *
 * Uses a real Redis instance (same pattern as the other consumer suites).
 * @requires Redis server running (default: localhost:6379, or set REDIS_TEST_URL)
 */
describe('dead-letter invalid match entries (L5)', () => {
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
    // Best-effort cleanup of the (shared) dead-letter stream entries we created.
    try {
      await redis.del(REDIS_STREAMS.SETTLEMENT_MATCHES_DEAD);
    } catch {
      // ignore
    }
    await testEnv.cleanup();
  }, 30000);

  it('XADDs an invalid entry to the dead-letter stream with the raw payload, and still XACKs the main stream', async () => {
    const onInvalid = createInvalidEntryHandler(redis);
    const rawPayload = { matchId: 'not-a-uuid', oops: 'bad' };

    const entryId = await redis.xadd(
      config.settlementMatchesStream,
      '*',
      'data',
      JSON.stringify(rawPayload),
    );

    const matches = await readMatches({
      redis,
      stream: config.settlementMatchesStream,
      consumerGroup: config.consumerGroup,
      consumerName: config.consumerName,
      readCount: config.readCount,
      onInvalid,
    });

    // No valid match returned.
    expect(matches).toHaveLength(0);

    // (a) It was XADDed to the dead-letter stream with the raw payload.
    const deadEntries = (await redis.xrange(
      REDIS_STREAMS.SETTLEMENT_MATCHES_DEAD,
      '-',
      '+',
    )) as [string, string[]][];
    expect(deadEntries).toHaveLength(1);

    const fields = deadEntries[0]?.[1] ?? [];
    const fieldMap: Record<string, string> = {};
    for (let i = 0; i < fields.length; i += 2) {
      fieldMap[fields[i] as string] = fields[i + 1] as string;
    }

    expect(fieldMap.id).toBe(entryId);
    expect(fieldMap.stream).toBe(config.settlementMatchesStream);
    expect(fieldMap.error).toBeDefined();
    expect(fieldMap.ts).toBeDefined();
    // raw is the original stream field object that carried the bad payload in
    // its `data` field — the offending content is preserved verbatim.
    const recoveredRaw = JSON.parse(fieldMap.raw as string) as {
      data?: string;
    };
    expect(recoveredRaw.data).toBe(JSON.stringify(rawPayload));

    // (b) The invalid entry is still ACKed on the main stream (no pending).
    const pending = (await redis.xpending(
      config.settlementMatchesStream,
      config.consumerGroup,
    )) as [number, string | null, string | null, unknown];
    expect(pending[0]).toBe(0);
  });
});
