import type { AppConfig } from '../../config';

/**
 * Creates a test configuration with sensible defaults for testing.
 * All values can be overridden as needed for specific test scenarios.
 *
 * @param overrides - Optional fields to override in the test configuration.
 * @returns A test AppConfig object.
 */
export const createTestConfig = (overrides?: Partial<AppConfig>): AppConfig => {
  const defaults: AppConfig = {
    redisUrl: process.env.REDIS_TEST_URL || 'redis://localhost:6379',
    settlementMatchesStream: 'test',
    consumerGroup: 'test',
    consumerName: 'test',
    readBlockMs: 100,
    readCount: 10,
    streamMaxLen: 10000,
  };

  return {
    ...defaults,
    ...overrides,
  };
};

