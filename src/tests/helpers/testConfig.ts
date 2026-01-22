import type { AppConfig } from '../../config';

/**
 * Generate a unique test identifier using timestamp and random suffix.
 * This provides better entropy than Date.now() alone to avoid collisions
 * when tests run in parallel or very close together.
 *
 * @returns A unique identifier string.
 */
export const generateUniqueTestId = (): string => {
  const timestamp = Date.now();
  const random = Math.random().toString(36).substring(2, 8);
  return `${timestamp}-${random}`;
};

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
    batchSize: 10,
    batchIntervalMs: 5000,
    pollIntervalMs: 200,
    settlementContractAddress:
      process.env.SETTLEMENT_CONTRACT_ADDRESS ||
      '0x0000000000000000000000000000000000000000',
    ethereumRpcUrl: process.env.ETHEREUM_RPC_URL || 'http://localhost:8545',
    settlementPrivateKey:
      process.env.SETTLEMENT_PRIVATE_KEY ||
      '0x0000000000000000000000000000000000000000000000000000000000000001',
    ethereumChainId: Number(process.env.ETHEREUM_CHAIN_ID || '1'),
  };

  return {
    ...defaults,
    ...overrides,
  };
};

/**
 * Creates a test configuration with unique stream and consumer group names.
 * Use this for tests that need isolation from other tests.
 *
 * @param overrides - Optional fields to override in the test configuration.
 * @returns A test AppConfig object with unique identifiers.
 */
export const createIsolatedTestConfig = (overrides?: Partial<AppConfig>): AppConfig => {
  const uniqueId = generateUniqueTestId();
  return createTestConfig({
    settlementMatchesStream: `test:settlement:matches:${uniqueId}`,
    consumerGroup: `test-settlement-engine-${uniqueId}`,
    consumerName: `test-consumer-${uniqueId}`,
    ...overrides,
  });
};

