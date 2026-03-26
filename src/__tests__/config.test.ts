import { loadConfig } from '../config';

/**
 * Unit tests for loadConfig().
 * Validates environment variable parsing, defaults, and error handling via Zod.
 */
describe('loadConfig', () => {
  const REQUIRED_ENV = {
    SETTLEMENT_CONTRACT_ADDRESS: '0x1234567890123456789012345678901234567890',
    ETHEREUM_RPC_URL: 'http://localhost:8545',
    SETTLEMENT_PRIVATE_KEY:
      'abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890',
  };

  let savedEnv: NodeJS.ProcessEnv;

  beforeEach(() => {
    savedEnv = { ...process.env };
    // Clear all config-related env vars so defaults are exercised
    const configKeys = [
      'REDIS_URL',
      'REDIS_STREAM_SETTLEMENT_MATCHES',
      'REDIS_CONSUMER_GROUP',
      'REDIS_CONSUMER_NAME',
      'REDIS_READ_BLOCK_MS',
      'REDIS_READ_COUNT',
      'REDIS_STREAM_MAXLEN',
      'SETTLEMENT_BATCH_SIZE',
      'SETTLEMENT_BATCH_INTERVAL_MS',
      'SETTLEMENT_POLL_INTERVAL_MS',
      'SETTLEMENT_CONTRACT_ADDRESS',
      'ETHEREUM_RPC_URL',
      'SETTLEMENT_PRIVATE_KEY',
      'ETHEREUM_CHAIN_ID',
    ];
    for (const key of configKeys) {
      delete process.env[key];
    }
  });

  afterEach(() => {
    process.env = savedEnv;
  });

  it('should load config with all defaults when only required vars are set', () => {
    Object.assign(process.env, REQUIRED_ENV);

    const config = loadConfig();

    expect(config.redisUrl).toBe('redis://localhost:6379');
    expect(config.settlementMatchesStream).toBe('settlement:matches');
    expect(config.consumerGroup).toBe('settlement-engine');
    expect(config.consumerName).toBe('settlement-engine-1');
    expect(config.readBlockMs).toBe(5000);
    expect(config.readCount).toBe(10);
    expect(config.streamMaxLen).toBe(10000);
    expect(config.batchSize).toBe(10);
    expect(config.batchIntervalMs).toBe(5000);
    expect(config.pollIntervalMs).toBe(200);
    expect(config.ethereumChainId).toBe(1);
    expect(config.settlementContractAddress).toBe(
      REQUIRED_ENV.SETTLEMENT_CONTRACT_ADDRESS,
    );
    expect(config.ethereumRpcUrl).toBe(REQUIRED_ENV.ETHEREUM_RPC_URL);
    expect(config.settlementPrivateKey).toBe(
      REQUIRED_ENV.SETTLEMENT_PRIVATE_KEY,
    );
  });

  it('should accept overridden numeric env vars', () => {
    Object.assign(process.env, REQUIRED_ENV, {
      REDIS_READ_COUNT: '50',
      SETTLEMENT_BATCH_SIZE: '25',
      SETTLEMENT_BATCH_INTERVAL_MS: '10000',
      SETTLEMENT_POLL_INTERVAL_MS: '500',
      REDIS_STREAM_MAXLEN: '5000',
      ETHEREUM_CHAIN_ID: '42161',
    });

    const config = loadConfig();

    expect(config.readCount).toBe(50);
    expect(config.batchSize).toBe(25);
    expect(config.batchIntervalMs).toBe(10000);
    expect(config.pollIntervalMs).toBe(500);
    expect(config.streamMaxLen).toBe(5000);
    expect(config.ethereumChainId).toBe(42161);
  });

  it('should accept private key with 0x prefix', () => {
    Object.assign(process.env, REQUIRED_ENV, {
      SETTLEMENT_PRIVATE_KEY:
        '0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890',
    });

    const config = loadConfig();

    expect(config.settlementPrivateKey).toBe(
      '0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890',
    );
  });

  it('should accept custom Redis URL', () => {
    Object.assign(process.env, REQUIRED_ENV, {
      REDIS_URL: 'redis://custom-host:6380',
    });

    const config = loadConfig();

    expect(config.redisUrl).toBe('redis://custom-host:6380');
  });

  // --- Error cases ---

  it('should throw when SETTLEMENT_CONTRACT_ADDRESS is missing', () => {
    Object.assign(process.env, REQUIRED_ENV);
    delete process.env.SETTLEMENT_CONTRACT_ADDRESS;

    expect(() => loadConfig()).toThrow();
  });

  it('should throw when ETHEREUM_RPC_URL is missing', () => {
    Object.assign(process.env, REQUIRED_ENV);
    delete process.env.ETHEREUM_RPC_URL;

    expect(() => loadConfig()).toThrow();
  });

  it('should throw when SETTLEMENT_PRIVATE_KEY is missing', () => {
    Object.assign(process.env, REQUIRED_ENV);
    delete process.env.SETTLEMENT_PRIVATE_KEY;

    expect(() => loadConfig()).toThrow();
  });

  it('should throw for invalid contract address (not hex)', () => {
    Object.assign(process.env, REQUIRED_ENV, {
      SETTLEMENT_CONTRACT_ADDRESS: 'not-an-address',
    });

    expect(() => loadConfig()).toThrow();
  });

  it('should throw for invalid contract address (too short)', () => {
    Object.assign(process.env, REQUIRED_ENV, {
      SETTLEMENT_CONTRACT_ADDRESS: '0x1234',
    });

    expect(() => loadConfig()).toThrow();
  });

  it('should throw for invalid RPC URL', () => {
    Object.assign(process.env, REQUIRED_ENV, {
      ETHEREUM_RPC_URL: 'not-a-url',
    });

    expect(() => loadConfig()).toThrow();
  });

  it('should throw for invalid private key (too short)', () => {
    Object.assign(process.env, REQUIRED_ENV, {
      SETTLEMENT_PRIVATE_KEY: 'abcd',
    });

    expect(() => loadConfig()).toThrow();
  });

  it('should throw for invalid private key (non-hex characters)', () => {
    Object.assign(process.env, REQUIRED_ENV, {
      SETTLEMENT_PRIVATE_KEY:
        'zzzzzz1234567890abcdef1234567890abcdef1234567890abcdef1234567890',
    });

    expect(() => loadConfig()).toThrow();
  });

  it('should throw for non-positive numeric values (zero batch size)', () => {
    Object.assign(process.env, REQUIRED_ENV, {
      SETTLEMENT_BATCH_SIZE: '0',
    });

    expect(() => loadConfig()).toThrow();
  });

  it('should throw for negative numeric values', () => {
    Object.assign(process.env, REQUIRED_ENV, {
      REDIS_READ_COUNT: '-5',
    });

    expect(() => loadConfig()).toThrow();
  });

  it('should throw for non-integer numeric values', () => {
    Object.assign(process.env, REQUIRED_ENV, {
      SETTLEMENT_BATCH_SIZE: '3.5',
    });

    expect(() => loadConfig()).toThrow();
  });
});
