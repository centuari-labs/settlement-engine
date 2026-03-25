import { loadConfig, type AppConfig } from '../config';

/**
 * Unit tests for configuration loading and validation.
 */
describe('loadConfig', () => {
  const validEnv = {
    REDIS_URL: 'redis://localhost:6379',
    REDIS_STREAM_SETTLEMENT_MATCHES: 'settlement:matches',
    REDIS_CONSUMER_GROUP: 'settlement-engine',
    REDIS_CONSUMER_NAME: 'settlement-engine-1',
    REDIS_READ_BLOCK_MS: '5000',
    REDIS_READ_COUNT: '10',
    REDIS_STREAM_MAXLEN: '10000',
    SETTLEMENT_BATCH_SIZE: '10',
    SETTLEMENT_BATCH_INTERVAL_MS: '5000',
    SETTLEMENT_POLL_INTERVAL_MS: '200',
    SETTLEMENT_CONTRACT_ADDRESS: '0x1234567890123456789012345678901234567890',
    ETHEREUM_RPC_URL: 'http://localhost:8545',
    SETTLEMENT_PRIVATE_KEY: '0x0000000000000000000000000000000000000000000000000000000000000001',
    ETHEREUM_CHAIN_ID: '1',
  };

  let originalEnv: NodeJS.ProcessEnv;

  beforeEach(() => {
    originalEnv = { ...process.env };
    // Clear relevant env vars
    for (const key of Object.keys(validEnv)) {
      delete process.env[key];
    }
  });

  afterEach(() => {
    process.env = originalEnv;
  });

  it('should load config with all valid environment variables', () => {
    Object.assign(process.env, validEnv);

    const config = loadConfig();

    expect(config).toEqual({
      redisUrl: 'redis://localhost:6379',
      settlementMatchesStream: 'settlement:matches',
      consumerGroup: 'settlement-engine',
      consumerName: 'settlement-engine-1',
      readBlockMs: 5000,
      readCount: 10,
      streamMaxLen: 10000,
      batchSize: 10,
      batchIntervalMs: 5000,
      pollIntervalMs: 200,
      settlementContractAddress: '0x1234567890123456789012345678901234567890',
      ethereumRpcUrl: 'http://localhost:8545',
      settlementPrivateKey: '0x0000000000000000000000000000000000000000000000000000000000000001',
      ethereumChainId: 1,
    });
  });

  it('should use default values for optional fields', () => {
    // Only set required fields (no defaults)
    process.env.SETTLEMENT_CONTRACT_ADDRESS = '0x1234567890123456789012345678901234567890';
    process.env.ETHEREUM_RPC_URL = 'http://localhost:8545';
    process.env.SETTLEMENT_PRIVATE_KEY = '0000000000000000000000000000000000000000000000000000000000000001';

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
  });

  it('should throw when SETTLEMENT_CONTRACT_ADDRESS is missing', () => {
    process.env.ETHEREUM_RPC_URL = 'http://localhost:8545';
    process.env.SETTLEMENT_PRIVATE_KEY = '0000000000000000000000000000000000000000000000000000000000000001';

    expect(() => loadConfig()).toThrow();
  });

  it('should throw when ETHEREUM_RPC_URL is missing', () => {
    process.env.SETTLEMENT_CONTRACT_ADDRESS = '0x1234567890123456789012345678901234567890';
    process.env.SETTLEMENT_PRIVATE_KEY = '0000000000000000000000000000000000000000000000000000000000000001';

    expect(() => loadConfig()).toThrow();
  });

  it('should throw when SETTLEMENT_PRIVATE_KEY is missing', () => {
    process.env.SETTLEMENT_CONTRACT_ADDRESS = '0x1234567890123456789012345678901234567890';
    process.env.ETHEREUM_RPC_URL = 'http://localhost:8545';

    expect(() => loadConfig()).toThrow();
  });

  it('should throw for invalid SETTLEMENT_CONTRACT_ADDRESS', () => {
    Object.assign(process.env, validEnv);
    process.env.SETTLEMENT_CONTRACT_ADDRESS = 'not-an-address';

    expect(() => loadConfig()).toThrow();
  });

  it('should throw for invalid ETHEREUM_RPC_URL', () => {
    Object.assign(process.env, validEnv);
    process.env.ETHEREUM_RPC_URL = 'not-a-url';

    expect(() => loadConfig()).toThrow();
  });

  it('should throw for invalid SETTLEMENT_PRIVATE_KEY', () => {
    Object.assign(process.env, validEnv);
    process.env.SETTLEMENT_PRIVATE_KEY = 'too-short';

    expect(() => loadConfig()).toThrow();
  });

  it('should accept private key without 0x prefix', () => {
    Object.assign(process.env, validEnv);
    process.env.SETTLEMENT_PRIVATE_KEY = '0000000000000000000000000000000000000000000000000000000000000001';

    const config = loadConfig();

    expect(config.settlementPrivateKey).toBe('0000000000000000000000000000000000000000000000000000000000000001');
  });

  it('should accept private key with 0x prefix', () => {
    Object.assign(process.env, validEnv);
    process.env.SETTLEMENT_PRIVATE_KEY = '0x0000000000000000000000000000000000000000000000000000000000000001';

    const config = loadConfig();

    expect(config.settlementPrivateKey).toBe('0x0000000000000000000000000000000000000000000000000000000000000001');
  });

  it('should parse numeric string values correctly', () => {
    Object.assign(process.env, validEnv);
    process.env.REDIS_READ_COUNT = '42';
    process.env.SETTLEMENT_BATCH_SIZE = '25';
    process.env.ETHEREUM_CHAIN_ID = '137';

    const config = loadConfig();

    expect(config.readCount).toBe(42);
    expect(config.batchSize).toBe(25);
    expect(config.ethereumChainId).toBe(137);
  });

  it('should throw for non-positive numeric values', () => {
    Object.assign(process.env, validEnv);
    process.env.REDIS_READ_COUNT = '0';

    expect(() => loadConfig()).toThrow();
  });

  it('should throw for negative numeric values', () => {
    Object.assign(process.env, validEnv);
    process.env.SETTLEMENT_BATCH_SIZE = '-5';

    expect(() => loadConfig()).toThrow();
  });

  it('should return readonly config object type', () => {
    Object.assign(process.env, validEnv);

    const config = loadConfig();

    // TypeScript compile-time check: all fields should be readonly
    // Runtime check: config is a plain object
    expect(typeof config.redisUrl).toBe('string');
    expect(typeof config.batchSize).toBe('number');
    expect(typeof config.readBlockMs).toBe('number');
  });
});
