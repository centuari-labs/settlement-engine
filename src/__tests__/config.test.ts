import { loadConfig } from '../config';

describe('loadConfig', () => {
  const originalEnv = process.env;

  const requiredEnv = {
    SETTLEMENT_CONTRACT_ADDRESS: '0x1234567890123456789012345678901234567890',
    ETHEREUM_RPC_URL: 'https://rpc.example.com',
    TURNKEY_API_PUBLIC_KEY: 'test-public-key',
    TURNKEY_API_PRIVATE_KEY: 'test-private-key',
    TURNKEY_ORGANIZATION_ID: 'test-org-id',
    TURNKEY_WALLET_ACCOUNT_ADDRESS: '0xabcdef1234567890abcdef1234567890abcdef12',
  };

  beforeEach(() => {
    // Reset process.env to a clean state with only required vars
    jest.resetModules();
    process.env = { ...requiredEnv };
  });

  afterEach(() => {
    process.env = originalEnv;
  });

  it('should apply all defaults when only required vars are set', () => {
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
    expect(config.pendingReclaimIntervalMs).toBe(60000);
    expect(config.xclaimMinIdleMs).toBe(60000);
    expect(config.failureBackoffBaseMs).toBe(1000);
    expect(config.failureBackoffMaxMs).toBe(60000);
    expect(config.ethereumChainId).toBe(1);
    expect(config.nonceLockTtlMs).toBe(30000);
    expect(config.txConfirmationTimeoutMs).toBe(120000);
    expect(config.nonceLockRetryDelayMs).toBe(500);
  });

  it('should coerce numeric environment variables from strings', () => {
    process.env.REDIS_READ_BLOCK_MS = '3000';
    process.env.REDIS_READ_COUNT = '25';
    process.env.SETTLEMENT_BATCH_SIZE = '20';
    process.env.SETTLEMENT_BATCH_INTERVAL_MS = '10000';
    process.env.ETHEREUM_CHAIN_ID = '421614';
    const config = loadConfig();
    expect(config.readBlockMs).toBe(3000);
    expect(config.readCount).toBe(25);
    expect(config.batchSize).toBe(20);
    expect(config.batchIntervalMs).toBe(10000);
    expect(config.ethereumChainId).toBe(421614);
  });

  it('should map Turnkey credential fields correctly', () => {
    const config = loadConfig();
    expect(config.turnkeyApiPublicKey).toBe('test-public-key');
    expect(config.turnkeyApiPrivateKey).toBe('test-private-key');
    expect(config.turnkeyOrganizationId).toBe('test-org-id');
    expect(config.walletAddress).toBe('0xabcdef1234567890abcdef1234567890abcdef12');
  });

  it('should throw when TURNKEY_API_PUBLIC_KEY is missing', () => {
    delete process.env.TURNKEY_API_PUBLIC_KEY;
    expect(() => loadConfig()).toThrow();
  });

  it('should throw when TURNKEY_API_PRIVATE_KEY is missing', () => {
    delete process.env.TURNKEY_API_PRIVATE_KEY;
    expect(() => loadConfig()).toThrow();
  });

  it('should throw when TURNKEY_ORGANIZATION_ID is missing', () => {
    delete process.env.TURNKEY_ORGANIZATION_ID;
    expect(() => loadConfig()).toThrow();
  });

  it('should throw for invalid TURNKEY_WALLET_ACCOUNT_ADDRESS', () => {
    process.env.TURNKEY_WALLET_ACCOUNT_ADDRESS = 'not-an-address';
    expect(() => loadConfig()).toThrow();
  });

  it('should throw for invalid contract address', () => {
    process.env.SETTLEMENT_CONTRACT_ADDRESS = 'not-an-address';
    expect(() => loadConfig()).toThrow();
  });

  it('should throw for invalid RPC URL', () => {
    process.env.ETHEREUM_RPC_URL = 'not-a-url';
    expect(() => loadConfig()).toThrow();
  });

  it('should throw when required vars are missing', () => {
    delete process.env.SETTLEMENT_CONTRACT_ADDRESS;
    expect(() => loadConfig()).toThrow();
  });

  it('should use custom values when provided', () => {
    process.env.REDIS_URL = 'redis://custom:6380';
    process.env.REDIS_STREAM_SETTLEMENT_MATCHES = 'custom:stream';
    process.env.REDIS_CONSUMER_GROUP = 'custom-group';
    process.env.REDIS_CONSUMER_NAME = 'custom-consumer';
    const config = loadConfig();
    expect(config.redisUrl).toBe('redis://custom:6380');
    expect(config.settlementMatchesStream).toBe('custom:stream');
    expect(config.consumerGroup).toBe('custom-group');
    expect(config.consumerName).toBe('custom-consumer');
  });
});
