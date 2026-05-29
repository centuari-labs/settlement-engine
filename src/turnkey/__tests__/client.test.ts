jest.unmock('../client');

jest.mock('@turnkey/sdk-server', () => ({
  Turnkey: jest.fn().mockImplementation(() => ({
    apiClient: jest.fn().mockReturnValue({}),
  })),
}));

jest.mock('@turnkey/viem', () => ({
  createAccountWithAddress: jest.fn().mockReturnValue({
    address: '0xabcdef1234567890abcdef1234567890abcdef12',
  }),
}));

import type { AppConfig } from '../../config';

const makeConfig = (overrides?: Partial<AppConfig>): AppConfig => ({
  redisUrl: 'redis://localhost:6379',
  settlementMatchesStream: 'settlement:matches',
  consumerGroup: 'settlement-engine',
  consumerName: 'test-consumer',
  readBlockMs: 5000,
  readCount: 10,
  streamMaxLen: 10000,
  batchSize: 10,
  batchIntervalMs: 5000,
  pollIntervalMs: 200,
  pendingReclaimIntervalMs: 60000,
  xclaimMinIdleMs: 60000,
  failureBackoffBaseMs: 1000,
  failureBackoffMaxMs: 60000,
  settlementContractAddress: '0x1234567890123456789012345678901234567890',
  ethereumRpcUrl: 'https://rpc.example.com',
  turnkeyApiPublicKey: 'test-pub-key',
  turnkeyApiPrivateKey: 'test-priv-key',
  turnkeyOrganizationId: 'test-org-id',
  walletAddress: '0xabcdef1234567890abcdef1234567890abcdef12',
  ethereumChainId: 1,
  nonceLockTtlMs: 30000,
  txConfirmationTimeoutMs: 120000,
  nonceLockRetryDelayMs: 500,
  ...overrides,
});

describe('turnkey/client', () => {
  let clientModule: typeof import('../client');

  beforeEach(async () => {
    jest.clearAllMocks();
    jest.resetModules();
    jest.doMock('@turnkey/sdk-server', () => ({
      Turnkey: jest.fn().mockImplementation(() => ({
        apiClient: jest.fn().mockReturnValue({}),
      })),
    }));
    jest.doMock('@turnkey/viem', () => ({
      createAccountWithAddress: jest.fn().mockReturnValue({
        address: '0xabcdef1234567890abcdef1234567890abcdef12',
      }),
    }));
    clientModule = await import('../client');
  });

  afterEach(() => {
    jest.resetModules();
  });

  it('getTurnkeyClient creates a Turnkey instance with correct config', () => {
    const FreshTurnkey = jest.requireMock('@turnkey/sdk-server').Turnkey;
    const config = makeConfig();
    clientModule.getTurnkeyClient(config);
    expect(FreshTurnkey).toHaveBeenCalledWith({
      apiBaseUrl: 'https://api.turnkey.com',
      apiPublicKey: 'test-pub-key',
      apiPrivateKey: 'test-priv-key',
      defaultOrganizationId: 'test-org-id',
    });
  });

  it('getTurnkeyClient caches the client for the same org+key', () => {
    const FreshTurnkey = jest.requireMock('@turnkey/sdk-server').Turnkey;
    const config = makeConfig();
    const client1 = clientModule.getTurnkeyClient(config);
    const client2 = clientModule.getTurnkeyClient(config);
    expect(client1).toBe(client2);
    expect(FreshTurnkey).toHaveBeenCalledTimes(1);
  });

  it('getSettlementAccount calls createAccountWithAddress with correct args', () => {
    const freshCreateAccount = jest.requireMock('@turnkey/viem').createAccountWithAddress;
    const config = makeConfig();
    clientModule.getSettlementAccount(config);
    expect(freshCreateAccount).toHaveBeenCalledWith(
      expect.objectContaining({
        organizationId: 'test-org-id',
        signWith: '0xabcdef1234567890abcdef1234567890abcdef12',
        ethereumAddress: '0xabcdef1234567890abcdef1234567890abcdef12',
      }),
    );
  });

  it('getSettlementAccount caches the account', () => {
    const freshCreateAccount = jest.requireMock('@turnkey/viem').createAccountWithAddress;
    const config = makeConfig();
    const account1 = clientModule.getSettlementAccount(config);
    const account2 = clientModule.getSettlementAccount(config);
    expect(account1).toBe(account2);
    expect(freshCreateAccount).toHaveBeenCalledTimes(1);
  });
});
