jest.unmock('../policy');
jest.unmock('../client');

const mockGetPolicies = jest.fn();
const mockCreatePolicy = jest.fn();
const mockApiClient = jest.fn().mockReturnValue({
  getPolicies: mockGetPolicies,
  createPolicy: mockCreatePolicy,
});

jest.mock('../client', () => ({
  getTurnkeyClient: jest.fn().mockReturnValue({
    apiClient: mockApiClient,
  }),
}));

import type { AppConfig } from '../../config';
import { ensureTurnkeyPolicy } from '../policy';

const makeConfig = (contractAddress = '0xAbCd1234567890123456789012345678901234Ab'): AppConfig => ({
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
  settlementContractAddress: contractAddress,
  ethereumRpcUrl: 'https://rpc.example.com',
  turnkeyApiPublicKey: 'test-pub-key',
  turnkeyApiPrivateKey: 'test-priv-key',
  turnkeyOrganizationId: 'test-org-id',
  walletAddress: '0xabcdef1234567890abcdef1234567890abcdef12',
  ethereumChainId: 1,
  nonceLockTtlMs: 30000,
  txConfirmationTimeoutMs: 120000,
  nonceLockRetryDelayMs: 500,
});

describe('ensureTurnkeyPolicy', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('does not create policy when matching policy already exists', async () => {
    const config = makeConfig();
    const expectedCondition = `eth.tx.to == '${config.settlementContractAddress.toLowerCase()}'`;
    mockGetPolicies.mockResolvedValue({
      policies: [{ condition: expectedCondition, effect: 'EFFECT_ALLOW' }],
    });

    await ensureTurnkeyPolicy(config);

    expect(mockCreatePolicy).not.toHaveBeenCalled();
  });

  it('normalizes condition comparison (case-insensitive, trimmed)', async () => {
    const config = makeConfig();
    mockGetPolicies.mockResolvedValue({
      policies: [
        {
          condition: `  ETH.TX.TO == '${config.settlementContractAddress.toLowerCase()}'  `,
          effect: 'EFFECT_ALLOW',
        },
      ],
    });

    await ensureTurnkeyPolicy(config);

    expect(mockCreatePolicy).not.toHaveBeenCalled();
  });

  it('creates policy when no matching policy exists', async () => {
    const config = makeConfig();
    mockGetPolicies.mockResolvedValue({ policies: [] });
    mockCreatePolicy.mockResolvedValue({});

    await ensureTurnkeyPolicy(config);

    expect(mockCreatePolicy).toHaveBeenCalledWith(
      expect.objectContaining({
        effect: 'EFFECT_ALLOW',
        condition: `eth.tx.to == '${config.settlementContractAddress.toLowerCase()}'`,
      }),
    );
  });

  it('creates policy when existing policies have different conditions', async () => {
    const config = makeConfig();
    mockGetPolicies.mockResolvedValue({
      policies: [{ condition: "eth.tx.to == '0xother'", effect: 'EFFECT_ALLOW' }],
    });
    mockCreatePolicy.mockResolvedValue({});

    await ensureTurnkeyPolicy(config);

    expect(mockCreatePolicy).toHaveBeenCalled();
  });

  it('throws when getPolicies fails', async () => {
    const config = makeConfig();
    mockGetPolicies.mockRejectedValue(new Error('network error'));

    await expect(ensureTurnkeyPolicy(config)).rejects.toThrow('Turnkey policy verification failed');
    expect(mockCreatePolicy).not.toHaveBeenCalled();
  });

  it('throws when createPolicy fails', async () => {
    const config = makeConfig();
    mockGetPolicies.mockResolvedValue({ policies: [] });
    mockCreatePolicy.mockRejectedValue(new Error('forbidden'));

    await expect(ensureTurnkeyPolicy(config)).rejects.toThrow('Turnkey policy verification failed');
  });
});
