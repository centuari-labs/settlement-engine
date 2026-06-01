/**
 * Tests for smartContract.ts — the actual implementation, NOT the global mock.
 *
 * Since setup.ts globally mocks this module, we must unmock it here.
 * We mock viem functions instead to avoid real blockchain calls.
 */

// Unmock the module under test (setup.ts globally mocks it)
jest.unmock('../smartContract');

// Mock viem at the module level
jest.mock('viem', () => {
  const actual = jest.requireActual('viem');
  return {
    ...actual,
    createPublicClient: jest.fn(),
    createWalletClient: jest.fn(),
    decodeEventLog: jest.fn(),
  };
});

jest.mock('viem/accounts', () => ({
  privateKeyToAccount: jest.fn().mockReturnValue({
    address: '0xSettlerAddress1234567890123456789012345678',
  }),
}));

import {
  createPublicClient,
  createWalletClient,
  decodeEventLog,
} from 'viem';
import type { AppConfig } from '../../config';
import { createMatch } from '../../tests/helpers/testFixtures';

const mockCreatePublicClient = createPublicClient as jest.MockedFunction<
  typeof createPublicClient
>;
const mockCreateWalletClient = createWalletClient as jest.MockedFunction<
  typeof createWalletClient
>;
const mockDecodeEventLog = decodeEventLog as jest.MockedFunction<
  typeof decodeEventLog
>;

const createTestAppConfig = (overrides?: Partial<AppConfig>): AppConfig => ({
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
  settlementPrivateKey:
    'abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789',
  ethereumChainId: 421614,
  nonceLockTtlMs: 30000,
  txConfirmationTimeoutMs: 120000,
  nonceLockRetryDelayMs: 500,
  sweeperEnabled: false,
  sweeperIntervalMs: 3600000,
  sweeperStuckThresholdMs: 86400000,
  sweeperBatchSize: 50,
  poisonIsolationEnabled: false,
  poisonIsolationMaxRounds: 1,
  ...overrides,
});

// We need fresh module imports between some tests to reset cached clients
let smartContractModule: typeof import('../smartContract');

describe('smartContract', () => {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  let mockPublicClient: any;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  let mockWalletClient: any;

  beforeEach(() => {
    jest.clearAllMocks();

    // Reset module registry to clear cached viem clients
    jest.resetModules();

    // Re-mock viem after module reset
    jest.doMock('viem', () => {
      const actual = jest.requireActual('viem');
      return {
        ...actual,
        createPublicClient: jest.fn(),
        createWalletClient: jest.fn(),
        decodeEventLog: jest.fn(),
      };
    });
    jest.doMock('viem/accounts', () => ({
      privateKeyToAccount: jest.fn().mockReturnValue({
        address: '0xSettlerAddress1234567890123456789012345678',
      }),
    }));

    mockPublicClient = {
      multicall: jest.fn(),
      waitForTransactionReceipt: jest.fn(),
      getBlock: jest.fn(),
    };
    mockWalletClient = {
      writeContract: jest.fn(),
      account: {
        address: '0xSettlerAddress1234567890123456789012345678',
      },
    };

    // Fresh import to get clean cached state
    const viem = require('viem');
    viem.createPublicClient.mockReturnValue(mockPublicClient);
    viem.createWalletClient.mockReturnValue(mockWalletClient);

    smartContractModule = require('../smartContract');
  });

  describe('getPublicClient', () => {
    it('should create a client on first call', () => {
      const viem = require('viem');
      const config = createTestAppConfig();
      smartContractModule.getPublicClient(config);
      expect(viem.createPublicClient).toHaveBeenCalledTimes(1);
    });

    it('should return cached client on same config', () => {
      const viem = require('viem');
      const config = createTestAppConfig();
      const client1 = smartContractModule.getPublicClient(config);
      const client2 = smartContractModule.getPublicClient(config);
      expect(viem.createPublicClient).toHaveBeenCalledTimes(1);
      expect(client1).toBe(client2);
    });

    it('should create new client when config changes', () => {
      const viem = require('viem');
      const config1 = createTestAppConfig({ ethereumChainId: 1 });
      const config2 = createTestAppConfig({ ethereumChainId: 42161 });
      smartContractModule.getPublicClient(config1);
      smartContractModule.getPublicClient(config2);
      expect(viem.createPublicClient).toHaveBeenCalledTimes(2);
    });
  });

  describe('filterAlreadySettledMatches', () => {
    it('should return empty arrays for empty input', async () => {
      const config = createTestAppConfig();
      const result = await smartContractModule.filterAlreadySettledMatches(
        [],
        config,
      );
      expect(result).toEqual({ unsettled: [], alreadySettled: [] });
      expect(mockPublicClient.multicall).not.toHaveBeenCalled();
    });

    it('should classify all as unsettled when none are settled', async () => {
      const config = createTestAppConfig();
      const matches = [
        {
          id: '1-0',
          stream: 'test',
          payload: createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440001' }),
        },
        {
          id: '2-0',
          stream: 'test',
          payload: createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440002' }),
        },
      ];

      mockPublicClient.multicall.mockResolvedValue([
        { status: 'success', result: false },
        { status: 'success', result: false },
      ]);

      const result = await smartContractModule.filterAlreadySettledMatches(
        matches,
        config,
      );
      expect(result.unsettled).toHaveLength(2);
      expect(result.alreadySettled).toHaveLength(0);
    });

    it('should classify settled matches correctly', async () => {
      const config = createTestAppConfig();
      const matches = [
        {
          id: '1-0',
          stream: 'test',
          payload: createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440001' }),
        },
        {
          id: '2-0',
          stream: 'test',
          payload: createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440002' }),
        },
      ];

      mockPublicClient.multicall.mockResolvedValue([
        { status: 'success', result: true },
        { status: 'success', result: false },
      ]);

      const result = await smartContractModule.filterAlreadySettledMatches(
        matches,
        config,
      );
      expect(result.unsettled).toHaveLength(1);
      expect(result.alreadySettled).toHaveLength(1);
      expect(result.alreadySettled[0].payload.matchId).toBe(
        '550e8400-e29b-41d4-a716-446655440001',
      );
    });

    it('should treat multicall failures as unsettled', async () => {
      const config = createTestAppConfig();
      const matches = [
        {
          id: '1-0',
          stream: 'test',
          payload: createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440001' }),
        },
      ];

      mockPublicClient.multicall.mockResolvedValue([
        { status: 'failure', error: new Error('rpc error') },
      ]);

      const result = await smartContractModule.filterAlreadySettledMatches(
        matches,
        config,
      );
      expect(result.unsettled).toHaveLength(1);
      expect(result.alreadySettled).toHaveLength(0);
    });

    it('should classify all as settled when all are settled', async () => {
      const config = createTestAppConfig();
      const matches = [
        {
          id: '1-0',
          stream: 'test',
          payload: createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440001' }),
        },
        {
          id: '2-0',
          stream: 'test',
          payload: createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440002' }),
        },
      ];

      mockPublicClient.multicall.mockResolvedValue([
        { status: 'success', result: true },
        { status: 'success', result: true },
      ]);

      const result = await smartContractModule.filterAlreadySettledMatches(
        matches,
        config,
      );
      expect(result.unsettled).toHaveLength(0);
      expect(result.alreadySettled).toHaveLength(2);
    });
  });

  describe('settleBatch', () => {
    it('should throw EMPTY_BATCH for empty matches', async () => {
      const config = createTestAppConfig();
      await expect(
        smartContractModule.settleBatch({ matches: [], config }),
      ).rejects.toMatchObject({
        code: 'EMPTY_BATCH',
        retryable: false,
        failedMatchIds: [],
      });
    });

    it('should settle a batch successfully', async () => {
      const config = createTestAppConfig();
      const matches = [createMatch()];
      const txHash = '0xabc123' as `0x${string}`;

      mockWalletClient.writeContract.mockResolvedValue(txHash);
      mockPublicClient.waitForTransactionReceipt.mockResolvedValue({
        status: 'success',
        transactionHash: txHash,
        blockNumber: 100n,
        gasUsed: 50000n,
        logs: [],
      });
      mockPublicClient.getBlock.mockResolvedValue({
        timestamp: 1700000000n,
      });

      const result = await smartContractModule.settleBatch({ matches, config });
      expect(result.transactionHash).toBe(txHash);
      expect(result.blockNumber).toBe(100);
      expect(result.gasUsed).toBe(50000);
      expect(result.timestamp).toBe(1700000000000); // seconds * 1000
      expect(result.settledMatchIds).toEqual([matches[0].matchId]);
    });

    describe('collateralAssets encoding (Phase 3)', () => {
      const settleAndCaptureContractMatches = async (
        matches: ReturnType<typeof createMatch>[],
        collateralAssetsByBorrower?: ReadonlyMap<string, readonly `0x${string}`[]>,
      ) => {
        const config = createTestAppConfig();
        mockWalletClient.writeContract.mockResolvedValue(
          '0xabc' as `0x${string}`,
        );
        mockPublicClient.waitForTransactionReceipt.mockResolvedValue({
          status: 'success',
          transactionHash: '0xabc',
          blockNumber: 100n,
          gasUsed: 50000n,
          logs: [],
        });
        mockPublicClient.getBlock.mockResolvedValue({
          timestamp: 1700000000n,
        });

        await smartContractModule.settleBatch({
          matches,
          config,
          collateralAssetsByBorrower,
        });

        const writeArgs = mockWalletClient.writeContract.mock.calls[0]![0];
        return writeArgs.args[0] as {
          borrower: `0x${string}`;
          collateralAssets: `0x${string}`[];
        }[];
      };

      it('encodes empty collateralAssets when no map is provided', async () => {
        const matches = [createMatch()];
        const contractMatches = await settleAndCaptureContractMatches(matches);
        expect(contractMatches[0]!.collateralAssets).toEqual([]);
      });

      it('encodes empty collateralAssets when the borrower has no queued flags', async () => {
        const matches = [
          createMatch({
            borrowerWallet: '0xbbbb000000000000000000000000000000000001',
          }),
        ];
        const lookup = new Map<string, readonly `0x${string}`[]>();
        const contractMatches = await settleAndCaptureContractMatches(
          matches,
          lookup,
        );
        expect(contractMatches[0]!.collateralAssets).toEqual([]);
      });

      it('encodes the borrower queued assets via lowercase lookup', async () => {
        const borrower = '0xBBBB000000000000000000000000000000000001';
        const btc = '0xcccc000000000000000000000000000000000003' as `0x${string}`;
        const eth = '0xdddd000000000000000000000000000000000004' as `0x${string}`;

        const matches = [createMatch({ borrowerWallet: borrower })];
        const lookup = new Map<string, readonly `0x${string}`[]>([
          [borrower.toLowerCase(), [btc, eth]],
        ]);

        const contractMatches = await settleAndCaptureContractMatches(
          matches,
          lookup,
        );

        expect(contractMatches[0]!.collateralAssets).toEqual([btc, eth]);
      });

      it('keeps per-borrower arrays distinct in a multi-borrower batch', async () => {
        const borrowerA = '0xaaaa000000000000000000000000000000000001';
        const borrowerB = '0xbbbb000000000000000000000000000000000002';
        const assetX = '0xcccc000000000000000000000000000000000003' as `0x${string}`;
        const assetY = '0xdddd000000000000000000000000000000000004' as `0x${string}`;

        const matches = [
          createMatch({ matchId: '00000000-0000-0000-0000-000000000001', borrowerWallet: borrowerA }),
          createMatch({ matchId: '00000000-0000-0000-0000-000000000002', borrowerWallet: borrowerB }),
        ];
        const lookup = new Map<string, readonly `0x${string}`[]>([
          [borrowerA.toLowerCase(), [assetX]],
          [borrowerB.toLowerCase(), [assetY]],
        ]);

        const contractMatches = await settleAndCaptureContractMatches(
          matches,
          lookup,
        );

        expect(contractMatches[0]!.collateralAssets).toEqual([assetX]);
        expect(contractMatches[1]!.collateralAssets).toEqual([assetY]);
      });

      it('de-dupes asset entries defensively even if the caller passes duplicates', async () => {
        const borrower = '0xbbbb000000000000000000000000000000000001';
        const btc = '0xcccc000000000000000000000000000000000003' as `0x${string}`;

        const matches = [createMatch({ borrowerWallet: borrower })];
        const lookup = new Map<string, readonly `0x${string}`[]>([
          [borrower.toLowerCase(), [btc, btc]],
        ]);

        const contractMatches = await settleAndCaptureContractMatches(
          matches,
          lookup,
        );

        expect(contractMatches[0]!.collateralAssets).toEqual([btc]);
      });
    });

    it('should throw retryable error when transaction is reverted', async () => {
      const config = createTestAppConfig();
      const matches = [createMatch()];

      mockWalletClient.writeContract.mockResolvedValue('0xabc' as `0x${string}`);
      mockPublicClient.waitForTransactionReceipt.mockResolvedValue({
        status: 'reverted',
        transactionHash: '0xabc',
        blockNumber: 100n,
        gasUsed: 50000n,
        logs: [],
      });

      // The thrown SettlementError (plain object, not Error instance) gets caught
      // by the outer catch and re-mapped via mapContractError. Since it's not an
      // Error instance, String(error) = "[object Object]" → UNKNOWN_ERROR (retryable).
      await expect(
        smartContractModule.settleBatch({ matches, config }),
      ).rejects.toMatchObject({
        retryable: true,
        failedMatchIds: [matches[0].matchId],
      });
    });

    describe('mapContractError (via settleBatch catch)', () => {
      const config = createTestAppConfig();

      const testErrorMapping = async (
        errorMessage: string,
        expectedCode: string,
        expectedRetryable: boolean,
      ) => {
        const matches = [createMatch()];
        mockWalletClient.writeContract.mockRejectedValue(
          new Error(errorMessage),
        );

        try {
          await smartContractModule.settleBatch({ matches, config });
          fail('Should have thrown');
        } catch (error: unknown) {
          const settlementError = error as {
            code: string;
            retryable: boolean;
            failedMatchIds: string[];
          };
          expect(settlementError.code).toBe(expectedCode);
          expect(settlementError.retryable).toBe(expectedRetryable);
          expect(settlementError.failedMatchIds).toHaveLength(1);
        }
      };

      it('should map AlreadySettled error as non-retryable', async () => {
        await testErrorMapping('AlreadySettled', 'ALREADY_SETTLED', false);
      });

      it('should map AlreadySettled hex selector as non-retryable', async () => {
        await testErrorMapping(
          'execution reverted: 0xb196a44a',
          'ALREADY_SETTLED',
          false,
        );
      });

      it('should map ContractPaused as retryable', async () => {
        await testErrorMapping('ContractPaused', 'CONTRACT_PAUSED', true);
      });

      it('should map EnforcedPause as retryable', async () => {
        await testErrorMapping('EnforcedPause', 'CONTRACT_PAUSED', true);
      });

      it('should map EmptyBatch error as non-retryable', async () => {
        await testErrorMapping('EmptyBatch', 'EMPTY_BATCH', false);
      });

      it('should map InvalidMatchData as non-retryable', async () => {
        await testErrorMapping(
          'InvalidMatchData',
          'INVALID_MATCH_DATA',
          false,
        );
      });

      it('should map InsufficientFunds as non-retryable', async () => {
        await testErrorMapping(
          'InsufficientFunds',
          'INSUFFICIENT_FUNDS',
          false,
        );
      });

      it('should map InvalidMaturity as non-retryable', async () => {
        await testErrorMapping(
          'InvalidMaturity',
          'INVALID_MATURITY',
          false,
        );
      });

      it('should map Unauthorized as non-retryable', async () => {
        await testErrorMapping('Unauthorized', 'CONTRACT_ERROR', false);
      });

      it('should map InvalidAmount as non-retryable', async () => {
        await testErrorMapping('InvalidAmount', 'CONTRACT_ERROR', false);
      });

      it('should map ZeroAddress as non-retryable', async () => {
        await testErrorMapping('ZeroAddress', 'CONTRACT_ERROR', false);
      });

      it('should map BondTokenNotFound as non-retryable', async () => {
        await testErrorMapping('BondTokenNotFound', 'CONTRACT_ERROR', false);
      });

      it('should map AccessControlUnauthorizedAccount as non-retryable', async () => {
        await testErrorMapping(
          'AccessControlUnauthorizedAccount',
          'CONTRACT_ERROR',
          false,
        );
      });

      it('should map ReentrancyGuardReentrantCall as retryable', async () => {
        await testErrorMapping(
          'ReentrancyGuardReentrantCall',
          'REENTRANCY',
          true,
        );
      });

      it('should map network errors as retryable', async () => {
        await testErrorMapping('network error occurred', 'NETWORK_ERROR', true);
      });

      it('should map timeout errors as retryable', async () => {
        await testErrorMapping('request timeout', 'NETWORK_ERROR', true);
      });

      it('should map ECONNREFUSED as retryable', async () => {
        await testErrorMapping('ECONNREFUSED', 'NETWORK_ERROR', true);
      });

      it('should map fetch errors as retryable', async () => {
        await testErrorMapping('fetch failed', 'NETWORK_ERROR', true);
      });

      it('should map unknown errors as retryable by default', async () => {
        await testErrorMapping(
          'some unknown error xyz',
          'UNKNOWN_ERROR',
          true,
        );
      });
    });

    it('should parse BondTokenCreated events from receipt logs', async () => {
      const config = createTestAppConfig();
      const matches = [createMatch()];
      const txHash = '0xabc123' as `0x${string}`;
      const viem = require('viem');

      mockWalletClient.writeContract.mockResolvedValue(txHash);
      mockPublicClient.waitForTransactionReceipt.mockResolvedValue({
        status: 'success',
        transactionHash: txHash,
        blockNumber: 100n,
        gasUsed: 50000n,
        logs: [
          {
            topics: ['0xbondtopic'] as [`0x${string}`],
            data: '0xbonddata' as `0x${string}`,
          },
        ],
      });
      mockPublicClient.getBlock.mockResolvedValue({
        timestamp: 1700000000n,
      });

      // Mock decodeEventLog to return a BondTokenCreated event on first call
      viem.decodeEventLog
        .mockReturnValueOnce({
          eventName: 'BondTokenCreated',
          args: {
            marketId: '0xmarket1',
            bondToken: '0xBondToken123',
            loanToken: '0xLoanToken456',
            maturity: 1735689600n,
            name: 'Bond Token',
            symbol: 'BT',
          },
        })
        .mockImplementation(() => {
          throw new Error('not this event');
        });

      const result = await smartContractModule.settleBatch({ matches, config });
      expect(result.bondTokenEvents).toHaveLength(1);
      expect(result.bondTokenEvents[0].bondToken).toBe('0xbondtoken123'); // lowercased
      expect(result.bondTokenEvents[0].loanToken).toBe('0xloantoken456'); // lowercased
    });

    it('should parse LendPositionCreated events from receipt logs', async () => {
      const config = createTestAppConfig();
      const matches = [createMatch()];
      const txHash = '0xabc123' as `0x${string}`;
      const viem = require('viem');

      mockWalletClient.writeContract.mockResolvedValue(txHash);
      mockPublicClient.waitForTransactionReceipt.mockResolvedValue({
        status: 'success',
        transactionHash: txHash,
        blockNumber: 100n,
        gasUsed: 50000n,
        logs: [
          {
            topics: ['0xlendtopic'] as [`0x${string}`],
            data: '0xlenddata' as `0x${string}`,
          },
        ],
      });
      mockPublicClient.getBlock.mockResolvedValue({
        timestamp: 1700000000n,
      });

      viem.decodeEventLog
        .mockImplementationOnce(() => {
          throw new Error('not BondTokenCreated');
        })
        .mockReturnValueOnce({
          eventName: 'LendPositionCreated',
          args: {
            marketId: '0xmarket1',
            lender: '0xLender123',
            bondToken: '0xBondToken456',
            cbtAmount: 1000000n,
            principal: 500000n,
            rate: 5000n,
          },
        })
        .mockImplementation(() => {
          throw new Error('not this event');
        });

      const result = await smartContractModule.settleBatch({ matches, config });
      expect(result.lendPositionEvents).toHaveLength(1);
      expect(result.lendPositionEvents[0].lender).toBe('0xlender123'); // lowercased
    });

    it('should parse BorrowPositionCreated events from receipt logs', async () => {
      const config = createTestAppConfig();
      const matches = [createMatch()];
      const txHash = '0xabc123' as `0x${string}`;
      const viem = require('viem');

      mockWalletClient.writeContract.mockResolvedValue(txHash);
      mockPublicClient.waitForTransactionReceipt.mockResolvedValue({
        status: 'success',
        transactionHash: txHash,
        blockNumber: 100n,
        gasUsed: 50000n,
        logs: [
          {
            topics: ['0xborrowtopic'] as [`0x${string}`],
            data: '0xborrowdata' as `0x${string}`,
          },
        ],
      });
      mockPublicClient.getBlock.mockResolvedValue({
        timestamp: 1700000000n,
      });

      viem.decodeEventLog
        .mockImplementationOnce(() => {
          throw new Error('not BondTokenCreated');
        })
        .mockImplementationOnce(() => {
          throw new Error('not LendPositionCreated');
        })
        .mockReturnValueOnce({
          eventName: 'BorrowPositionCreated',
          args: {
            marketId: '0xmarket1',
            borrower: '0xBorrower789',
            principal: 500000n,
            debt: 600000n,
            rate: 5000n,
          },
        });

      const result = await smartContractModule.settleBatch({ matches, config });
      expect(result.borrowPositionEvents).toHaveLength(1);
      expect(result.borrowPositionEvents[0].borrower).toBe('0xborrower789'); // lowercased
    });

    it('should return empty event arrays when no logs match', async () => {
      const config = createTestAppConfig();
      const matches = [createMatch()];
      const txHash = '0xabc123' as `0x${string}`;
      const viem = require('viem');

      mockWalletClient.writeContract.mockResolvedValue(txHash);
      mockPublicClient.waitForTransactionReceipt.mockResolvedValue({
        status: 'success',
        transactionHash: txHash,
        blockNumber: 100n,
        gasUsed: 50000n,
        logs: [],
      });
      mockPublicClient.getBlock.mockResolvedValue({
        timestamp: 1700000000n,
      });

      const result = await smartContractModule.settleBatch({ matches, config });
      expect(result.bondTokenEvents).toHaveLength(0);
      expect(result.lendPositionEvents).toHaveLength(0);
      expect(result.borrowPositionEvents).toHaveLength(0);
    });
  });
});
