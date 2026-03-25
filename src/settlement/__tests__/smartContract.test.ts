/**
 * Unit tests for smartContract module.
 *
 * NOTE: smartContract is globally mocked in setup.ts. To test the actual module,
 * we use jest.requireActual() to get the real implementation.
 */
import type { SettlementError, SettleBatchOptions } from '../smartContract';
import { createMatch } from '../../tests/helpers/testFixtures';
import { createTestConfig } from '../../tests/helpers/testConfig';

// Get the ACTUAL (not mocked) module
const actual = jest.requireActual('../smartContract') as typeof import('../smartContract');
const { settleBatch } = actual;

// We need to mock viem modules to avoid real RPC calls
jest.mock('viem', () => {
  const originalViem = jest.requireActual('viem');
  return {
    ...originalViem,
    createWalletClient: jest.fn(),
    createPublicClient: jest.fn(),
  };
});

jest.mock('viem/accounts', () => {
  const originalAccounts = jest.requireActual('viem/accounts');
  return {
    ...originalAccounts,
    privateKeyToAccount: jest.fn().mockReturnValue({
      address: '0x1234567890123456789012345678901234567890',
      signMessage: jest.fn(),
      signTransaction: jest.fn(),
      signTypedData: jest.fn(),
    }),
  };
});

describe('settleBatch', () => {
  const config = createTestConfig();

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('should throw for empty batch', async () => {
    try {
      await settleBatch({ matches: [], config });
      fail('Should have thrown');
    } catch (error) {
      const settlementError = error as SettlementError;
      expect(settlementError.code).toBe('EMPTY_BATCH');
      expect(settlementError.retryable).toBe(false);
      expect(settlementError.failedMatchIds).toEqual([]);
    }
  });

  it('should handle successful settlement', async () => {
    const { createWalletClient, createPublicClient } = require('viem');

    const mockHash = '0xabc123' as `0x${string}`;
    const mockWriteContract = jest.fn().mockResolvedValue(mockHash);
    const mockWaitForTransactionReceipt = jest.fn().mockResolvedValue({
      status: 'success',
      blockNumber: BigInt(12345),
      gasUsed: BigInt(50000),
      transactionHash: mockHash,
    });
    const mockGetBlock = jest.fn().mockResolvedValue({
      timestamp: BigInt(1704067200),
    });

    createWalletClient.mockReturnValue({
      writeContract: mockWriteContract,
    });
    createPublicClient.mockReturnValue({
      waitForTransactionReceipt: mockWaitForTransactionReceipt,
      getBlock: mockGetBlock,
    });

    const match = createMatch();
    const consoleSpy = jest.spyOn(console, 'log').mockImplementation();

    const result = await settleBatch({ matches: [match], config });

    expect(result.transactionHash).toBe(mockHash);
    expect(result.blockNumber).toBe(12345);
    expect(result.gasUsed).toBe(50000);
    expect(result.timestamp).toBe(1704067200 * 1000);
    expect(result.settledMatchIds).toEqual([match.matchId]);

    consoleSpy.mockRestore();
  });

  it('should handle reverted transaction', async () => {
    const { createWalletClient, createPublicClient } = require('viem');

    const mockHash = '0xabc123' as `0x${string}`;
    createWalletClient.mockReturnValue({
      writeContract: jest.fn().mockResolvedValue(mockHash),
    });
    createPublicClient.mockReturnValue({
      waitForTransactionReceipt: jest.fn().mockResolvedValue({
        status: 'reverted',
        blockNumber: BigInt(12345),
        gasUsed: BigInt(50000),
        transactionHash: mockHash,
      }),
    });

    const match = createMatch();
    const consoleSpy = jest.spyOn(console, 'log').mockImplementation();

    try {
      await settleBatch({ matches: [match], config });
      fail('Should have thrown');
    } catch (error) {
      const settlementError = error as SettlementError;
      // The reverted error gets caught and mapped by mapContractError
      expect(settlementError.failedMatchIds).toEqual([match.matchId]);
    }

    consoleSpy.mockRestore();
  });

  it('should map AlreadySettled error correctly', async () => {
    const { createWalletClient, createPublicClient } = require('viem');

    createWalletClient.mockReturnValue({
      writeContract: jest.fn().mockRejectedValue(new Error('AlreadySettled')),
    });
    createPublicClient.mockReturnValue({});

    const match = createMatch();
    const consoleSpy = jest.spyOn(console, 'log').mockImplementation();

    try {
      await settleBatch({ matches: [match], config });
      fail('Should have thrown');
    } catch (error) {
      const settlementError = error as SettlementError;
      expect(settlementError.code).toBe('ALREADY_SETTLED');
      expect(settlementError.retryable).toBe(false);
    }

    consoleSpy.mockRestore();
  });

  it('should map ContractPaused error as retryable', async () => {
    const { createWalletClient, createPublicClient } = require('viem');

    createWalletClient.mockReturnValue({
      writeContract: jest.fn().mockRejectedValue(new Error('ContractPaused')),
    });
    createPublicClient.mockReturnValue({});

    const match = createMatch();
    const consoleSpy = jest.spyOn(console, 'log').mockImplementation();

    try {
      await settleBatch({ matches: [match], config });
      fail('Should have thrown');
    } catch (error) {
      const settlementError = error as SettlementError;
      expect(settlementError.code).toBe('CONTRACT_PAUSED');
      expect(settlementError.retryable).toBe(true);
    }

    consoleSpy.mockRestore();
  });

  it('should map EmptyBatch error correctly', async () => {
    const { createWalletClient, createPublicClient } = require('viem');

    createWalletClient.mockReturnValue({
      writeContract: jest.fn().mockRejectedValue(new Error('EmptyBatch')),
    });
    createPublicClient.mockReturnValue({});

    const match = createMatch();
    const consoleSpy = jest.spyOn(console, 'log').mockImplementation();

    try {
      await settleBatch({ matches: [match], config });
      fail('Should have thrown');
    } catch (error) {
      const settlementError = error as SettlementError;
      expect(settlementError.code).toBe('EMPTY_BATCH');
      expect(settlementError.retryable).toBe(false);
    }

    consoleSpy.mockRestore();
  });

  it('should map InvalidMatchData error correctly', async () => {
    const { createWalletClient, createPublicClient } = require('viem');

    createWalletClient.mockReturnValue({
      writeContract: jest.fn().mockRejectedValue(new Error('InvalidMatchData')),
    });
    createPublicClient.mockReturnValue({});

    const match = createMatch();
    const consoleSpy = jest.spyOn(console, 'log').mockImplementation();

    try {
      await settleBatch({ matches: [match], config });
      fail('Should have thrown');
    } catch (error) {
      const settlementError = error as SettlementError;
      expect(settlementError.code).toBe('INVALID_MATCH_DATA');
      expect(settlementError.retryable).toBe(false);
    }

    consoleSpy.mockRestore();
  });

  it('should map network errors as retryable', async () => {
    const { createWalletClient, createPublicClient } = require('viem');

    createWalletClient.mockReturnValue({
      writeContract: jest.fn().mockRejectedValue(new Error('network timeout')),
    });
    createPublicClient.mockReturnValue({});

    const match = createMatch();
    const consoleSpy = jest.spyOn(console, 'log').mockImplementation();

    try {
      await settleBatch({ matches: [match], config });
      fail('Should have thrown');
    } catch (error) {
      const settlementError = error as SettlementError;
      expect(settlementError.code).toBe('NETWORK_ERROR');
      expect(settlementError.retryable).toBe(true);
    }

    consoleSpy.mockRestore();
  });

  it('should map ECONNREFUSED as retryable network error', async () => {
    const { createWalletClient, createPublicClient } = require('viem');

    createWalletClient.mockReturnValue({
      writeContract: jest.fn().mockRejectedValue(new Error('ECONNREFUSED')),
    });
    createPublicClient.mockReturnValue({});

    const match = createMatch();
    const consoleSpy = jest.spyOn(console, 'log').mockImplementation();

    try {
      await settleBatch({ matches: [match], config });
      fail('Should have thrown');
    } catch (error) {
      const settlementError = error as SettlementError;
      expect(settlementError.code).toBe('NETWORK_ERROR');
      expect(settlementError.retryable).toBe(true);
    }

    consoleSpy.mockRestore();
  });

  it('should map unknown errors as retryable with UNKNOWN_ERROR code', async () => {
    const { createWalletClient, createPublicClient } = require('viem');

    createWalletClient.mockReturnValue({
      writeContract: jest.fn().mockRejectedValue(new Error('Something unexpected happened')),
    });
    createPublicClient.mockReturnValue({});

    const match = createMatch();
    const consoleSpy = jest.spyOn(console, 'log').mockImplementation();

    try {
      await settleBatch({ matches: [match], config });
      fail('Should have thrown');
    } catch (error) {
      const settlementError = error as SettlementError;
      expect(settlementError.code).toBe('UNKNOWN_ERROR');
      expect(settlementError.retryable).toBe(true);
    }

    consoleSpy.mockRestore();
  });

  it('should handle non-Error thrown values', async () => {
    const { createWalletClient, createPublicClient } = require('viem');

    createWalletClient.mockReturnValue({
      writeContract: jest.fn().mockRejectedValue('string error'),
    });
    createPublicClient.mockReturnValue({});

    const match = createMatch();
    const consoleSpy = jest.spyOn(console, 'log').mockImplementation();

    try {
      await settleBatch({ matches: [match], config });
      fail('Should have thrown');
    } catch (error) {
      const settlementError = error as SettlementError;
      expect(settlementError.message).toContain('string error');
      expect(settlementError.code).toBe('UNKNOWN_ERROR');
    }

    consoleSpy.mockRestore();
  });

  it('should normalize private key without 0x prefix', async () => {
    const { createWalletClient, createPublicClient } = require('viem');
    const { privateKeyToAccount } = require('viem/accounts');

    const mockHash = '0xabc123' as `0x${string}`;
    createWalletClient.mockReturnValue({
      writeContract: jest.fn().mockResolvedValue(mockHash),
    });
    createPublicClient.mockReturnValue({
      waitForTransactionReceipt: jest.fn().mockResolvedValue({
        status: 'success',
        blockNumber: BigInt(1),
        gasUsed: BigInt(1),
        transactionHash: mockHash,
      }),
      getBlock: jest.fn().mockResolvedValue({ timestamp: BigInt(1) }),
    });

    const configWithoutPrefix = createTestConfig({
      settlementPrivateKey: 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
    });

    const match = createMatch();
    const consoleSpy = jest.spyOn(console, 'log').mockImplementation();

    await settleBatch({ matches: [match], config: configWithoutPrefix });

    expect(privateKeyToAccount).toHaveBeenCalledWith(
      '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
    );

    consoleSpy.mockRestore();
  });

  it('should pass multiple matches to contract in correct format', async () => {
    const { createWalletClient, createPublicClient } = require('viem');

    const mockHash = '0xabc123' as `0x${string}`;
    const mockWriteContract = jest.fn().mockResolvedValue(mockHash);

    createWalletClient.mockReturnValue({
      writeContract: mockWriteContract,
    });
    createPublicClient.mockReturnValue({
      waitForTransactionReceipt: jest.fn().mockResolvedValue({
        status: 'success',
        blockNumber: BigInt(1),
        gasUsed: BigInt(1),
        transactionHash: mockHash,
      }),
      getBlock: jest.fn().mockResolvedValue({ timestamp: BigInt(1) }),
    });

    const match1 = createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440001' });
    const match2 = createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440002' });
    const consoleSpy = jest.spyOn(console, 'log').mockImplementation();

    const result = await settleBatch({ matches: [match1, match2], config });

    expect(result.settledMatchIds).toEqual([match1.matchId, match2.matchId]);

    // Verify writeContract was called with transformed match array
    expect(mockWriteContract).toHaveBeenCalledWith(
      expect.objectContaining({
        functionName: 'settleMatches',
        args: [expect.arrayContaining([
          expect.objectContaining({
            matchedAmount: BigInt(match1.matchedAmount),
            rate: BigInt(match1.rate),
            borrowerIsTaker: match1.borrowerIsTaker,
          }),
        ])],
      }),
    );

    consoleSpy.mockRestore();
  });
});
