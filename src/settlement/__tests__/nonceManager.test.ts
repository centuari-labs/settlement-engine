// NonceManager is not globally mocked, but it imports getPublicClient from
// smartContract which IS globally mocked in setup.ts. Since unit tests
// construct NonceManager directly with a mock publicClient, this is fine.

import { NonceManager, type NoncePublicClient } from '../nonceManager';
import type { Address, Hash } from 'viem';

/**
 * Creates a mock Redis client with the methods used by NonceManager.
 */
const createMockRedis = () => ({
  set: jest.fn(),
  get: jest.fn(),
  del: jest.fn(),
  incr: jest.fn(),
  decr: jest.fn(),
  eval: jest.fn(),
});

type MockRedis = ReturnType<typeof createMockRedis>;

/**
 * Creates a mock PublicClient with methods used by NonceManager.
 */
const createMockPublicClient = () =>
  ({
    getTransactionCount: jest.fn().mockResolvedValue(5),
    waitForTransactionReceipt: jest.fn(),
  }) as unknown as NoncePublicClient & {
    getTransactionCount: jest.Mock;
    waitForTransactionReceipt: jest.Mock;
  };

const TEST_ADDRESS = '0xSettlerAddress1234567890123456789012345678' as Address;
const TEST_CONFIG = {
  lockTtlMs: 1000, // Short for tests
  lockRetryDelayMs: 50,
  txConfirmationTimeoutMs: 5000,
  consumerName: 'test-consumer',
};

describe('NonceManager', () => {
  let mockRedis: MockRedis;
  let mockPublicClient: ReturnType<typeof createMockPublicClient>;
  let nonceManager: NonceManager;

  beforeEach(() => {
    jest.clearAllMocks();
    jest.useFakeTimers();
    mockRedis = createMockRedis();
    mockPublicClient = createMockPublicClient();
    nonceManager = new NonceManager(
      mockRedis as never,
      mockPublicClient,
      TEST_ADDRESS,
      TEST_CONFIG,
    );
  });

  afterEach(async () => {
    jest.useRealTimers();
    await nonceManager.destroy();
  });

  describe('acquireNonce', () => {
    it('should acquire lock and return nonce from Redis', async () => {
      // Lock acquired successfully
      mockRedis.set.mockResolvedValueOnce('OK');
      // No pending tx
      mockRedis.get.mockResolvedValueOnce(null);
      // Nonce from Redis
      mockRedis.get.mockResolvedValueOnce('5');
      // Incr succeeds
      mockRedis.incr.mockResolvedValueOnce(6);

      const nonce = await nonceManager.acquireNonce();

      expect(nonce).toBe(5);
      // Verify lock was acquired with NX PX
      expect(mockRedis.set).toHaveBeenCalledWith(
        expect.stringContaining('settlement:nonce:lock:'),
        expect.stringContaining('test-consumer:'),
        'PX',
        1000,
        'NX',
      );
      // Verify nonce was incremented
      expect(mockRedis.incr).toHaveBeenCalledWith(
        expect.stringContaining('settlement:nonce:'),
      );
    });

    it('should initialize nonce from chain when not in Redis', async () => {
      // Lock acquired
      mockRedis.set.mockResolvedValueOnce('OK');
      // No pending tx
      mockRedis.get.mockResolvedValueOnce(null);
      // No nonce in Redis
      mockRedis.get.mockResolvedValueOnce(null);
      // resetFromChain sets the nonce
      mockRedis.set.mockResolvedValueOnce('OK');
      // After reset, nonce is available
      mockRedis.get.mockResolvedValueOnce('5');
      mockRedis.incr.mockResolvedValueOnce(6);

      const nonce = await nonceManager.acquireNonce();

      expect(nonce).toBe(5);
      expect(mockPublicClient.getTransactionCount).toHaveBeenCalledWith({
        address: TEST_ADDRESS,
        blockTag: 'pending',
      });
    });

    it('should recover pending tx on acquire if confirmed', async () => {
      // Lock acquired
      mockRedis.set.mockResolvedValueOnce('OK');
      // Pending tx exists
      mockRedis.get.mockResolvedValueOnce('0xabc123');
      // Receipt found
      mockPublicClient.waitForTransactionReceipt.mockResolvedValueOnce({
        status: 'success',
        blockNumber: 100n,
      });
      // Del pending tx
      mockRedis.del.mockResolvedValueOnce(1);
      // resetFromChain
      mockPublicClient.getTransactionCount.mockResolvedValueOnce(6);
      mockRedis.set.mockResolvedValueOnce('OK');
      // Read nonce after reset
      mockRedis.get.mockResolvedValueOnce('6');
      mockRedis.incr.mockResolvedValueOnce(7);

      const nonce = await nonceManager.acquireNonce();

      expect(nonce).toBe(6);
      expect(mockPublicClient.waitForTransactionReceipt).toHaveBeenCalledWith({
        hash: '0xabc123',
        timeout: 5000,
      });
    });

    it('should reset from chain if pending tx is not confirmed', async () => {
      // Lock acquired
      mockRedis.set.mockResolvedValueOnce('OK');
      // Pending tx exists
      mockRedis.get.mockResolvedValueOnce('0xabc123');
      // Receipt times out
      mockPublicClient.waitForTransactionReceipt.mockRejectedValueOnce(
        new Error('timeout'),
      );
      // Del pending tx
      mockRedis.del.mockResolvedValueOnce(1);
      // resetFromChain
      mockPublicClient.getTransactionCount.mockResolvedValueOnce(5);
      mockRedis.set.mockResolvedValueOnce('OK');
      // Read nonce after reset
      mockRedis.get.mockResolvedValueOnce('5');
      mockRedis.incr.mockResolvedValueOnce(6);

      const nonce = await nonceManager.acquireNonce();

      expect(nonce).toBe(5);
    });

    it('should retry lock acquisition when held by another instance', async () => {
      // Use real timers for this test since acquireLock uses Date.now() + setTimeout
      jest.useRealTimers();

      // Recreate manager with very short retry delay
      const fastManager = new NonceManager(
        mockRedis as never,
        mockPublicClient,
        TEST_ADDRESS,
        { ...TEST_CONFIG, lockRetryDelayMs: 10, lockTtlMs: 500 },
      );

      // First attempt: lock held
      mockRedis.set.mockResolvedValueOnce(null);
      // Second attempt: lock acquired
      mockRedis.set.mockResolvedValueOnce('OK');
      // No pending tx
      mockRedis.get.mockResolvedValueOnce(null);
      // Nonce from Redis
      mockRedis.get.mockResolvedValueOnce('10');
      mockRedis.incr.mockResolvedValueOnce(11);

      const nonce = await fastManager.acquireNonce();
      expect(nonce).toBe(10);
      expect(mockRedis.set).toHaveBeenCalledTimes(2);

      await fastManager.destroy();
      jest.useFakeTimers();
    });

    it('should throw if lock cannot be acquired within timeout', async () => {
      jest.useRealTimers();

      const fastManager = new NonceManager(
        mockRedis as never,
        mockPublicClient,
        TEST_ADDRESS,
        { ...TEST_CONFIG, lockRetryDelayMs: 10, lockTtlMs: 100 },
      );

      // Lock always held
      mockRedis.set.mockResolvedValue(null);

      await expect(fastManager.acquireNonce()).rejects.toThrow(
        'Failed to acquire nonce lock',
      );

      await fastManager.destroy();
      jest.useFakeTimers();
    });
  });

  describe('confirmNonce', () => {
    it('should store pending tx hash in Redis', async () => {
      const txHash = '0xdeadbeef' as Hash;
      mockRedis.set.mockResolvedValueOnce('OK');

      await nonceManager.confirmNonce(txHash);

      expect(mockRedis.set).toHaveBeenCalledWith(
        expect.stringContaining('settlement:nonce:pending_tx:'),
        txHash,
      );
    });
  });

  describe('onTxConfirmed', () => {
    it('should clear pending tx and release lock', async () => {
      // Setup: acquire lock first
      mockRedis.set.mockResolvedValueOnce('OK');
      mockRedis.get.mockResolvedValueOnce(null);
      mockRedis.get.mockResolvedValueOnce('5');
      mockRedis.incr.mockResolvedValueOnce(6);
      await nonceManager.acquireNonce();

      mockRedis.del.mockResolvedValueOnce(1);
      mockRedis.eval.mockResolvedValueOnce(1);

      await nonceManager.onTxConfirmed();

      // Verify pending tx cleared
      expect(mockRedis.del).toHaveBeenCalledWith(
        expect.stringContaining('settlement:nonce:pending_tx:'),
      );
      // Verify lock released via Lua script
      expect(mockRedis.eval).toHaveBeenCalled();
    });
  });

  describe('handleFailure', () => {
    beforeEach(async () => {
      // Acquire lock for each test
      mockRedis.set.mockResolvedValueOnce('OK');
      mockRedis.get.mockResolvedValueOnce(null);
      mockRedis.get.mockResolvedValueOnce('5');
      mockRedis.incr.mockResolvedValueOnce(6);
      await nonceManager.acquireNonce();
      jest.clearAllMocks();
    });

    it('should reset from chain on nonce too low error', async () => {
      mockPublicClient.getTransactionCount.mockResolvedValueOnce(7);
      mockRedis.set.mockResolvedValueOnce('OK');
      mockRedis.del.mockResolvedValueOnce(1);
      mockRedis.eval.mockResolvedValueOnce(1);

      await nonceManager.handleFailure(new Error('nonce too low'));

      expect(mockPublicClient.getTransactionCount).toHaveBeenCalledWith({
        address: TEST_ADDRESS,
        blockTag: 'pending',
      });
    });

    it('should reset from chain on nonce already used error', async () => {
      mockPublicClient.getTransactionCount.mockResolvedValueOnce(7);
      mockRedis.set.mockResolvedValueOnce('OK');
      mockRedis.del.mockResolvedValueOnce(1);
      mockRedis.eval.mockResolvedValueOnce(1);

      await nonceManager.handleFailure(
        new Error('nonce has already been used'),
      );

      expect(mockPublicClient.getTransactionCount).toHaveBeenCalled();
    });

    it('should decrement nonce on replacement underpriced error', async () => {
      mockRedis.decr.mockResolvedValueOnce(5);
      mockRedis.del.mockResolvedValueOnce(1);
      mockRedis.eval.mockResolvedValueOnce(1);

      await nonceManager.handleFailure(
        new Error('replacement transaction underpriced'),
      );

      expect(mockRedis.decr).toHaveBeenCalledWith(
        expect.stringContaining('settlement:nonce:'),
      );
    });

    it('should not change nonce on already known error', async () => {
      mockRedis.del.mockResolvedValueOnce(1);
      mockRedis.eval.mockResolvedValueOnce(1);

      await nonceManager.handleFailure(new Error('already known'));

      expect(mockRedis.decr).not.toHaveBeenCalled();
      expect(mockPublicClient.getTransactionCount).not.toHaveBeenCalled();
    });

    it('should decrement nonce on network error (pre-submission)', async () => {
      mockRedis.decr.mockResolvedValueOnce(5);
      mockRedis.del.mockResolvedValueOnce(1);
      mockRedis.eval.mockResolvedValueOnce(1);

      await nonceManager.handleFailure(new Error('ECONNREFUSED'));

      expect(mockRedis.decr).toHaveBeenCalled();
    });

    it('should always release lock on failure', async () => {
      mockRedis.del.mockResolvedValueOnce(1);
      mockRedis.decr.mockResolvedValueOnce(5);
      mockRedis.eval.mockResolvedValueOnce(1);

      await nonceManager.handleFailure(new Error('some error'));

      // Verify lock release Lua script was called
      expect(mockRedis.eval).toHaveBeenCalled();
    });
  });

  describe('resetFromChain', () => {
    it('should fetch nonce with pending blockTag and store in Redis', async () => {
      mockPublicClient.getTransactionCount.mockResolvedValueOnce(42);
      mockRedis.set.mockResolvedValueOnce('OK');

      await nonceManager.resetFromChain();

      expect(mockPublicClient.getTransactionCount).toHaveBeenCalledWith({
        address: TEST_ADDRESS,
        blockTag: 'pending',
      });
      expect(mockRedis.set).toHaveBeenCalledWith(
        expect.stringContaining('settlement:nonce:'),
        '42',
      );
    });
  });

  describe('destroy', () => {
    it('should release lock if held', async () => {
      // Acquire lock
      mockRedis.set.mockResolvedValueOnce('OK');
      mockRedis.get.mockResolvedValueOnce(null);
      mockRedis.get.mockResolvedValueOnce('5');
      mockRedis.incr.mockResolvedValueOnce(6);
      await nonceManager.acquireNonce();

      mockRedis.eval.mockResolvedValueOnce(1);

      await nonceManager.destroy();

      expect(mockRedis.eval).toHaveBeenCalled();
    });

    it('should be safe to call multiple times', async () => {
      await nonceManager.destroy();
      await nonceManager.destroy();
      // Should not throw
    });
  });
});
