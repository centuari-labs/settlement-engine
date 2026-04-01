import type Redis from 'ioredis';
import type { Address, Hash } from 'viem';
import { privateKeyToAccount } from 'viem/accounts';
import type { AppConfig } from '../config';
import { getPublicClient } from './smartContract';
import { logger } from '../logger';

/**
 * Minimal interface for the public client methods used by NonceManager.
 * Avoids coupling to viem's narrowed generics from createPublicClient().
 */
export interface NoncePublicClient {
  getTransactionCount(args: {
    address: Address;
    blockTag: 'pending';
  }): Promise<number>;
  waitForTransactionReceipt(args: {
    hash: Hash;
    timeout?: number;
  }): Promise<{ status: string; blockNumber: bigint }>;
}

/**
 * Lua script for atomic compare-and-delete.
 * Only deletes the key if the current value matches the expected value.
 * Returns 1 if deleted, 0 if value mismatch or key missing.
 */
const RELEASE_LOCK_SCRIPT = `
if redis.call("get", KEYS[1]) == ARGV[1] then
  return redis.call("del", KEYS[1])
else
  return 0
end
`;

/**
 * Lua script for atomic compare-and-extend TTL.
 * Only extends the TTL if the current value matches the expected value.
 * Returns 1 if extended, 0 if value mismatch or key missing.
 */
const EXTEND_LOCK_SCRIPT = `
if redis.call("get", KEYS[1]) == ARGV[1] then
  return redis.call("pexpire", KEYS[1], ARGV[2])
else
  return 0
end
`;

/**
 * Redis key builders scoped by wallet address.
 */
const redisKeys = (address: string) => ({
  nonce: `settlement:nonce:${address}`,
  lock: `settlement:nonce:lock:${address}`,
  pendingTx: `settlement:nonce:pending_tx:${address}`,
});

/**
 * Redis-based distributed nonce manager for settlement transactions.
 *
 * Uses a SETNX lock pattern to ensure only one transaction is in-flight
 * per wallet at any time, matching the blockchain's own constraint.
 * A lock heartbeat periodically extends the TTL to cover long receipt waits.
 */
export class NonceManager {
  private readonly redis: Redis;
  private readonly publicClient: NoncePublicClient;
  private readonly walletAddress: Address;
  private readonly lockTtlMs: number;
  private readonly lockRetryDelayMs: number;
  private readonly txConfirmationTimeoutMs: number;
  private readonly consumerName: string;
  private readonly keys: ReturnType<typeof redisKeys>;

  private lockId: string | null = null;
  private heartbeatInterval: NodeJS.Timeout | null = null;

  constructor(
    redis: Redis,
    publicClient: NoncePublicClient,
    walletAddress: Address,
    config: {
      lockTtlMs: number;
      lockRetryDelayMs: number;
      txConfirmationTimeoutMs: number;
      consumerName: string;
    },
  ) {
    this.redis = redis;
    this.publicClient = publicClient;
    this.walletAddress = walletAddress;
    this.lockTtlMs = config.lockTtlMs;
    this.lockRetryDelayMs = config.lockRetryDelayMs;
    this.txConfirmationTimeoutMs = config.txConfirmationTimeoutMs;
    this.consumerName = config.consumerName;
    this.keys = redisKeys(walletAddress.toLowerCase());
  }

  /**
   * Acquire a nonce for the next transaction.
   *
   * 1. Acquires the Redis lock (retries until acquired or timeout)
   * 2. Checks for a pending tx from a previous crash — if found, waits briefly for receipt
   * 3. Reads or initializes the nonce from chain
   * 4. Increments the nonce in Redis atomically
   * 5. Starts a lock heartbeat to keep the lock alive during tx confirmation
   *
   * @returns The nonce to use for the next transaction.
   * @throws Error if the lock cannot be acquired within the timeout.
   */
  async acquireNonce(): Promise<number> {
    await this.acquireLock();

    // Check for a pending tx from a previous crash
    const pendingTxHash = await this.redis.get(this.keys.pendingTx);
    if (pendingTxHash) {
      logger.info({ component: 'nonce-manager', txHash: pendingTxHash }, 'Found pending tx from previous run');
      try {
        const receipt = await this.publicClient.waitForTransactionReceipt({
          hash: pendingTxHash as Hash,
          timeout: 5_000, // Short timeout — just checking if it landed
        });
        // Tx landed — clear pending state and reset nonce from chain
        logger.info(
          { component: 'nonce-manager', txHash: pendingTxHash, status: receipt.status, blockNumber: Number(receipt.blockNumber) },
          'Pending tx confirmed',
        );
        await this.redis.del(this.keys.pendingTx);
        await this.resetFromChain();
      } catch {
        // Tx not confirmed — it may be stuck or dropped.
        // Reset nonce from chain to get the correct next nonce.
        logger.warn({ component: 'nonce-manager', txHash: pendingTxHash }, 'Pending tx not confirmed, resetting nonce from chain');
        await this.redis.del(this.keys.pendingTx);
        await this.resetFromChain();
      }
    }

    // Read current nonce from Redis, or initialize from chain
    let nonceStr = await this.redis.get(this.keys.nonce);
    if (nonceStr === null) {
      await this.resetFromChain();
      nonceStr = await this.redis.get(this.keys.nonce);
    }

    const nonce = Number(nonceStr);

    // Increment for next caller
    await this.redis.incr(this.keys.nonce);

    // Start heartbeat to keep the lock alive during tx confirmation
    this.startLockHeartbeat();

    logger.info({ component: 'nonce-manager', nonce, walletAddress: this.walletAddress }, 'Acquired nonce');

    return nonce;
  }

  /**
   * Record that a transaction was submitted (but not yet confirmed).
   * Stores the tx hash in Redis so it can be recovered after a crash.
   *
   * @param txHash - The transaction hash that was submitted.
   */
  async confirmNonce(txHash: Hash): Promise<void> {
    await this.redis.set(this.keys.pendingTx, txHash);
    logger.info({ component: 'nonce-manager', txHash }, 'Recorded pending tx');
  }

  /**
   * Called after the transaction has been confirmed on-chain.
   * Clears the pending tx and releases the lock.
   */
  async onTxConfirmed(): Promise<void> {
    this.stopLockHeartbeat();
    await this.redis.del(this.keys.pendingTx);
    await this.releaseLock();
  }

  /**
   * Handle a transaction failure. Determines the appropriate nonce action
   * based on the error type, then releases the lock.
   *
   * @param error - The error from the failed transaction.
   */
  async handleFailure(error: unknown): Promise<void> {
    this.stopLockHeartbeat();

    const errorMessage =
      error instanceof Error ? error.message : String(error);

    if (
      errorMessage.includes('nonce too low') ||
      errorMessage.includes('nonce has already been used')
    ) {
      // Nonce was consumed by another tx — reset from chain
      logger.warn({ component: 'nonce-manager', errorMessage }, 'Nonce too low, resetting from chain');
      await this.resetFromChain();
    } else if (errorMessage.includes('already known')) {
      // Tx was already submitted to mempool — it may still confirm
      logger.warn({ component: 'nonce-manager', errorMessage }, 'Tx already known in mempool');
      // Don't change nonce — the tx is in the mempool and may confirm
    } else if (errorMessage.includes('replacement transaction underpriced')) {
      // Same nonce tx exists with higher gas — decrement so next retry uses same nonce
      logger.warn({ component: 'nonce-manager', errorMessage }, 'Replacement underpriced');
      await this.redis.decr(this.keys.nonce);
    } else {
      // Network error or other pre-submission failure — tx was never sent, decrement nonce
      logger.warn({ component: 'nonce-manager', errorMessage }, 'Pre-submission failure, decrementing nonce');
      await this.redis.decr(this.keys.nonce);
    }

    await this.redis.del(this.keys.pendingTx);
    await this.releaseLock();
  }

  /**
   * Fetch the current nonce from the chain and update Redis.
   * Uses `blockTag: 'pending'` to account for mempool transactions.
   */
  async resetFromChain(): Promise<void> {
    const nonce = await this.publicClient.getTransactionCount({
      address: this.walletAddress,
      blockTag: 'pending',
    });
    await this.redis.set(this.keys.nonce, nonce.toString());
    logger.info({ component: 'nonce-manager', nonce, walletAddress: this.walletAddress }, 'Reset nonce from chain');
  }

  /**
   * Release the lock and stop the heartbeat. Call on shutdown.
   */
  async destroy(): Promise<void> {
    this.stopLockHeartbeat();
    if (this.lockId) {
      await this.releaseLock();
    }
  }

  /**
   * Acquire the Redis lock with retry.
   * Uses SETNX + PX for atomic set-if-not-exists with millisecond expiry.
   *
   * @throws Error if lock cannot be acquired within lockTtlMs.
   */
  private async acquireLock(): Promise<void> {
    const lockId = `${this.consumerName}:${Date.now()}`;
    const deadline = Date.now() + this.lockTtlMs;

    while (Date.now() < deadline) {
      const result = await this.redis.set(
        this.keys.lock,
        lockId,
        'PX',
        this.lockTtlMs,
        'NX',
      );

      if (result === 'OK') {
        this.lockId = lockId;
        return;
      }

      // Lock held by another instance — wait and retry
      await new Promise((resolve) =>
        setTimeout(resolve, this.lockRetryDelayMs),
      );
    }

    throw new Error(
      `[nonce-manager] Failed to acquire nonce lock within ${this.lockTtlMs}ms`,
    );
  }

  /**
   * Release the Redis lock using atomic compare-and-delete.
   * Only the lock holder can release (prevents releasing another instance's lock).
   */
  private async releaseLock(): Promise<void> {
    if (!this.lockId) return;

    await this.redis.eval(RELEASE_LOCK_SCRIPT, 1, this.keys.lock, this.lockId);
    this.lockId = null;
  }

  /**
   * Start a heartbeat that periodically extends the lock TTL.
   * Runs every lockTtlMs / 3 to keep the lock alive during tx confirmation.
   */
  private startLockHeartbeat(): void {
    this.stopLockHeartbeat();

    const intervalMs = Math.floor(this.lockTtlMs / 3);
    this.heartbeatInterval = setInterval(() => {
      if (!this.lockId) {
        this.stopLockHeartbeat();
        return;
      }

      void this.redis
        .eval(
          EXTEND_LOCK_SCRIPT,
          1,
          this.keys.lock,
          this.lockId,
          this.lockTtlMs.toString(),
        )
        .catch((err) => {
          logger.error({ component: 'nonce-manager', err }, 'Failed to extend lock TTL');
        });
    }, intervalMs);
  }

  /**
   * Stop the lock heartbeat timer.
   */
  private stopLockHeartbeat(): void {
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
      this.heartbeatInterval = null;
    }
  }
}

/**
 * Create a NonceManager instance from application config.
 *
 * @param redis - Redis client instance.
 * @param config - Application configuration.
 * @returns A new NonceManager instance.
 */
export const createNonceManager = (
  redis: Redis,
  config: AppConfig,
): NonceManager => {
  const publicClient = getPublicClient(config) as unknown as NoncePublicClient;
  const privateKey = config.settlementPrivateKey.startsWith('0x')
    ? (config.settlementPrivateKey as `0x${string}`)
    : (`0x${config.settlementPrivateKey}` as `0x${string}`);
  const account = privateKeyToAccount(privateKey);

  return new NonceManager(redis, publicClient, account.address, {
    lockTtlMs: config.nonceLockTtlMs,
    lockRetryDelayMs: config.nonceLockRetryDelayMs,
    txConfirmationTimeoutMs: config.txConfirmationTimeoutMs,
    consumerName: config.consumerName,
  });
};
