import type Redis from 'ioredis';
import type { AppConfig } from '../config';
import {
  readMatches,
  processPendingEntriesOnStartup,
  type ReadMatchesOptions,
} from '../redis/settlementMatchConsumer';
import { BatchAccumulator } from './batchAccumulator';
import {
  processSettlementBatch,
  type SettlementBatchContext,
  BatchProcessingError,
} from './processBatch';
import {
  unlockFailedMatches,
  recordFailedMatches,
  restoreOrdersForFailedMatches,
} from './database';
import type { NonceManager } from './nonceManager';
import { calculateBackoffDelay } from './helpers';
import { logger } from '../logger';

/**
 * Options for creating a batch processor.
 */
export interface BatchProcessorOptions {
  /**
   * Redis client.
   */
  readonly redis: Redis;
  /**
   * Application configuration.
   */
  readonly config: AppConfig;
  /**
   * Batch accumulator instance.
   */
  readonly accumulator: BatchAccumulator;
  /**
   * Optional handler for invalid entries.
   */
  readonly onInvalid?: ReadMatchesOptions['onInvalid'];
  /**
   * Nonce manager for explicit nonce sequencing.
   */
  readonly nonceManager?: NonceManager;
}

/**
 * Batch processor that polls periodically, reads matches when needed, and processes batches.
 */
export class BatchProcessor {
  private readonly redis: Redis;
  private readonly config: AppConfig;
  private readonly accumulator: BatchAccumulator;
  private readonly onInvalid?: ReadMatchesOptions['onInvalid'];
  private readonly readMatchesOptions: ReadMatchesOptions;
  private readonly batchContext: SettlementBatchContext;
  private isRunning = false;
  private pollIntervalId: NodeJS.Timeout | null = null;
  private pendingReclaimIntervalId: NodeJS.Timeout | null = null;
  private processingPromise: Promise<void> | null = null;
  private consecutiveFailures = 0;
  private nextRetryAt = 0;

  /**
   * Create a new batch processor.
   *
   * @param options - Options for creating the batch processor.
   */
  constructor(options: BatchProcessorOptions) {
    this.redis = options.redis;
    this.config = options.config;
    this.accumulator = options.accumulator;
    this.onInvalid = options.onInvalid;

    this.readMatchesOptions = {
      redis: this.redis,
      stream: this.config.settlementMatchesStream,
      consumerGroup: this.config.consumerGroup,
      consumerName: this.config.consumerName,
      readCount: this.config.readCount,
      onInvalid: this.onInvalid,
    };

    this.batchContext = {
      redis: this.redis,
      stream: this.config.settlementMatchesStream,
      consumerGroup: this.config.consumerGroup,
      streamMaxLen: this.config.streamMaxLen,
      nonceManager: options.nonceManager,
    };
  }

  /**
   * Start the batch processor loop.
   * The processor will poll periodically, read matches when needed, and process batches.
   */
  start(): void {
    if (this.isRunning) {
      logger.warn({ component: 'batch-processor' }, 'Already running');
      return;
    }

    this.isRunning = true;

    logger.info(
      {
        component: 'batch-processor',
        pollIntervalMs: this.config.pollIntervalMs,
        batchSize: this.config.batchSize,
        batchIntervalMs: this.config.batchIntervalMs,
        pendingReclaimIntervalMs: this.config.pendingReclaimIntervalMs,
      },
      'Starting batch processor',
    );

    // Start polling loop (reads new entries only)
    this.pollIntervalId = setInterval(() => {
      void this.poll();
    }, this.config.pollIntervalMs);

    // Start pending reclaim timer (processes failed/abandoned entries on a longer interval)
    this.pendingReclaimIntervalId = setInterval(() => {
      void this.reclaimPending();
    }, this.config.pendingReclaimIntervalMs);

    // Do an initial poll immediately
    void this.poll();
  }

  /**
   * Stop the batch processor.
   * The processor will finish processing any in-flight batches before stopping.
   *
   * @returns Promise that resolves when the processor has stopped.
   */
  async stop(): Promise<void> {
    if (!this.isRunning) {
      return;
    }

    logger.info({ component: 'batch-processor' }, 'Stopping batch processor...');

    this.isRunning = false;

    // Clear polling interval
    if (this.pollIntervalId) {
      clearInterval(this.pollIntervalId);
      this.pollIntervalId = null;
    }

    // Clear pending reclaim interval
    if (this.pendingReclaimIntervalId) {
      clearInterval(this.pendingReclaimIntervalId);
      this.pendingReclaimIntervalId = null;
    }

    // Wait for any in-flight processing to complete
    if (this.processingPromise) {
      await this.processingPromise;
    }

    // Process any remaining matches in the accumulator
    const pendingCount = this.accumulator.getPendingCount();
    if (pendingCount > 0) {
      logger.info(
        { component: 'batch-processor', pendingCount },
        'Processing pending matches before shutdown',
      );
      await this.processBatch();
    }

    logger.info({ component: 'batch-processor' }, 'Batch processor stopped');
  }

  /**
   * Poll the accumulator and process batches as needed.
   * This is called periodically by the polling interval.
   */
  private async poll(): Promise<void> {
    if (!this.isRunning) {
      return;
    }

    // If we're in backoff after a failure, skip until nextRetryAt
    if (Date.now() < this.nextRetryAt) {
      return;
    }

    // If there's already a processing operation in flight, skip this poll
    if (this.processingPromise) {
      return;
    }

    try {
      // Check if accumulator needs more matches
      if (this.accumulator.needsMoreMatches()) {
        // Check Redis connection status before reading
        if (
          this.redis.status !== 'ready' &&
          this.redis.status !== 'connecting'
        ) {
          throw new Error(
            `Redis connection is not ready. Status: ${this.redis.status}`,
          );
        }

        // Read new matches from stream only (pending reclaim runs on separate timer)
        const matches = await readMatches(this.readMatchesOptions);
        if (matches.length > 0) {
          this.accumulator.addMatches(matches);
          logger.info(
            {
              component: 'batch-processor',
              newMatches: matches.length,
              pending: this.accumulator.getPendingCount(),
            },
            'Read new matches',
          );
        }
      }

      // Check if batch should be processed
      if (this.accumulator.shouldProcess()) {
        this.processingPromise = this.processBatch();
        await this.processingPromise;
        this.processingPromise = null;
      }
    } catch (error) {
      logger.error({ component: 'batch-processor', err: error }, 'Error in poll');
      // Ensure processing promise is cleared even if processBatch throws
      if (this.processingPromise) {
        this.processingPromise = null;
      }
    }
  }

  /**
   * Process the current batch from the accumulator.
   */
  private async processBatch(): Promise<void> {
    const batch = this.accumulator.getBatch();

    if (batch.length === 0) {
      return;
    }

    const startTime = Date.now();

    try {
      logger.info(
        { component: 'batch-processor', matchCount: batch.length },
        'Processing batch',
      );

      await processSettlementBatch(batch, this.batchContext, this.config);

      const duration = Date.now() - startTime;

      logger.info(
        { component: 'batch-processor', matchCount: batch.length, duration },
        'Batch processing successful',
      );
      this.consecutiveFailures = 0;
    } catch (error) {
      const duration = Date.now() - startTime;

      if (error instanceof BatchProcessingError) {
        logger.error(
          {
            component: 'batch-processor',
            matchCount: batch.length,
            duration,
            retryable: error.retryable,
            err: error,
          },
          'Batch processing failed',
        );

        // Matches remain in Redis pending state (not ACKed) for retry
        // Retryable errors will be retried through pending entry processing
        // Non-retryable errors remain in pending state for manual intervention
        if (error.retryable) {
          logger.info(
            {
              component: 'batch-processor',
              matchCount: batch.length,
              matchIds: batch.map((m) => m.id),
            },
            'Retryable error, matches remain in Redis pending state for retry',
          );
        } else {
          logger.error(
            {
              component: 'batch-processor',
              matchIds: batch.map((m) => m.id),
              errorCode: (error.originalError as any)?.code,
            },
            'Non-retryable error, running full failure cleanup',
          );

          const payloads = batch.map((m) => m.payload);
          const failureReason = (error.originalError as any)?.code ?? error.message;

          // 1. Release the user_balance.in_orders lock
          try {
            await unlockFailedMatches(payloads);
          } catch (e) {
            logger.error({ component: 'batch-processor', err: e }, 'Failed to release in_orders lock');
          }

          // 2. Mark matches as FAILED in database
          try {
            await recordFailedMatches(payloads, failureReason);
          } catch (e) {
            logger.error({ component: 'batch-processor', err: e }, 'Failed to record match failures');
          }

          // 3. Restore order quantities (reduce filled_quantity, cancel/partially_fill)
          try {
            await restoreOrdersForFailedMatches(payloads);
          } catch (e) {
            logger.error({ component: 'batch-processor', err: e }, 'Failed to restore order quantities');
          }

          // 4. ACK + delete Redis entries to prevent infinite retry loop
          try {
            for (const match of batch) {
              await this.batchContext.redis.xack(
                match.stream, this.batchContext.consumerGroup, match.id,
              );
              await this.batchContext.redis.xdel(match.stream, match.id);
            }
            logger.info(
              { component: 'batch-processor', count: batch.length },
              'ACKed and deleted failed entries from Redis',
            );
          } catch (e) {
            logger.error({ component: 'batch-processor', err: e }, 'Failed to ACK/delete Redis entries');
          }

          // Entries are cleaned up — no backoff needed, don't re-throw
          this.consecutiveFailures = 0;
          return;
        }
      } else {
        // Unexpected error - matches remain in Redis pending state for retry
        logger.error(
          {
            component: 'batch-processor',
            matchCount: batch.length,
            duration,
            err: error,
            matchIds: batch.map((m) => m.id),
          },
          'Unexpected error during batch processing, matches remain in Redis pending state',
        );
      }

      this.consecutiveFailures += 1;
      const delay = calculateBackoffDelay(
        this.consecutiveFailures,
        this.config.failureBackoffBaseMs,
        this.config.failureBackoffMaxMs,
      );
      this.nextRetryAt = Date.now() + delay;
      logger.info(
        { component: 'batch-processor', delayMs: delay, failures: this.consecutiveFailures },
        'Backing off after failure',
      );

      throw error;
    }
  }

  /**
   * Reclaim pending entries (failed/abandoned matches) and add to accumulator.
   * Runs on a separate timer at pendingReclaimIntervalMs to avoid loading
   * unbounded entries on every poll cycle.
   */
  private async reclaimPending(): Promise<void> {
    if (!this.isRunning) {
      return;
    }

    try {
      if (
        this.redis.status !== 'ready' &&
        this.redis.status !== 'connecting'
      ) {
        return;
      }

      const reclaimOptions = {
        ...this.readMatchesOptions,
        maxEntries: this.config.batchSize * 3,
        xclaimMinIdleMs: this.config.xclaimMinIdleMs,
      };
      const pendingMatches = await processPendingEntriesOnStartup(
        reclaimOptions,
      );
      if (pendingMatches.length > 0) {
        this.accumulator.addMatches(pendingMatches);
        logger.info(
          {
            component: 'batch-processor',
            reclaimed: pendingMatches.length,
            pending: this.accumulator.getPendingCount(),
          },
          'Reclaimed pending matches',
        );
      }
    } catch (error) {
      logger.error({ component: 'batch-processor', err: error }, 'Error reclaiming pending entries');
    }
  }

}
