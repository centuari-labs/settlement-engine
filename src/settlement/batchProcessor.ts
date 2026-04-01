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
  findUnprocessedSettlementBatches,
  retryEventProcessing,
  unlockFailedMatches,
  recordFailedMatches,
  restoreOrdersForFailedMatches,
} from './database';
import type { NonceManager } from './nonceManager';

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
  private eventRecoveryIntervalId: NodeJS.Timeout | null = null;
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
      // eslint-disable-next-line no-console
      console.warn('[batch-processor] Already running');
      return;
    }

    this.isRunning = true;

    // eslint-disable-next-line no-console
    console.log(
      '[batch-processor] Starting batch processor',
      {
        pollIntervalMs: this.config.pollIntervalMs,
        batchSize: this.config.batchSize,
        batchIntervalMs: this.config.batchIntervalMs,
        pendingReclaimIntervalMs: this.config.pendingReclaimIntervalMs,
      },
    );

    // Start polling loop (reads new entries only)
    this.pollIntervalId = setInterval(() => {
      void this.poll();
    }, this.config.pollIntervalMs);

    // Start pending reclaim timer (processes failed/abandoned entries on a longer interval)
    this.pendingReclaimIntervalId = setInterval(() => {
      void this.reclaimPending();
    }, this.config.pendingReclaimIntervalMs);

    // Start event recovery timer (retries failed event processing every 60s)
    const EVENT_RECOVERY_INTERVAL_MS = 60_000;
    this.eventRecoveryIntervalId = setInterval(() => {
      void this.recoverUnprocessedEvents();
    }, EVENT_RECOVERY_INTERVAL_MS);

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

    // eslint-disable-next-line no-console
    console.log('[batch-processor] Stopping batch processor...');

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

    // Clear event recovery interval
    if (this.eventRecoveryIntervalId) {
      clearInterval(this.eventRecoveryIntervalId);
      this.eventRecoveryIntervalId = null;
    }

    // Wait for any in-flight processing to complete
    if (this.processingPromise) {
      await this.processingPromise;
    }

    // Process any remaining matches in the accumulator
    const pendingCount = this.accumulator.getPendingCount();
    if (pendingCount > 0) {
      // eslint-disable-next-line no-console
      console.log(
        `[batch-processor] Processing ${pendingCount} pending matches before shutdown`,
      );
      await this.processBatch();
    }

    // eslint-disable-next-line no-console
    console.log('[batch-processor] Batch processor stopped');
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
          // eslint-disable-next-line no-console
          console.log(
            `[batch-processor] Read ${matches.length} new matches, accumulator now has ${this.accumulator.getPendingCount()} pending`,
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
      // eslint-disable-next-line no-console
      console.error('[batch-processor] Error in poll', error);
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
      // eslint-disable-next-line no-console
      console.log(
        `[batch-processor] Processing batch of ${batch.length} matches`,
      );

      await processSettlementBatch(batch, this.batchContext, this.config);

      const duration = Date.now() - startTime;

      // eslint-disable-next-line no-console
      console.log(
        `[batch-processor] Batch processing successful`,
        {
          matchCount: batch.length,
          duration,
        },
      );
      this.consecutiveFailures = 0;
    } catch (error) {
      const duration = Date.now() - startTime;

      if (error instanceof BatchProcessingError) {
        // eslint-disable-next-line no-console
        console.error(
          `[batch-processor] Batch processing failed`,
          {
            matchCount: batch.length,
            duration,
            retryable: error.retryable,
            error: error.message,
          },
        );

        // Matches remain in Redis pending state (not ACKed) for retry
        // Retryable errors will be retried through pending entry processing
        // Non-retryable errors remain in pending state for manual intervention
        if (error.retryable) {
          // eslint-disable-next-line no-console
          console.log(
            `[batch-processor] Retryable error, matches remain in Redis pending state for retry`,
            {
              matchCount: batch.length,
              matchIds: batch.map((m) => m.id),
            },
          );
        } else {
          // eslint-disable-next-line no-console
          console.error(
            `[batch-processor] Non-retryable error, running full failure cleanup`,
            {
              matchIds: batch.map((m) => m.id),
              errorCode: (error.originalError as any)?.code,
            },
          );

          const payloads = batch.map((m) => m.payload);
          const failureReason = (error.originalError as any)?.code ?? error.message;

          // 1. Unlock portfolio locked_amount
          try {
            await unlockFailedMatches(payloads);
          } catch (e) {
            // eslint-disable-next-line no-console
            console.error(`[batch-processor] Failed to unlock portfolio amounts`, e);
          }

          // 2. Mark matches as FAILED in database
          try {
            await recordFailedMatches(payloads, failureReason);
          } catch (e) {
            // eslint-disable-next-line no-console
            console.error(`[batch-processor] Failed to record match failures`, e);
          }

          // 3. Restore order quantities (reduce filled_quantity, cancel/partially_fill)
          try {
            await restoreOrdersForFailedMatches(payloads);
          } catch (e) {
            // eslint-disable-next-line no-console
            console.error(`[batch-processor] Failed to restore order quantities`, e);
          }

          // 4. ACK + delete Redis entries to prevent infinite retry loop
          try {
            for (const match of batch) {
              await this.batchContext.redis.xack(
                match.stream, this.batchContext.consumerGroup, match.id,
              );
              await this.batchContext.redis.xdel(match.stream, match.id);
            }
            // eslint-disable-next-line no-console
            console.log(
              `[batch-processor] ACKed and deleted ${batch.length} failed entries from Redis`,
            );
          } catch (e) {
            // eslint-disable-next-line no-console
            console.error(`[batch-processor] Failed to ACK/delete Redis entries`, e);
          }

          // Entries are cleaned up — no backoff needed, don't re-throw
          this.consecutiveFailures = 0;
          return;
        }
      } else {
        // Unexpected error - matches remain in Redis pending state for retry
        // eslint-disable-next-line no-console
        console.error(
          `[batch-processor] Unexpected error during batch processing, matches remain in Redis pending state`,
          {
            matchCount: batch.length,
            duration,
            error,
            matchIds: batch.map((m) => m.id),
          },
        );
      }

      this.consecutiveFailures += 1;
      const delay = Math.min(
        this.config.failureBackoffBaseMs * Math.pow(2, this.consecutiveFailures - 1),
        this.config.failureBackoffMaxMs,
      );
      this.nextRetryAt = Date.now() + delay;
      // eslint-disable-next-line no-console
      console.log(
        `[batch-processor] Backing off for ${delay}ms (failure ${this.consecutiveFailures})`,
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
        // eslint-disable-next-line no-console
        console.log(
          `[batch-processor] Reclaimed ${pendingMatches.length} pending matches, accumulator now has ${this.accumulator.getPendingCount()} pending`,
        );
      }
    } catch (error) {
      // eslint-disable-next-line no-console
      console.error('[batch-processor] Error reclaiming pending entries', error);
    }
  }

  /**
   * Recover unprocessed settlement events.
   * Finds settlement batches where event processing failed (events_processed = false)
   * and retries processing their stored raw events.
   */
  private async recoverUnprocessedEvents(): Promise<void> {
    if (!this.isRunning) {
      return;
    }

    try {
      const unprocessed = await findUnprocessedSettlementBatches(10);

      if (unprocessed.length === 0) {
        return;
      }

      // eslint-disable-next-line no-console
      console.log(
        `[batch-processor] Found ${unprocessed.length} unprocessed settlement batches, retrying event processing`,
      );

      for (const batch of unprocessed) {
        try {
          await retryEventProcessing(batch.id, batch.rawEvents, this.config);

          // eslint-disable-next-line no-console
          console.log(
            `[batch-processor] Successfully recovered events for batch ${batch.id}`,
          );
        } catch (error) {
          // eslint-disable-next-line no-console
          console.error(
            `[batch-processor] Failed to recover events for batch ${batch.id}`,
            {
              error: error instanceof Error ? error.message : String(error),
            },
          );
          // Continue with other batches — don't let one failure block the rest
        }
      }
    } catch (error) {
      // eslint-disable-next-line no-console
      console.error('[batch-processor] Error in event recovery loop', error);
    }
  }
}

