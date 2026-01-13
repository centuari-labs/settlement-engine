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
  private processingPromise: Promise<void> | null = null;

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
      },
    );

    // Start polling loop
    this.pollIntervalId = setInterval(() => {
      void this.poll();
    }, this.config.pollIntervalMs);

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

        // First, try to process pending entries (retry failed matches)
        const pendingMatches = await processPendingEntriesOnStartup(
          this.readMatchesOptions,
        );
        if (pendingMatches.length > 0) {
          this.accumulator.addMatches(pendingMatches);
          // eslint-disable-next-line no-console
          console.log(
            `[batch-processor] Processed ${pendingMatches.length} pending matches, accumulator now has ${this.accumulator.getPendingCount()} pending`,
          );
        }

        // Then, read new matches from stream
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

      await processSettlementBatch(batch, this.batchContext);

      const duration = Date.now() - startTime;

      // eslint-disable-next-line no-console
      console.log(
        `[batch-processor] Batch processing successful`,
        {
          matchCount: batch.length,
          duration,
        },
      );
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
            `[batch-processor] Non-retryable error, matches remain in Redis pending state for manual intervention`,
            {
              matchIds: batch.map((m) => m.id),
            },
          );
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

      throw error;
    }
  }
}

