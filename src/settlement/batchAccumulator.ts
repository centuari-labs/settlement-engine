import type { MatchWithMeta } from '../redis/settlementMatchConsumer';

/**
 * Batch accumulator that collects matches and determines when a batch should be processed.
 * Supports hybrid batching: process when either batch size threshold or time interval is reached.
 */
export class BatchAccumulator {
  private readonly queue: MatchWithMeta[] = [];
  private readonly seenIds: Set<string> = new Set();
  private lastProcessedTime: number = Date.now();
  private readonly batchSize: number;
  private readonly batchIntervalMs: number;
  private readonly maxCapacity: number;

  /**
   * Create a new batch accumulator.
   *
   * @param batchSize - Maximum number of matches per batch.
   * @param batchIntervalMs - Time interval in milliseconds after which a batch should be processed.
   * @param maxCapacity - Maximum queue size (backpressure). Default: batchSize * 5.
   */
  constructor(
    batchSize: number,
    batchIntervalMs: number,
    maxCapacity?: number,
  ) {
    this.batchSize = batchSize;
    this.batchIntervalMs = batchIntervalMs;
    this.maxCapacity = maxCapacity ?? batchSize * 5;
  }

  /**
   * Add matches to the accumulator queue.
   * Stops accepting when queue reaches maxCapacity (backpressure).
   *
   * @param matches - Matches to add to the queue.
   */
  addMatches(matches: readonly MatchWithMeta[]): void {
    for (const match of matches) {
      if (this.queue.length >= this.maxCapacity) {
        break;
      }
      if (this.seenIds.has(match.id)) {
        continue;
      }

      this.seenIds.add(match.id);
      this.queue.push(match);
    }
  }

  /**
   * Check if the batch should be processed.
   * Returns true if either:
   * - The queue size has reached the batch size threshold, OR
   * - The time interval has elapsed since the last processing.
   *
   * @returns True if batch should be processed, false otherwise.
   */
  shouldProcess(): boolean {
    if (this.queue.length >= this.batchSize) {
      return true;
    }

    const timeSinceLastProcess = Date.now() - this.lastProcessedTime;
    if (timeSinceLastProcess >= this.batchIntervalMs && this.queue.length > 0) {
      return true;
    }

    return false;
  }

  /**
   * Check if the accumulator needs more matches.
   * Returns true if the queue is not at batch size threshold yet.
   * This is used to determine when to trigger reads from Redis.
   *
   * @returns True if more matches are needed, false otherwise.
   */
  needsMoreMatches(): boolean {
    return this.queue.length < this.batchSize;
  }

  /**
   * Get the current batch and clear the queue.
   * Also resets the last processed time.
   *
   * @returns Array of matches in the current batch.
   */
  getBatch(): MatchWithMeta[] {
    const batch = [...this.queue];
    this.queue.length = 0;
    this.seenIds.clear();
    this.resetTimer();
    return batch;
  }

  /**
   * Get the current number of pending matches in the queue.
   *
   * @returns Number of pending matches.
   */
  getPendingCount(): number {
    return this.queue.length;
  }

  /**
   * Reset the time-based trigger timer.
   * Called after processing a batch to start the timer for the next batch.
   */
  resetTimer(): void {
    this.lastProcessedTime = Date.now();
  }
}

