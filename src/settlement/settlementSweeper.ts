import type { AppConfig } from '../config';
import { logger } from '../logger';
import { checkMatchesSettledOnChain } from './smartContract';
import { findStuckPendingMatches, remediateUnsettledMatch } from './database';

/**
 * Options for creating a settlement sweeper.
 */
export interface SettlementSweeperOptions {
  /**
   * Application configuration (sweeper interval/threshold/batch size + on-chain reads).
   */
  readonly config: AppConfig;
}

/**
 * Counters for a single sweep run. Returned for tests and metrics/logging.
 */
export interface SweepResult {
  /** Stuck candidates scanned this run. */
  readonly scanned: number;
  /** Matches confirmed not-settled on-chain: lock released + orders restored + marked FAILED. */
  readonly failedUnlocked: number;
  /** Matches settled on-chain but missing DB records: quarantined + alerted. */
  readonly ghostSettled: number;
  /** Matches whose on-chain status could not be read: skipped, retried later. */
  readonly skippedUnknown: number;
}

/**
 * Track C2 — stuck-PENDING settlement sweeper.
 *
 * Periodically finds matches still `settlement_status = 'PENDING'` past the
 * stuck threshold (settlement never landed — engine crash mid-batch, process
 * died before writeback, or the already-settled fast-path acked without
 * persisting). Each is verified on-chain and then:
 *   - not settled on-chain -> release both sides' in_orders + restore the
 *     orders + mark the match FAILED (atomically, via remediateUnsettledMatch)
 *   - settled on-chain      -> quarantine + critical alert (the DB record was
 *     lost; positions/bond mints cannot be safely reconstructed without the
 *     settling-tx events, so balances are left untouched for manual/indexer
 *     reconcile)
 *   - unknown (RPC failure) -> skip; retried on the next sweep
 *
 * Mirrors {@link BatchProcessor}'s timer lifecycle. A sweep failure never
 * crashes the engine, and overlapping sweeps are prevented.
 */
export class SettlementSweeper {
  private readonly config: AppConfig;
  private isRunning = false;
  private sweepIntervalId: NodeJS.Timeout | null = null;
  private sweepPromise: Promise<void> | null = null;

  /**
   * Create a new settlement sweeper.
   *
   * @param options - Options for creating the sweeper.
   */
  constructor(options: SettlementSweeperOptions) {
    this.config = options.config;
  }

  /**
   * Start the sweeper loop. Runs an initial sweep immediately, then on an
   * interval.
   */
  start(): void {
    if (this.isRunning) {
      logger.warn({ component: 'sweeper' }, 'Already running');
      return;
    }
    this.isRunning = true;

    logger.info(
      {
        component: 'sweeper',
        intervalMs: this.config.sweeperIntervalMs,
        stuckThresholdMs: this.config.sweeperStuckThresholdMs,
        batchSize: this.config.sweeperBatchSize,
      },
      'Starting stuck-PENDING settlement sweeper',
    );

    this.sweepIntervalId = setInterval(() => {
      void this.runSweep();
    }, this.config.sweeperIntervalMs);

    // Initial sweep on startup.
    void this.runSweep();
  }

  /**
   * Stop the sweeper. Waits for any in-flight sweep to finish.
   *
   * @returns Promise that resolves when the sweeper has stopped.
   */
  async stop(): Promise<void> {
    if (!this.isRunning) {
      return;
    }
    logger.info({ component: 'sweeper' }, 'Stopping settlement sweeper...');
    this.isRunning = false;

    if (this.sweepIntervalId) {
      clearInterval(this.sweepIntervalId);
      this.sweepIntervalId = null;
    }

    if (this.sweepPromise) {
      await this.sweepPromise;
    }
    logger.info({ component: 'sweeper' }, 'Settlement sweeper stopped');
  }

  /**
   * Guarded wrapper used by the timer: prevents overlapping sweeps and ensures a
   * failing sweep never crashes the process.
   */
  private async runSweep(): Promise<void> {
    if (!this.isRunning || this.sweepPromise) {
      return;
    }
    this.sweepPromise = this.sweep()
      .then(() => undefined)
      .catch((error) => {
        logger.error(
          { component: 'sweeper', err: error },
          'Sweep failed',
        );
      })
      .finally(() => {
        this.sweepPromise = null;
      });
    await this.sweepPromise;
  }

  /**
   * Run a single sweep. Public so tests can invoke it directly.
   *
   * @returns Counters for the run.
   */
  async sweep(): Promise<SweepResult> {
    const candidates = await findStuckPendingMatches(
      this.config.sweeperStuckThresholdMs,
      this.config.sweeperBatchSize,
    );

    if (candidates.length === 0) {
      return { scanned: 0, failedUnlocked: 0, ghostSettled: 0, skippedUnknown: 0 };
    }

    logger.info(
      { component: 'sweeper', count: candidates.length },
      'Found stuck PENDING match(es), verifying on-chain',
    );

    const settledMap = await checkMatchesSettledOnChain(
      candidates.map((m) => m.id),
      this.config,
    );

    let failedUnlocked = 0;
    let ghostSettled = 0;
    let skippedUnknown = 0;

    for (const match of candidates) {
      const settled = settledMap.get(match.id);

      if (settled === undefined) {
        skippedUnknown += 1;
        logger.warn(
          { component: 'sweeper', matchId: match.id },
          'On-chain status unknown, skipping this round',
        );
        continue;
      }

      if (settled) {
        // Ghost-settled: settled on-chain but the DB record was lost. Positions
        // cannot be safely reconstructed without the settling-tx events, so we
        // do NOT mutate balances — quarantine + critical alert.
        ghostSettled += 1;
        logger.error(
          { component: 'sweeper', matchId: match.id, createdAt: match.createdAt },
          'GHOST-SETTLED match needs manual reconcile: settled on-chain but still PENDING in DB',
        );
        continue;
      }

      // Not settled on-chain: the real stuck case. Release locks + restore orders + mark FAILED.
      try {
        const actioned = await remediateUnsettledMatch(match);
        if (actioned) {
          failedUnlocked += 1;
          logger.warn(
            {
              component: 'sweeper',
              matchId: match.id,
              matchedAmount: match.matchedAmount,
              lenderWallet: match.lenderWallet,
              borrowerWallet: match.borrowerWallet,
            },
            'Released stuck locks, restored orders, marked match FAILED',
          );
        }
      } catch (error) {
        logger.error(
          { component: 'sweeper', matchId: match.id, err: error },
          'Failed to resolve stuck match; leaving for next sweep',
        );
        // Leave it for the next sweep.
      }
    }

    const result: SweepResult = {
      scanned: candidates.length,
      failedUnlocked,
      ghostSettled,
      skippedUnknown,
    };

    logger.info({ component: 'sweeper', ...result }, 'Sweep complete');

    return result;
  }
}
