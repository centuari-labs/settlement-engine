import type Redis from 'ioredis';
import type { Address } from 'viem';
import type { MatchWithMeta } from '../redis/settlementMatchConsumer';
import type { AppConfig } from '../config';
import { logger } from '../logger';
import {
  settleBatch,
  filterAlreadySettledMatches,
  getPublicClient,
  type SettlementError,
} from './smartContract';
import {
  applySettlementResult,
  getPool,
  readPendingCollateralFlagsForBorrowers,
  quarantineFailedMatch,
  writebackSettledMatches,
  POISON_FAILURE_REASON,
  type DatabaseError,
} from './database';
import {
  simulateSettleBatch,
  simulateMatchesForPoison,
} from './poisonIsolation';
import { loadConfig } from '../config';
import type { NonceManager } from './nonceManager';

/**
 * Context for batch settlement processing that provides access to Redis and
 * stream configuration needed for deletion and trimming.
 */
export interface SettlementBatchContext {
  /**
   * Redis client used for stream operations.
   */
  readonly redis: Redis;
  /**
   * Name of the Redis stream that carries settlement matches.
   */
  readonly stream: string;
  /**
   * Consumer group name for ACKing entries.
   */
  readonly consumerGroup: string;
  /**
   * Maximum number of entries to keep in the stream after trimming.
   */
  readonly streamMaxLen: number;
  /**
   * Nonce manager for explicit nonce sequencing across instances.
   */
  readonly nonceManager?: NonceManager;
}

/**
 * Error that occurred during batch processing.
 */
export class BatchProcessingError extends Error {
  /**
   * Whether the error is retryable.
   */
  readonly retryable: boolean;
  /**
   * Original error that caused the failure.
   */
  readonly originalError: unknown;

  constructor(message: string, retryable: boolean, originalError: unknown) {
    super(message);
    this.name = 'BatchProcessingError';
    this.retryable = retryable;
    this.originalError = originalError;
  }
}

/**
 * ACK and delete stream entries from Redis.
 */
const ackAndDeleteEntries = async (
  context: SettlementBatchContext,
  matches: readonly MatchWithMeta[],
): Promise<void> => {
  const idsByStream = new Map<string, string[]>();
  for (const match of matches) {
    const ids = idsByStream.get(match.stream) ?? [];
    ids.push(match.id);
    idsByStream.set(match.stream, ids);
  }

  for (const [stream, ids] of idsByStream.entries()) {
    for (const id of ids) {
      await context.redis.xack(stream, context.consumerGroup, id);
    }
  }

  const DELETE_BATCH_SIZE = 100;
  for (const [stream, ids] of idsByStream.entries()) {
    for (let i = 0; i < ids.length; i += DELETE_BATCH_SIZE) {
      const batch = ids.slice(i, i + DELETE_BATCH_SIZE);
      await context.redis.xdel(stream, ...batch);
    }
  }
};

/**
 * Sentinel `settled_tx_hash` for a ghost-settled match (H1).
 *
 * On a retry where the match is already settled on-chain (`isSettled == true`)
 * but our prior writeback never flipped PENDING -> SETTLED, we don't have the
 * original settlement tx hash to hand to the lock-release writeback. We stamp
 * this sentinel so the row is auditably marked as recovered by the
 * already-settled fast-path rather than a fresh in-process settlement.
 */
export const GHOST_SETTLED_TX_HASH = 'ghost-settled-recovered';

/**
 * H1 — release stranded `in_orders` for matches the retry path found already
 * settled on-chain. The original ACK/XDEL fast-path skipped the lock-release
 * decrement, so `in_orders` stayed over-counted forever. `writebackSettledMatches`
 * is idempotent: its inner `UPDATE matches ... WHERE settlement_status='PENDING'`
 * returns 0 rows (and fires no decrement) for any match already SETTLED, so it
 * is safe to run on every already-settled match — it decrements exactly once,
 * on the first transition, and no-ops on every subsequent retry.
 *
 * Best-effort: a failure here is logged but NOT thrown. The on-chain settlement
 * is already final, so we must still ACK/XDEL to stop infinite redelivery; any
 * row left PENDING is picked up by the stuck-PENDING reconciliation sweeper.
 */
const releaseLocksForAlreadySettled = async (
  alreadySettled: readonly MatchWithMeta[],
): Promise<void> => {
  if (alreadySettled.length === 0) {
    return;
  }
  try {
    const settledMatchIds = new Set(
      alreadySettled.map((m) => m.payload.matchId),
    );
    const result = await writebackSettledMatches(
      getPool(),
      alreadySettled.map((m) => m.payload),
      settledMatchIds,
      GHOST_SETTLED_TX_HASH,
    );
    logger.info(
      {
        component: 'process-settlement-batch',
        count: alreadySettled.length,
        released: result.settled,
        alreadyReleased: result.alreadySettled,
      },
      'Released locks for already-settled matches (H1)',
    );
  } catch (error) {
    // On-chain settlement is final; do not block the ACK/XDEL below. The
    // stuck-PENDING reconciliation sweeper is the backstop for any row this
    // leaves behind.
    logger.error(
      {
        component: 'process-settlement-batch',
        count: alreadySettled.length,
        err: (error as Error).message,
      },
      'Failed to release locks for already-settled matches (H1) — ACKing anyway',
    );
  }
};

/**
 * Quarantine poison matches (Track C8): mark each FAILED + restore orders +
 * release its `in_orders` lock in one atomic tx (`quarantineFailedMatch`), then
 * ACK/XDEL its Redis entry so it is never redelivered into a future batch.
 * Idempotent — a match already FAILED no-ops the DB flip but is still ACKed.
 */
const quarantinePoisonMatches = async (
  context: SettlementBatchContext,
  poison: readonly MatchWithMeta[],
  reasons: ReadonlyMap<string, string>,
): Promise<void> => {
  for (const match of poison) {
    const code = reasons.get(match.id);
    const reason = code ? `${POISON_FAILURE_REASON}:${code}` : POISON_FAILURE_REASON;
    await quarantineFailedMatch(match.payload, reason);
  }
  await ackAndDeleteEntries(context, poison);
};

/**
 * Process a batch of settlement matches.
 *
 * This function:
 * 1. Filters out already-settled matches (ACKs them from Redis immediately)
 * 2. Calls the smart contract to settle the remaining batch
 * 3. Persists settlement results to the database
 * 4. ACKs and deletes stream entries after successful settlement
 *
 * If any step fails, the entries remain in pending state for retry.
 *
 * @param matches - Array of matches to process in the batch.
 * @param context - Context providing Redis client and stream configuration.
 * @param config - Optional application configuration. If not provided, will be loaded from environment.
 * @throws BatchProcessingError if settlement fails (entries remain pending).
 */
export const processSettlementBatch = async (
  matches: readonly MatchWithMeta[],
  context: SettlementBatchContext,
  config?: AppConfig,
): Promise<void> => {
  if (matches.length === 0) {
    return;
  }

  // Poison-match isolation: one invalid match in the batch reverts the whole
  // on-chain tx and keeps failing on retry. filterAlreadySettledMatches only
  // drops already-settled matches, not invalid ones. Tracked: hub-only plan
  // Track C8.

  logger.info(
    {
      component: 'process-settlement-batch',
      matchCount: matches.length,
      matches: matches.map((match) => ({
        id: match.id,
        matchId: match.payload.matchId,
        lendOrderId: match.payload.lendOrderId,
        borrowOrderId: match.payload.borrowOrderId,
      })),
    },
    'Processing batch',
  );

  // Step 0: Filter out already-settled matches (prevents infinite retry loop)
  const { unsettled, alreadySettled } = await filterAlreadySettledMatches(
    matches,
    config,
  );

  if (alreadySettled.length > 0) {
    // H1: run the idempotent lock-release BEFORE ACK/XDEL. The ghost-settled
    // retry path (tx mined, but writeback threw before the PENDING -> SETTLED
    // flip) would otherwise ACK/XDEL these without ever decrementing
    // `in_orders`, stranding the reservation forever.
    await releaseLocksForAlreadySettled(alreadySettled);
    await ackAndDeleteEntries(context, alreadySettled);
    logger.info(
      { component: 'process-settlement-batch', count: alreadySettled.length },
      'ACKed already-settled matches',
    );
  }

  if (unsettled.length === 0) {
    // All matches were already settled; we're done
    return;
  }

  // Step 0.5: Read `pending_collateral_flags` for the distinct borrowers in
  // this batch. The result map drives `MatchData.collateralAssets` per match
  // inside `settleBatch` → `transformMatchToContractFormat`. Reading here
  // (immediately before submitting the on-chain tx) keeps the race window
  // between user unflag and settle to ~tens of milliseconds — the queue
  // truly represents the user's latest intent at submit time. The match
  // payload's plumbing-only `borrowerCollateralAssets` (P2) is intentionally
  // ignored: a stale order-time snapshot would defeat the unflag race fix.
  let collateralAssetsByBorrower: Map<string, readonly Address[]> = new Map();
  try {
    const distinctBorrowers = Array.from(
      new Set(
        unsettled.map((m) => (m.payload.borrowerWallet as Address).toLowerCase()),
      ),
    ) as Address[];
    collateralAssetsByBorrower = await readPendingCollateralFlagsForBorrowers(
      getPool(),
      distinctBorrowers,
    );
    if (collateralAssetsByBorrower.size > 0) {
      logger.info(
        {
          component: 'process-settlement-batch',
          borrowersWithFlags: collateralAssetsByBorrower.size,
          totalAssets: Array.from(collateralAssetsByBorrower.values()).reduce(
            (sum, arr) => sum + arr.length,
            0,
          ),
        },
        'Read pending collateral flags for batch',
      );
    }
  } catch (error) {
    // Reading the queue is best-effort: if it fails we still settle the
    // batch with empty `collateralAssets` arrays. The user's queued flags
    // will land at the next settlement (queue rows persist) or via a
    // direct-caller `CollateralManager.flag(asset)` call. Aborting the
    // entire batch over a transient queue read is the wrong tradeoff.
    logger.error(
      {
        component: 'process-settlement-batch',
        err: (error as Error).message,
        borrowerCount: unsettled.length,
      },
      'Failed to read pending_collateral_flags — settling without collateral encoding',
    );
  }

  const cfg = config ?? loadConfig();

  // Step 0.75: Poison-match isolation (Track C8). Dry-run the batch off-chain;
  // if it would revert, quarantine the offending match(es) (mark FAILED +
  // restore orders + release locks + ACK from Redis) and settle only the
  // survivors. Without this, one invalid match reverts the whole tx and keeps
  // failing on retry, blocking every valid match it was batched with. This is
  // required behavior and always runs (no opt-out flag).
  let toSettle: readonly MatchWithMeta[] = unsettled;
  try {
    const batchSim = await simulateSettleBatch(
      unsettled.map((m) => m.payload),
      cfg,
      collateralAssetsByBorrower,
    );
    if (batchSim !== null) {
      if (batchSim.retryable) {
        // Transient (RPC down / paused / nonce). Do NOT quarantine — leave
        // the batch PENDING for retry.
        throw new BatchProcessingError(
          `Pre-flight simulation transient failure: ${batchSim.message}`,
          true,
          batchSim,
        );
      }
      // Real revert: isolate the poison match(es).
      const iso = await simulateMatchesForPoison(
        unsettled,
        cfg,
        collateralAssetsByBorrower,
      );
      if (iso.poison.length > 0) {
        await quarantinePoisonMatches(context, iso.poison, iso.poisonReasons);
        logger.warn(
          {
            component: 'process-settlement-batch',
            poisonCount: iso.poison.length,
            survivorCount: iso.survivors.length,
            poisonMatchIds: iso.poison.map((m) => m.payload.matchId),
          },
          'Quarantined poison matches; settling survivors',
        );
      }
      if (!iso.survivorsSimulateClean) {
        // Survivors still revert collectively after one isolation round
        // (interaction-only failure). Don't guess — hand the remaining batch
        // to the existing whole-batch failure path.
        throw new BatchProcessingError(
          'Survivors still revert after poison isolation',
          false,
          batchSim,
        );
      }
      toSettle = iso.survivors;
    }
  } catch (error) {
    if (error instanceof BatchProcessingError) {
      throw error;
    }
    // simulateMatchesForPoison throws a transient SettlementError when an RPC
    // probe is flaky — leave the batch pending rather than quarantining.
    const se = error as SettlementError;
    const retryable = se?.retryable !== undefined ? se.retryable : true;
    throw new BatchProcessingError(
      `Poison isolation failed: ${se?.message ?? String(error)}`,
      retryable,
      error,
    );
  }

  if (toSettle.length === 0) {
    // Every match was poison (or already-settled). Nothing left to submit.
    logger.info(
      { component: 'process-settlement-batch' },
      'No matches left to settle after poison isolation',
    );
    return;
  }

  let settlementResult;
  try {
    // Step 1: Call smart contract to settle the batch.
    //
    // KNOWN, ACCEPTED TOCTOU (M5): there is a check-to-use gap between the
    // poison-isolation dry-run above (Step 0.75 — an off-chain `simulateContract`
    // / eth_call against the current head) and this live `settleBatch` tx. State
    // can change in that window (a collateral unflag lands, an oracle price moves,
    // another settler races a match), so a match that simulated clean can still
    // revert on-chain — and, conversely, a match flagged poison could have become
    // settleable. We deliberately do NOT try to close this gap with a lock or a
    // single atomic call: the on-chain contract is the final authority. If the
    // live tx reverts, the batch stays PENDING and is retried (poison isolation
    // re-runs against fresh state on the next pass); a genuinely-stuck match is
    // ultimately caught by the stuck-PENDING reconciliation sweeper (Track C2).
    // The dry-run is a best-effort filter to keep one poison match from blocking
    // a whole batch, not a correctness guarantee.
    const startTime = Date.now();
    settlementResult = await settleBatch({
      matches: toSettle.map((m) => m.payload),
      config,
      nonceManager: context.nonceManager,
      collateralAssetsByBorrower,
    });
    const duration = Date.now() - startTime;

    logger.info(
      {
        component: 'process-settlement-batch',
        transactionHash: settlementResult.transactionHash,
        blockNumber: settlementResult.blockNumber,
        gasUsed: settlementResult.gasUsed,
        duration,
        matchCount: toSettle.length,
      },
      'Smart contract settlement successful',
    );
  } catch (error) {
    const settlementError = error as SettlementError;
    const isRetryable =
      settlementError.retryable !== undefined
        ? settlementError.retryable
        : true;

    logger.error(
      {
        component: 'process-settlement-batch',
        err: settlementError.message,
        code: settlementError.code,
        retryable: isRetryable,
        matchCount: toSettle.length,
      },
      'Smart contract settlement failed',
    );

    throw new BatchProcessingError(
      `Smart contract settlement failed: ${settlementError.message}`,
      isRetryable,
      error,
    );
  }

  // Step 2: Eager-write the parsed events to indexer-v3's schema. Each event
  // is committed in its own pg tx with applied_by_* stamps. If this fails
  // mid-batch the on-chain tx is already final, so we don't ACK Redis —
  // retry will no-op the already-stamped rows via their stamps, and the
  // indexer-v3 tail is a secondary safety net for any gaps we leave behind.
  try {
    const startTime = Date.now();
    // The cached viem public client is typed narrowly by createPublicClient;
    // applyOnChainEffect declares the general PublicClient interface. Cast
    // is safe because we always pass a pre-fetched receipt — client is never
    // dereferenced by the helper in this path.
    const publicClient = getPublicClient(
      config ?? loadConfig(),
    ) as unknown as import('viem').PublicClient;
    await applySettlementResult(
      getPool(),
      publicClient,
      settlementResult,
      toSettle.map((m) => m.payload),
    );
    const duration = Date.now() - startTime;

    logger.info(
      { component: 'process-settlement-batch', duration, matchCount: toSettle.length },
      'Applied settlement result to indexer-v3 schema',
    );
  } catch (error) {
    const dbError = error as DatabaseError;
    const isRetryable =
      dbError.retryable !== undefined ? dbError.retryable : true;

    logger.error(
      {
        component: 'process-settlement-batch',
        err: dbError.message ?? (error as Error).message,
        code: dbError.code,
        retryable: isRetryable,
        matchCount: toSettle.length,
      },
      'Settlement apply to indexer-v3 schema failed',
    );

    throw new BatchProcessingError(
      `Settlement apply failed: ${dbError.message ?? (error as Error).message}`,
      isRetryable,
      error,
    );
  }

  // Step 3: ACK and delete entries from Redis.
  // Phase 1 committed settlement records + raw events, so even if Phase 2
  // (event processing) failed, the data is safe and the recovery loop will retry.
  await ackAndDeleteEntries(context, toSettle);

  // Apply a length-based trim on the main settlement stream to enforce a
  // bounded retention window and avoid unbounded growth.
  await context.redis.xtrim(
    context.stream,
    'MAXLEN',
    '~',
    context.streamMaxLen,
  );

  logger.info(
    {
      component: 'process-settlement-batch',
      matchCount: toSettle.length,
      transactionHash: settlementResult.transactionHash,
    },
    'Batch processing complete',
  );
};


