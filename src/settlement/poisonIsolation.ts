import type { Address } from 'viem';
import type { Match } from '../schemas/match';
import type { MatchWithMeta } from '../redis/settlementMatchConsumer';
import { loadConfig, type AppConfig } from '../config';
import { SETTLEMENT_CONTRACT_ABI } from './abi';
import {
  getPublicClient,
  getSettlerAddress,
  transformMatchToContractFormat,
  mapContractError,
  type SettlementError,
} from './smartContract';

/**
 * Poison-match isolation (Track C8).
 *
 * `Settlement.settleMatches(MatchData[])` is one on-chain tx for the whole
 * batch: a single invalid match reverts everything, the batch stays
 * `settlement_status='PENDING'`, and it keeps failing on retry — blocking every
 * valid match it was batched with. These helpers dry-run a batch off-chain so
 * the caller can drop the offender(s) and settle the survivors.
 *
 * Mechanism: read-only `simulateContract` (eth_call) with `from = settler`, so
 * the `onlyOperator` gate on `settleMatch(es)` passes. We deliberately do NOT
 * use a Multicall3 batch for the per-match probe — aggregate3 sub-calls run as
 * the Multicall3 contract, which fails `onlyOperator` and would flag EVERY
 * match as poison. Instead the per-match probe is N independent concurrent
 * eth_calls, each from the settler.
 */

/** Outcome of isolating poison matches out of an otherwise-valid batch. */
export interface PoisonIsolationResult {
  /** Matches that individually dry-ran clean. */
  readonly survivors: readonly MatchWithMeta[];
  /** Matches whose individual dry-run reverted non-retryably. */
  readonly poison: readonly MatchWithMeta[];
  /** Decoded revert code/message per poison match, keyed by Redis entry id. */
  readonly poisonReasons: ReadonlyMap<string, string>;
  /**
   * Whether the survivor set re-simulates clean with the real array semantics.
   * False means an interaction-only revert remains (individually-valid matches
   * that collectively still revert) — the caller should NOT settle them.
   */
  readonly survivorsSimulateClean: boolean;
}

/**
 * Whole-batch dry-run of `settleMatches`. This uses the real array ordering, so
 * it is the authoritative gate before submitting the live tx.
 *
 * @returns `null` if the batch would settle cleanly; otherwise the mapped
 *   {@link SettlementError} (its `retryable` flag distinguishes a transient
 *   RPC/paused failure from a real revert).
 */
export const simulateSettleBatch = async (
  matches: readonly Match[],
  config: AppConfig = loadConfig(),
  collateralAssetsByBorrower?: ReadonlyMap<string, readonly Address[]>,
): Promise<SettlementError | null> => {
  if (matches.length === 0) {
    return null; // empty batch is trivially clean (nothing to submit)
  }

  const publicClient = getPublicClient(config);
  const contractMatches = matches.map((m) =>
    transformMatchToContractFormat(m, collateralAssetsByBorrower),
  );

  try {
    await publicClient.simulateContract({
      address: config.settlementContractAddress as Address,
      abi: SETTLEMENT_CONTRACT_ABI,
      functionName: 'settleMatches',
      args: [contractMatches],
      account: getSettlerAddress(config),
    });
    return null;
  } catch (error) {
    return mapContractError(
      error,
      matches.map((m) => m.matchId),
    );
  }
};

/**
 * Pinpoint the poison match(es) in a batch the whole-batch dry-run rejected.
 *
 * Runs N concurrent single-match `settleMatch` dry-runs, partitions into
 * survivors / poison, then re-simulates the survivors as a `settleMatches`
 * array to catch interaction-only reverts the per-match pass cannot see.
 *
 * If ANY per-match probe (or the survivor re-sim) hits a transient/retryable
 * error, this THROWS that {@link SettlementError} rather than returning a
 * verdict — the caller must leave the whole batch pending for retry and must
 * NOT quarantine on a flaky RPC.
 */
export const simulateMatchesForPoison = async (
  matches: readonly MatchWithMeta[],
  config: AppConfig = loadConfig(),
  collateralAssetsByBorrower?: ReadonlyMap<string, readonly Address[]>,
): Promise<PoisonIsolationResult> => {
  const publicClient = getPublicClient(config);
  const account = getSettlerAddress(config);
  const address = config.settlementContractAddress as Address;

  const outcomes = await Promise.all(
    matches.map(async (m) => {
      try {
        await publicClient.simulateContract({
          address,
          abi: SETTLEMENT_CONTRACT_ABI,
          functionName: 'settleMatch',
          args: [transformMatchToContractFormat(m.payload, collateralAssetsByBorrower)],
          account,
        });
        return { match: m, error: null as SettlementError | null };
      } catch (error) {
        return { match: m, error: mapContractError(error, [m.payload.matchId]) };
      }
    }),
  );

  // A transient error on any probe poisons our confidence in the verdicts —
  // surface it so the caller retries the whole batch instead of quarantining.
  const transient = outcomes.find((o) => o.error?.retryable);
  if (transient?.error) {
    throw transient.error;
  }

  const survivors: MatchWithMeta[] = [];
  const poison: MatchWithMeta[] = [];
  const poisonReasons = new Map<string, string>();
  for (const outcome of outcomes) {
    if (outcome.error) {
      poison.push(outcome.match);
      poisonReasons.set(outcome.match.id, outcome.error.code ?? outcome.error.message);
    } else {
      survivors.push(outcome.match);
    }
  }

  // Re-validate survivors with the real array semantics. Empty survivors are
  // trivially clean (nothing left to settle).
  let survivorsSimulateClean = true;
  if (survivors.length > 0) {
    const survivorSim = await simulateSettleBatch(
      survivors.map((m) => m.payload),
      config,
      collateralAssetsByBorrower,
    );
    if (survivorSim?.retryable) {
      throw survivorSim; // transient — don't act
    }
    survivorsSimulateClean = survivorSim === null;
  }

  return { survivors, poison, poisonReasons, survivorsSimulateClean };
};
