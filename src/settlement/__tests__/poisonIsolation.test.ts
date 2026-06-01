/**
 * Tests for poisonIsolation.ts — the real implementation.
 *
 * poisonIsolation imports getPublicClient / getSettlerAddress /
 * transformMatchToContractFormat / mapContractError from smartContract, which
 * setup.ts globally mocks. We unmock it so the real helpers run, and spy
 * getPublicClient to inject a fake client whose simulateContract we drive.
 */

jest.unmock('../smartContract');

import * as smartContract from '../smartContract';
import {
  simulateSettleBatch,
  simulateMatchesForPoison,
} from '../poisonIsolation';
import { createMatch, createMatchWithMeta } from '../../tests/helpers/testFixtures';
import { createTestConfig } from '../../tests/helpers/testConfig';

const config = createTestConfig();

// Fake viem public client with a controllable simulateContract.
const simulateContract = jest.fn();
const fakeClient = { simulateContract } as unknown as ReturnType<
  typeof smartContract.getPublicClient
>;

beforeEach(() => {
  jest.clearAllMocks();
  jest.spyOn(smartContract, 'getPublicClient').mockReturnValue(fakeClient);
});

describe('simulateSettleBatch', () => {
  it('returns null when the whole-batch dry-run succeeds', async () => {
    simulateContract.mockResolvedValue({ result: undefined });

    const result = await simulateSettleBatch([createMatch()], config);

    expect(result).toBeNull();
    expect(simulateContract).toHaveBeenCalledTimes(1);
    expect(simulateContract.mock.calls[0][0]).toEqual(
      expect.objectContaining({ functionName: 'settleMatches' }),
    );
  });

  it('returns a non-retryable SettlementError on a real revert', async () => {
    simulateContract.mockRejectedValue(new Error('InvalidMatchData()'));

    const result = await simulateSettleBatch([createMatch()], config);

    expect(result).not.toBeNull();
    expect(result?.retryable).toBe(false);
    expect(result?.code).toBe('INVALID_MATCH_DATA');
  });

  it('returns a retryable SettlementError on a transient RPC failure', async () => {
    simulateContract.mockRejectedValue(new Error('request timeout'));

    const result = await simulateSettleBatch([createMatch()], config);

    expect(result?.retryable).toBe(true);
    expect(result?.code).toBe('NETWORK_ERROR');
  });

  it('treats an empty batch as trivially clean without an RPC call', async () => {
    const result = await simulateSettleBatch([], config);

    expect(result).toBeNull();
    expect(simulateContract).not.toHaveBeenCalled();
  });
});

describe('simulateMatchesForPoison', () => {
  // Poison match is tagged by a sentinel matchedAmount so the mock can revert
  // exactly its single-match probe.
  const POISON_AMOUNT = '999';

  const driveProbe = () =>
    simulateContract.mockImplementation(
      async (opts: { functionName: string; args: readonly unknown[] }) => {
        if (opts.functionName === 'settleMatch') {
          const md = opts.args[0] as { matchedAmount: bigint };
          if (md.matchedAmount === BigInt(POISON_AMOUNT)) {
            throw new Error('InsufficientFunds()');
          }
          return { result: undefined };
        }
        // settleMatches survivor re-sim: clean by default.
        return { result: undefined };
      },
    );

  it('partitions poison from survivors and re-simulates survivors clean', async () => {
    driveProbe();
    const m1 = createMatchWithMeta({ matchId: '550e8400-e29b-41d4-a716-446655440001' });
    const poison = createMatchWithMeta({
      matchId: '550e8400-e29b-41d4-a716-446655440002',
      matchedAmount: POISON_AMOUNT,
    });
    const m3 = createMatchWithMeta({ matchId: '550e8400-e29b-41d4-a716-446655440003' });

    const result = await simulateMatchesForPoison([m1, poison, m3], config);

    expect(result.poison.map((m) => m.id)).toEqual([poison.id]);
    expect(result.survivors.map((m) => m.id)).toEqual([m1.id, m3.id]);
    expect(result.poisonReasons.get(poison.id)).toBe('INSUFFICIENT_FUNDS');
    expect(result.survivorsSimulateClean).toBe(true);
    // 3 per-match probes + 1 survivor re-sim.
    expect(simulateContract).toHaveBeenCalledTimes(4);
  });

  it('throws the transient error if any per-match probe is flaky (no verdict)', async () => {
    simulateContract.mockImplementation(
      async (opts: { functionName: string; args: readonly unknown[] }) => {
        const md = opts.args[0] as { matchedAmount: bigint };
        if (md.matchedAmount === BigInt(POISON_AMOUNT)) {
          throw new Error('connection timeout');
        }
        return { result: undefined };
      },
    );
    const ok = createMatchWithMeta({ matchId: '550e8400-e29b-41d4-a716-446655440001' });
    const flaky = createMatchWithMeta({
      matchId: '550e8400-e29b-41d4-a716-446655440002',
      matchedAmount: POISON_AMOUNT,
    });

    await expect(simulateMatchesForPoison([ok, flaky], config)).rejects.toMatchObject({
      retryable: true,
    });
  });

  it('reports survivorsSimulateClean=false on an interaction-only revert', async () => {
    // All matches pass their individual probe, but the survivor (full) re-sim
    // reverts non-retryably — the collectively-invalid case.
    simulateContract.mockImplementation(
      async (opts: { functionName: string }) => {
        if (opts.functionName === 'settleMatch') {
          return { result: undefined };
        }
        throw new Error('InvalidMatchData()'); // settleMatches re-sim reverts
      },
    );
    const m1 = createMatchWithMeta({ matchId: '550e8400-e29b-41d4-a716-446655440001' });
    const m2 = createMatchWithMeta({ matchId: '550e8400-e29b-41d4-a716-446655440002' });

    const result = await simulateMatchesForPoison([m1, m2], config);

    expect(result.poison).toHaveLength(0);
    expect(result.survivors).toHaveLength(2);
    expect(result.survivorsSimulateClean).toBe(false);
  });

  it('throws the transient error if the survivor re-simulation is flaky', async () => {
    // The per-match probe quarantines one poison match, but the survivor
    // re-sim then hits a transient RPC error — surface it so the caller retries
    // the whole batch instead of settling on an unverified survivor set.
    simulateContract.mockImplementation(
      async (opts: { functionName: string; args: readonly unknown[] }) => {
        if (opts.functionName === 'settleMatch') {
          const md = opts.args[0] as { matchedAmount: bigint };
          if (md.matchedAmount === BigInt(POISON_AMOUNT)) {
            throw new Error('InsufficientFunds()');
          }
          return { result: undefined };
        }
        throw new Error('connection timeout'); // survivor re-sim is flaky
      },
    );
    const survivor = createMatchWithMeta({ matchId: '550e8400-e29b-41d4-a716-446655440001' });
    const poison = createMatchWithMeta({
      matchId: '550e8400-e29b-41d4-a716-446655440002',
      matchedAmount: POISON_AMOUNT,
    });

    await expect(
      simulateMatchesForPoison([survivor, poison], config),
    ).rejects.toMatchObject({ retryable: true });
  });

  it('treats an all-poison batch as trivially clean survivors (no re-sim call)', async () => {
    simulateContract.mockImplementation(
      async (opts: { functionName: string }) => {
        if (opts.functionName === 'settleMatch') {
          throw new Error('InsufficientFunds()');
        }
        throw new Error('settleMatches should not be called for empty survivors');
      },
    );
    const p1 = createMatchWithMeta({ matchId: '550e8400-e29b-41d4-a716-446655440001' });
    const p2 = createMatchWithMeta({ matchId: '550e8400-e29b-41d4-a716-446655440002' });

    const result = await simulateMatchesForPoison([p1, p2], config);

    expect(result.poison).toHaveLength(2);
    expect(result.survivors).toHaveLength(0);
    expect(result.survivorsSimulateClean).toBe(true);
    // Only the 2 per-match probes; no survivor re-sim.
    expect(simulateContract).toHaveBeenCalledTimes(2);
  });
});
