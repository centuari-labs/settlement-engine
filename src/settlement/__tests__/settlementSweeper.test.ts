import { SettlementSweeper } from '../settlementSweeper';
import {
  findStuckPendingMatches,
  remediateUnsettledMatch,
  type StuckMatch,
} from '../database';
import { checkMatchesSettledOnChain } from '../smartContract';
import { createTestConfig } from '../../tests/helpers/testConfig';

// Mock only the two database functions the sweeper uses, with an explicit
// factory — this avoids loading the real database/ barrel (and its heavy,
// on-chain-effects-dependent modules) just to derive an auto-mock shape.
// '../smartContract' is auto-mocked globally in tests/setup.ts.
jest.mock('../database', () => ({
  findStuckPendingMatches: jest.fn(),
  remediateUnsettledMatch: jest.fn(),
}));

const mockFind = findStuckPendingMatches as jest.MockedFunction<
  typeof findStuckPendingMatches
>;
const mockRemediate = remediateUnsettledMatch as jest.MockedFunction<
  typeof remediateUnsettledMatch
>;
const mockCheck = checkMatchesSettledOnChain as jest.MockedFunction<
  typeof checkMatchesSettledOnChain
>;

const stuckMatch = (id: string): StuckMatch => ({
  id,
  lendOrderId: `lend-${id}`,
  borrowOrderId: `borrow-${id}`,
  lenderWallet: '0x1111111111111111111111111111111111111111',
  borrowerWallet: '0x2222222222222222222222222222222222222222',
  loanToken: '0x3333333333333333333333333333333333333333',
  matchedAmount: '1000',
  lenderSettlementFee: '10',
  borrowerSettlementFee: '5',
  makerFee: '2',
  takerFee: '3',
  borrowerIsTaker: true,
  createdAt: new Date('2026-01-01T00:00:00Z'),
});

describe('SettlementSweeper.sweep', () => {
  const config = createTestConfig({
    sweeperStuckThresholdMs: 1000,
    sweeperBatchSize: 50,
  });
  let sweeper: SettlementSweeper;

  beforeEach(() => {
    jest.clearAllMocks();
    mockRemediate.mockResolvedValue(true);
    mockCheck.mockResolvedValue(new Map());
    sweeper = new SettlementSweeper({ config });
  });

  it('does nothing when there are no stuck candidates', async () => {
    mockFind.mockResolvedValue([]);

    const result = await sweeper.sweep();

    expect(result).toEqual({
      scanned: 0,
      failedUnlocked: 0,
      ghostSettled: 0,
      skippedUnknown: 0,
    });
    expect(mockCheck).not.toHaveBeenCalled();
    expect(mockRemediate).not.toHaveBeenCalled();
  });

  it('queries with the configured threshold and batch size', async () => {
    mockFind.mockResolvedValue([]);

    await sweeper.sweep();

    expect(mockFind).toHaveBeenCalledWith(1000, 50);
  });

  it('releases the lock, restores orders, and marks FAILED when not settled on-chain', async () => {
    const match = stuckMatch('m1');
    mockFind.mockResolvedValue([match]);
    mockCheck.mockResolvedValue(new Map([['m1', false]]));

    const result = await sweeper.sweep();

    expect(mockRemediate).toHaveBeenCalledTimes(1);
    expect(mockRemediate).toHaveBeenCalledWith(match);
    expect(result).toMatchObject({ scanned: 1, failedUnlocked: 1, ghostSettled: 0, skippedUnknown: 0 });
  });

  it('does not count a match that was already resolved (remediate returns false)', async () => {
    mockFind.mockResolvedValue([stuckMatch('m1')]);
    mockCheck.mockResolvedValue(new Map([['m1', false]]));
    mockRemediate.mockResolvedValue(false);

    const result = await sweeper.sweep();

    expect(mockRemediate).toHaveBeenCalledTimes(1);
    expect(result).toMatchObject({ scanned: 1, failedUnlocked: 0 });
  });

  it('quarantines (no mutation) when settled on-chain but DB record is lost', async () => {
    mockFind.mockResolvedValue([stuckMatch('m1')]);
    mockCheck.mockResolvedValue(new Map([['m1', true]]));

    const result = await sweeper.sweep();

    expect(mockRemediate).not.toHaveBeenCalled();
    expect(result).toMatchObject({ scanned: 1, failedUnlocked: 0, ghostSettled: 1, skippedUnknown: 0 });
  });

  it('skips a match whose on-chain status is unknown', async () => {
    mockFind.mockResolvedValue([stuckMatch('m1')]);
    mockCheck.mockResolvedValue(new Map()); // read failed -> omitted

    const result = await sweeper.sweep();

    expect(mockRemediate).not.toHaveBeenCalled();
    expect(result).toMatchObject({ scanned: 1, failedUnlocked: 0, ghostSettled: 0, skippedUnknown: 1 });
  });

  it('handles a mixed batch correctly', async () => {
    mockFind.mockResolvedValue([stuckMatch('a'), stuckMatch('b'), stuckMatch('c')]);
    mockCheck.mockResolvedValue(
      new Map([
        ['a', false], // -> remediate
        ['b', true], // -> ghost
        // 'c' omitted -> unknown
      ]),
    );

    const result = await sweeper.sweep();

    expect(mockRemediate).toHaveBeenCalledTimes(1);
    expect(mockRemediate).toHaveBeenCalledWith(expect.objectContaining({ id: 'a' }));
    expect(result).toEqual({ scanned: 3, failedUnlocked: 1, ghostSettled: 1, skippedUnknown: 1 });
  });

  it('continues processing other matches when one remediation throws', async () => {
    mockFind.mockResolvedValue([stuckMatch('a'), stuckMatch('b')]);
    mockCheck.mockResolvedValue(
      new Map([
        ['a', false],
        ['b', false],
      ]),
    );
    mockRemediate
      .mockRejectedValueOnce(new Error('db down'))
      .mockResolvedValueOnce(true);

    const result = await sweeper.sweep();

    expect(mockRemediate).toHaveBeenCalledTimes(2);
    // Only the successful one is counted; the failed one is left for next sweep.
    expect(result).toMatchObject({ scanned: 2, failedUnlocked: 1 });
  });
});
