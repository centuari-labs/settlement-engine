import type { Pool, PoolClient } from 'pg';
import type { Match } from '../../schemas/match';
import {
  applyMatchSettlementWriteback,
  writebackSettledMatches,
} from '../database/lock-release';

jest.mock('../database/connection', () => {
  const actual = jest.requireActual('../database/connection');
  return {
    ...actual,
    withTransaction: jest.fn(
      async <T,>(fn: (client: PoolClient) => Promise<T>): Promise<T> => {
        const fakeTx = {
          query: jest.fn(async () => ({ rows: [], rowCount: 0 })),
        } as unknown as PoolClient;
        return fn(fakeTx);
      },
    ),
  };
});

import { withTransaction } from '../database/connection';

const mockedWithTransaction = withTransaction as jest.MockedFunction<
  typeof withTransaction
>;

const TX_HASH =
  '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' as const;

const buildMatch = (overrides: Partial<Match> = {}): Match => ({
  matchId: '11111111-1111-1111-1111-111111111111',
  marketId: '22222222-2222-2222-2222-222222222222',
  lendOrderId: '33333333-3333-3333-3333-333333333333',
  borrowOrderId: '44444444-4444-4444-4444-444444444444',
  lenderWallet: '0xaaaa000000000000000000000000000000000001',
  borrowerWallet: '0xbbbb000000000000000000000000000000000002',
  matchedAmount: '1000000',
  rate: 500,
  loanToken: '0xcccc000000000000000000000000000000000003',
  maturity: 1_800_000_000,
  timestamp: 1_700_000_000_000,
  borrowerIsTaker: true,
  makerFeeAmount: '300',
  takerFeeAmount: '500',
  lenderSettlementFeeAmount: '100',
  borrowerSettlementFeeAmount: '100',
  ...overrides,
});

const buildSettledRow = (
  overrides: { lender?: string; borrower?: string; asset?: string } = {},
): { rows: Record<string, string>[]; rowCount: number } => ({
  rows: [
    {
      lender_account_id:
        overrides.lender ?? 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',
      borrower_account_id:
        overrides.borrower ?? 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb',
      asset_id: overrides.asset ?? 'cccccccc-cccc-cccc-cccc-cccccccccccc',
    },
  ],
  rowCount: 1,
});

interface CapturedQuery {
  sql: string;
  params: readonly unknown[];
}

const fakeTx = (
  responses: { rows: unknown[]; rowCount: number }[] = [],
): { tx: PoolClient; calls: CapturedQuery[] } => {
  const calls: CapturedQuery[] = [];
  let i = 0;
  const query = jest.fn(async (sql: string, params: readonly unknown[]) => {
    calls.push({ sql, params });
    const out = responses[i] ?? { rows: [], rowCount: 0 };
    i += 1;
    return out;
  });
  return { tx: { query } as unknown as PoolClient, calls };
};

describe('applyMatchSettlementWriteback', () => {
  describe('happy path — PENDING → SETTLED transition', () => {
    it('flips matches.settlement_status to SETTLED and decrements both portfolios', async () => {
      const { tx, calls } = fakeTx([
        buildSettledRow(),
        { rows: [], rowCount: 1 },
        { rows: [], rowCount: 1 },
      ]);

      const transitioned = await applyMatchSettlementWriteback(
        tx,
        buildMatch(),
        TX_HASH,
      );

      expect(transitioned).toBe(true);
      expect(calls).toHaveLength(3);

      // First call: UPDATE matches with idempotency guard.
      expect(calls[0]!.sql).toMatch(/UPDATE matches/);
      expect(calls[0]!.sql).toMatch(/settlement_status = 'SETTLED'/);
      expect(calls[0]!.sql).toMatch(/AND settlement_status = 'PENDING'/);
      expect(calls[0]!.params).toEqual([
        '11111111-1111-1111-1111-111111111111',
        TX_HASH,
      ]);

      // Both subsequent calls are portfolio decrements.
      expect(calls[1]!.sql).toMatch(/UPDATE portfolio/);
      expect(calls[1]!.sql).toMatch(/GREATEST/);
      expect(calls[2]!.sql).toMatch(/UPDATE portfolio/);
      expect(calls[2]!.sql).toMatch(/GREATEST/);
    });

    it('decrements lender by matched_amount + lender_settlement_fee + lender_trade_fee (maker when borrowerIsTaker)', async () => {
      const { tx, calls } = fakeTx([
        buildSettledRow({
          lender: 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',
        }),
        { rows: [], rowCount: 1 },
        { rows: [], rowCount: 1 },
      ]);

      await applyMatchSettlementWriteback(
        tx,
        buildMatch({
          matchedAmount: '1000000',
          lenderSettlementFeeAmount: '100',
          makerFeeAmount: '300',
          takerFeeAmount: '500',
          borrowerIsTaker: true,
        }),
        TX_HASH,
      );

      // lender_account_id < borrower_account_id ('a...' < 'b...') so lender
      // update fires first.
      const lenderCall = calls[1]!;
      expect(lenderCall.params).toEqual([
        '1000000',
        '100',
        '300',
        'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',
        'cccccccc-cccc-cccc-cccc-cccccccccccc',
      ]);
    });

    it('decrements borrower by borrower_settlement_fee + borrower_trade_fee (taker when borrowerIsTaker)', async () => {
      const { tx, calls } = fakeTx([
        buildSettledRow({
          borrower: 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb',
        }),
        { rows: [], rowCount: 1 },
        { rows: [], rowCount: 1 },
      ]);

      await applyMatchSettlementWriteback(
        tx,
        buildMatch({
          borrowerSettlementFeeAmount: '100',
          makerFeeAmount: '300',
          takerFeeAmount: '500',
          borrowerIsTaker: true,
        }),
        TX_HASH,
      );

      const borrowerCall = calls[2]!;
      expect(borrowerCall.params).toEqual([
        '100',
        '500',
        'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb',
        'cccccccc-cccc-cccc-cccc-cccccccccccc',
      ]);
    });

    it('flips fee assignment when borrowerIsTaker is false (lender takes, pays takerFee)', async () => {
      const { tx, calls } = fakeTx([
        buildSettledRow(),
        { rows: [], rowCount: 1 },
        { rows: [], rowCount: 1 },
      ]);

      await applyMatchSettlementWriteback(
        tx,
        buildMatch({
          matchedAmount: '1000000',
          lenderSettlementFeeAmount: '100',
          borrowerSettlementFeeAmount: '100',
          makerFeeAmount: '300',
          takerFeeAmount: '500',
          borrowerIsTaker: false,
        }),
        TX_HASH,
      );

      // Lender (first because lender < borrower) gets takerFee.
      expect(calls[1]!.params).toEqual([
        '1000000',
        '100',
        '500',
        'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',
        'cccccccc-cccc-cccc-cccc-cccccccccccc',
      ]);
      // Borrower (second) gets makerFee.
      expect(calls[2]!.params).toEqual([
        '100',
        '300',
        'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb',
        'cccccccc-cccc-cccc-cccc-cccccccccccc',
      ]);
    });

    it('orders portfolio updates by account_id ascending to match db-writer deadlock-avoidance', async () => {
      // Inverted: lender id > borrower id. Borrower update should fire first.
      const { tx, calls } = fakeTx([
        buildSettledRow({
          lender: 'ffffffff-ffff-ffff-ffff-ffffffffffff',
          borrower: 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',
        }),
        { rows: [], rowCount: 1 },
        { rows: [], rowCount: 1 },
      ]);

      await applyMatchSettlementWriteback(tx, buildMatch(), TX_HASH);

      // calls[1] = borrower (smaller id), 4 params: feeAmount + tradeFee + accountId + assetId.
      expect(calls[1]!.params).toHaveLength(4);
      expect(calls[1]!.params[2]).toBe('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa');
      // calls[2] = lender, 5 params: matched + feeAmount + tradeFee + accountId + assetId.
      expect(calls[2]!.params).toHaveLength(5);
      expect(calls[2]!.params[3]).toBe('ffffffff-ffff-ffff-ffff-ffffffffffff');
    });
  });

  describe('idempotency — already SETTLED', () => {
    it('returns false and skips portfolio updates when matches WHERE clause matches no rows', async () => {
      const { tx, calls } = fakeTx([{ rows: [], rowCount: 0 }]);

      const transitioned = await applyMatchSettlementWriteback(
        tx,
        buildMatch(),
        TX_HASH,
      );

      expect(transitioned).toBe(false);
      // Only the matches UPDATE was attempted; portfolio updates skipped.
      expect(calls).toHaveLength(1);
      expect(calls[0]!.sql).toMatch(/UPDATE matches/);
    });
  });

  describe('GREATEST guard against negative locked_amount', () => {
    it('uses GREATEST(..., 0) on both portfolio updates', async () => {
      const { tx, calls } = fakeTx([
        buildSettledRow(),
        { rows: [], rowCount: 1 },
        { rows: [], rowCount: 1 },
      ]);

      await applyMatchSettlementWriteback(tx, buildMatch(), TX_HASH);

      expect(calls[1]!.sql).toMatch(/GREATEST\(\s*\n?\s*locked_amount\s*-\s*\(/);
      expect(calls[1]!.sql).toMatch(/,\s*\n?\s*0\)/);
      expect(calls[2]!.sql).toMatch(/GREATEST\(\s*\n?\s*locked_amount\s*-\s*\(/);
      expect(calls[2]!.sql).toMatch(/,\s*\n?\s*0\)/);
    });
  });
});

describe('writebackSettledMatches', () => {
  beforeEach(() => {
    mockedWithTransaction.mockClear();
  });

  it('returns zero counts when matches array is empty', async () => {
    const result = await writebackSettledMatches(
      {} as Pool,
      [],
      new Set<string>(),
      TX_HASH,
    );

    expect(result).toEqual({ settled: 0, alreadySettled: 0 });
    expect(mockedWithTransaction).not.toHaveBeenCalled();
  });

  it('skips matches not in settledMatchIds (filters to settled subset)', async () => {
    const m1 = buildMatch({ matchId: '11111111-1111-1111-1111-111111111111' });
    const m2 = buildMatch({ matchId: '22222222-2222-2222-2222-222222222222' });
    const settledIds = new Set([m1.matchId]);

    mockedWithTransaction.mockImplementationOnce(
      async <T,>(fn: (client: PoolClient) => Promise<T>): Promise<T> => {
        let i = 0;
        const responses = [
          buildSettledRow(),
          { rows: [], rowCount: 1 },
          { rows: [], rowCount: 1 },
        ];
        const tx = {
          query: jest.fn(async () => {
            const out = responses[i] ?? { rows: [], rowCount: 0 };
            i += 1;
            return out;
          }),
        } as unknown as PoolClient;
        return fn(tx);
      },
    );

    const result = await writebackSettledMatches(
      {} as Pool,
      [m1, m2],
      settledIds,
      TX_HASH,
    );

    expect(result.settled).toBe(1);
    expect(result.alreadySettled).toBe(0);
    expect(mockedWithTransaction).toHaveBeenCalledTimes(1);
  });

  it('counts both settled and alreadySettled outcomes across a batch', async () => {
    const m1 = buildMatch({ matchId: '11111111-1111-1111-1111-111111111111' });
    const m2 = buildMatch({ matchId: '22222222-2222-2222-2222-222222222222' });
    const settledIds = new Set([m1.matchId, m2.matchId]);

    let callIdx = 0;
    mockedWithTransaction.mockImplementation(
      async <T,>(fn: (client: PoolClient) => Promise<T>): Promise<T> => {
        const idx = callIdx++;
        let qIdx = 0;
        const responses =
          idx === 0
            ? [
                buildSettledRow(),
                { rows: [], rowCount: 1 },
                { rows: [], rowCount: 1 },
              ]
            : [{ rows: [], rowCount: 0 }];
        const tx = {
          query: jest.fn(async () => {
            const out = responses[qIdx] ?? { rows: [], rowCount: 0 };
            qIdx += 1;
            return out;
          }),
        } as unknown as PoolClient;
        return fn(tx);
      },
    );

    const result = await writebackSettledMatches(
      {} as Pool,
      [m1, m2],
      settledIds,
      TX_HASH,
    );

    expect(result.settled).toBe(1);
    expect(result.alreadySettled).toBe(1);
    expect(mockedWithTransaction).toHaveBeenCalledTimes(2);
  });

  it('keeps processing remaining matches when one writeback throws', async () => {
    const m1 = buildMatch({ matchId: '11111111-1111-1111-1111-111111111111' });
    const m2 = buildMatch({ matchId: '22222222-2222-2222-2222-222222222222' });
    const settledIds = new Set([m1.matchId, m2.matchId]);

    let callIdx = 0;
    mockedWithTransaction.mockImplementation(
      async <T,>(fn: (client: PoolClient) => Promise<T>): Promise<T> => {
        const idx = callIdx++;
        if (idx === 0) {
          throw new Error('pg exploded');
        }
        let qIdx = 0;
        const responses = [
          buildSettledRow(),
          { rows: [], rowCount: 1 },
          { rows: [], rowCount: 1 },
        ];
        const tx = {
          query: jest.fn(async () => {
            const out = responses[qIdx] ?? { rows: [], rowCount: 0 };
            qIdx += 1;
            return out;
          }),
        } as unknown as PoolClient;
        return fn(tx);
      },
    );

    const result = await writebackSettledMatches(
      {} as Pool,
      [m1, m2],
      settledIds,
      TX_HASH,
    );

    expect(result.settled).toBe(1);
    expect(result.alreadySettled).toBe(0);
    expect(mockedWithTransaction).toHaveBeenCalledTimes(2);
  });
});
