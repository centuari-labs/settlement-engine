import type { PoolClient } from 'pg';

// Mock the connection module: quarantineFailedMatch runs in one withTransaction.
// Same approach as pending-settlement-sweep.test.ts — assert the exact SQL +
// params without a live database.
const mockQuery = jest.fn();
const mockWithTransaction = jest.fn();

jest.mock('../database/connection', () => ({
  getPool: () => ({ query: mockQuery }),
  withTransaction: (fn: (client: PoolClient) => unknown) => mockWithTransaction(fn),
}));

import {
  quarantineFailedMatch,
  POISON_FAILURE_REASON,
} from '../database/order-failure';
import { createMatch } from '../../tests/helpers/testFixtures';
import type { Match } from '../../schemas/match';

interface CapturedQuery {
  sql: string;
  params: readonly unknown[];
}

/** A fake transaction client that records calls and returns scripted rowCounts. */
const fakeTx = (
  rowCounts: number[] = [],
): { tx: PoolClient; calls: CapturedQuery[] } => {
  const calls: CapturedQuery[] = [];
  let i = 0;
  const query = jest.fn(async (sql: string, params: readonly unknown[]) => {
    calls.push({ sql, params });
    const rowCount = rowCounts[i] ?? 1;
    i += 1;
    return { rows: [], rowCount };
  });
  return { tx: { query } as unknown as PoolClient, calls };
};

const LENDER_WALLET = '0xaaaa000000000000000000000000000000000001';
const BORROWER_WALLET = '0xbbbb000000000000000000000000000000000002';
const LOAN_TOKEN = '0xcccc000000000000000000000000000000000003';

const buildMatch = (overrides: Partial<Match> = {}): Match =>
  createMatch({
    matchId: '11111111-1111-1111-1111-111111111111',
    lendOrderId: '33333333-3333-3333-3333-333333333333',
    borrowOrderId: '44444444-4444-4444-4444-444444444444',
    lenderWallet: LENDER_WALLET,
    borrowerWallet: BORROWER_WALLET,
    loanToken: LOAN_TOKEN,
    matchedAmount: '1000000',
    lenderSettlementFeeAmount: '100',
    borrowerSettlementFeeAmount: '100',
    makerFeeAmount: '300',
    takerFeeAmount: '500',
    borrowerIsTaker: true,
    ...overrides,
  });

beforeEach(() => {
  jest.clearAllMocks();
  // Default: withTransaction invokes fn with a fresh successful fakeTx.
  mockWithTransaction.mockImplementation((fn: (c: PoolClient) => unknown) =>
    fn(fakeTx().tx),
  );
});

describe('quarantineFailedMatch', () => {
  it('flips PENDING -> FAILED with the poison reason as the idempotency gate', async () => {
    const { tx, calls } = fakeTx([1, 1, 1, 1, 1]);
    mockWithTransaction.mockImplementationOnce((fn: (c: PoolClient) => unknown) => fn(tx));

    const reason = `${POISON_FAILURE_REASON}:INSUFFICIENT_FUNDS`;
    const actioned = await quarantineFailedMatch(buildMatch(), reason);

    expect(actioned).toBe(true);
    expect(calls[0].sql).toMatch(/UPDATE matches/);
    expect(calls[0].sql).toMatch(/settlement_status = 'FAILED'/);
    expect(calls[0].sql).toMatch(/AND settlement_status = 'PENDING'/);
    expect(calls[0].params).toEqual([
      '11111111-1111-1111-1111-111111111111',
      reason,
    ]);
  });

  it('returns false and does nothing else when the match is no longer PENDING', async () => {
    const { tx, calls } = fakeTx([0]); // flip affects 0 rows (already resolved)
    mockWithTransaction.mockImplementationOnce((fn: (c: PoolClient) => unknown) => fn(tx));

    const actioned = await quarantineFailedMatch(buildMatch(), POISON_FAILURE_REASON);

    expect(actioned).toBe(false);
    expect(calls).toHaveLength(1); // only the guarded flip ran
  });

  it('restores both orders then releases both sides of in_orders with the right params', async () => {
    const { tx, calls } = fakeTx([1, 1, 1, 1, 1]);
    mockWithTransaction.mockImplementationOnce((fn: (c: PoolClient) => unknown) => fn(tx));

    await quarantineFailedMatch(buildMatch(), POISON_FAILURE_REASON);

    expect(calls).toHaveLength(5);

    // 2: restore lend order
    expect(calls[1].sql).toMatch(/UPDATE orders/);
    expect(calls[1].sql).toMatch(/SETTLEMENT_FAILED/);
    expect(calls[1].params).toEqual(['33333333-3333-3333-3333-333333333333', '1000000', '100']);

    // 3: restore borrow order
    expect(calls[2].sql).toMatch(/UPDATE orders/);
    expect(calls[2].params).toEqual(['44444444-4444-4444-4444-444444444444', '1000000', '100']);

    // 4 + 5: in_orders releases. lenderWallet ('0xaaaa…') < borrowerWallet ('0xbbbb…'),
    // so the lender unlock fires first (deadlock-safe ascending wallet order).
    expect(calls[3].sql).toMatch(/UPDATE user_balance/);
    expect(calls[3].sql).toMatch(/GREATEST\(0, in_orders/);
    // borrowerIsTaker = true -> lenderTradeFee = makerFee (300).
    expect(calls[3].params).toEqual(['1000000', '100', '300', LENDER_WALLET, LOAN_TOKEN]);

    expect(calls[4].sql).toMatch(/UPDATE user_balance/);
    // borrower pays takerFee (500) when borrowerIsTaker.
    expect(calls[4].params).toEqual(['100', '500', BORROWER_WALLET, LOAN_TOKEN]);
  });

  it('flips the trade-fee assignment when borrowerIsTaker is false', async () => {
    const { tx, calls } = fakeTx([1, 1, 1, 1, 1]);
    mockWithTransaction.mockImplementationOnce((fn: (c: PoolClient) => unknown) => fn(tx));

    await quarantineFailedMatch(buildMatch({ borrowerIsTaker: false }), POISON_FAILURE_REASON);

    // Lender now pays takerFee (500); borrower pays makerFee (300).
    expect(calls[3].params).toEqual(['1000000', '100', '500', LENDER_WALLET, LOAN_TOKEN]);
    expect(calls[4].params).toEqual(['100', '300', BORROWER_WALLET, LOAN_TOKEN]);
  });

  it('orders the in_orders updates by ascending wallet (deadlock avoidance)', async () => {
    const highLender = '0xffff000000000000000000000000000000000009';
    const lowBorrower = '0x1111000000000000000000000000000000000000';
    const { tx, calls } = fakeTx([1, 1, 1, 1, 1]);
    mockWithTransaction.mockImplementationOnce((fn: (c: PoolClient) => unknown) => fn(tx));

    await quarantineFailedMatch(
      buildMatch({ lenderWallet: highLender, borrowerWallet: lowBorrower }),
      POISON_FAILURE_REASON,
    );

    // Borrower (smaller wallet) unlock fires first: 4 params (fee, tradeFee, wallet, token).
    expect(calls[3].params).toHaveLength(4);
    expect(calls[3].params[2]).toBe(lowBorrower);
    // Lender unlock second: 5 params (matched, fee, tradeFee, wallet, token).
    expect(calls[4].params).toHaveLength(5);
    expect(calls[4].params[3]).toBe(highLender);
  });
});
