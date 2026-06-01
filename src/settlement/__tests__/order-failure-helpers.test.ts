import type { PoolClient } from 'pg';

// Mock the connection pool: order-failure's batch helpers use
// getPool().connect() with explicit BEGIN/COMMIT (not withTransaction). Assert
// the exact SQL + params without a live database.
interface CapturedQuery {
  sql: string;
  params?: readonly unknown[];
}

let calls: CapturedQuery[] = [];
let failOnSql: RegExp | null = null;
const release = jest.fn();

const client = {
  query: jest.fn(async (sql: string, params?: readonly unknown[]) => {
    calls.push({ sql, params });
    if (failOnSql && failOnSql.test(sql)) {
      throw new Error('boom');
    }
    return { rows: [], rowCount: 1 };
  }),
  release,
} as unknown as PoolClient;

jest.mock('../database/connection', () => ({
  getPool: () => ({ connect: async () => client }),
}));

import {
  unlockFailedMatches,
  recordFailedMatches,
  restoreOrdersForFailedMatches,
} from '../database/order-failure';
import { createMatch } from '../../tests/helpers/testFixtures';

const LENDER = '0xaaaa000000000000000000000000000000000001';
const BORROWER = '0xbbbb000000000000000000000000000000000002';
const LOAN = '0xcccc000000000000000000000000000000000003';

const match = (overrides = {}) =>
  createMatch({
    matchId: '11111111-1111-1111-1111-111111111111',
    lendOrderId: '33333333-3333-3333-3333-333333333333',
    borrowOrderId: '44444444-4444-4444-4444-444444444444',
    lenderWallet: LENDER,
    borrowerWallet: BORROWER,
    loanToken: LOAN,
    matchedAmount: '1000000',
    lenderSettlementFeeAmount: '100',
    borrowerSettlementFeeAmount: '100',
    makerFeeAmount: '300',
    takerFeeAmount: '500',
    borrowerIsTaker: true,
    ...overrides,
  });

beforeEach(() => {
  calls = [];
  failOnSql = null;
  jest.clearAllMocks();
});

describe('unlockFailedMatches', () => {
  it('releases both sides in_orders inside BEGIN/COMMIT, lender first by wallet order', async () => {
    await unlockFailedMatches([match()]);

    expect(calls[0].sql).toBe('BEGIN');
    expect(calls[calls.length - 1].sql).toBe('COMMIT');
    const updates = calls.filter((c) => /UPDATE user_balance/.test(c.sql));
    expect(updates).toHaveLength(2);
    // lender ('0xaaaa…') < borrower ('0xbbbb…') → lender first. borrowerIsTaker
    // = true → lenderTradeFee = makerFee (300).
    expect(updates[0].params).toEqual(['1000000', '100', '300', LENDER, LOAN]);
    expect(updates[1].params).toEqual(['100', '500', BORROWER, LOAN]);
    expect(release).toHaveBeenCalled();
  });

  it('flips the trade-fee split when borrowerIsTaker is false', async () => {
    await unlockFailedMatches([match({ borrowerIsTaker: false })]);
    const updates = calls.filter((c) => /UPDATE user_balance/.test(c.sql));
    // lender pays takerFee (500); borrower pays makerFee (300).
    expect(updates[0].params).toEqual(['1000000', '100', '500', LENDER, LOAN]);
    expect(updates[1].params).toEqual(['100', '300', BORROWER, LOAN]);
  });

  it('orders updates by ascending wallet (deadlock avoidance)', async () => {
    const highLender = '0xffff000000000000000000000000000000000009';
    const lowBorrower = '0x1111000000000000000000000000000000000000';
    await unlockFailedMatches([
      match({ lenderWallet: highLender, borrowerWallet: lowBorrower }),
    ]);
    const updates = calls.filter((c) => /UPDATE user_balance/.test(c.sql));
    // borrower (smaller) first: 4 params.
    expect(updates[0].params).toHaveLength(4);
    expect(updates[0].params?.[2]).toBe(lowBorrower);
    expect(updates[1].params?.[3]).toBe(highLender);
  });

  it('rolls back on a query failure', async () => {
    failOnSql = /UPDATE user_balance/;
    await unlockFailedMatches([match()]);
    expect(calls.some((c) => c.sql === 'ROLLBACK')).toBe(true);
    expect(calls.some((c) => c.sql === 'COMMIT')).toBe(false);
    expect(release).toHaveBeenCalled();
  });
});

describe('recordFailedMatches', () => {
  it('marks each match FAILED with the reason', async () => {
    await recordFailedMatches([match()], 'SOME_REASON');
    const update = calls.find((c) => /UPDATE matches/.test(c.sql));
    expect(update?.sql).toMatch(/settlement_status = 'FAILED'/);
    expect(update?.params).toEqual(['11111111-1111-1111-1111-111111111111', 'SOME_REASON']);
  });

  it('rolls back on failure', async () => {
    failOnSql = /UPDATE matches/;
    await recordFailedMatches([match()], 'SOME_REASON');
    expect(calls.some((c) => c.sql === 'ROLLBACK')).toBe(true);
  });
});

describe('restoreOrdersForFailedMatches', () => {
  it('restores both lend and borrow orders', async () => {
    await restoreOrdersForFailedMatches([match()]);
    const updates = calls.filter((c) => /UPDATE orders/.test(c.sql));
    expect(updates).toHaveLength(2);
    expect(updates[0].params).toEqual(['33333333-3333-3333-3333-333333333333', '1000000', '100']);
    expect(updates[1].params).toEqual(['44444444-4444-4444-4444-444444444444', '1000000', '100']);
  });

  it('rolls back on failure', async () => {
    failOnSql = /UPDATE orders/;
    await restoreOrdersForFailedMatches([match()]);
    expect(calls.some((c) => c.sql === 'ROLLBACK')).toBe(true);
  });
});
