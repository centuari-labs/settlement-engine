import type { Pool, PoolClient } from 'pg';
import type { PublicClient, TransactionReceipt } from 'viem';
import type {
  ParsedBorrowPosition,
  ParsedLendPosition,
  SettlementResult,
} from '../smartContract';

jest.mock('@centuari-labs/on-chain-effects', () => {
  return {
    applyOnChainEffect: jest.fn(async () => ({ applied: true })),
  };
});

import { applyOnChainEffect } from '@centuari-labs/on-chain-effects';
import { applySettlementResult } from '../database/apply-settlement';

const mockedApply = applyOnChainEffect as jest.MockedFunction<
  typeof applyOnChainEffect
>;

const TX_HASH =
  '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' as const;
const BLOCK_HASH =
  '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb' as const;

const fakeReceipt = (): TransactionReceipt =>
  ({
    transactionHash: TX_HASH,
    blockHash: BLOCK_HASH,
    blockNumber: 100n,
    status: 'success',
    logs: [],
  }) as unknown as TransactionReceipt;

const fakeClient = (): PublicClient => ({}) as unknown as PublicClient;

const fakePool = (): Pool => ({}) as unknown as Pool;

const lendEvent = (
  overrides: Partial<ParsedLendPosition> = {},
): ParsedLendPosition => ({
  marketId:
    '0x1111111111111111111111111111111111111111111111111111111111111111',
  lender: '0x2222222222222222222222222222222222222222',
  bondToken: '0x3333333333333333333333333333333333333333',
  cbtAmount: 1_000_000n,
  principal: 1_000_000n,
  rate: 500n,
  logIndex: 0,
  ...overrides,
});

const borrowEvent = (
  overrides: Partial<ParsedBorrowPosition> = {},
): ParsedBorrowPosition => ({
  marketId:
    '0x1111111111111111111111111111111111111111111111111111111111111111',
  borrower: '0x4444444444444444444444444444444444444444',
  principal: 1_000_000n,
  debt: 1_050_000n,
  rate: 500n,
  logIndex: 1,
  ...overrides,
});

const settlementResult = (
  overrides: Partial<SettlementResult> = {},
): SettlementResult => ({
  transactionHash: TX_HASH,
  blockHash: BLOCK_HASH,
  blockNumber: 100,
  gasUsed: 500_000,
  timestamp: Date.now(),
  settledMatchIds: ['match-1'],
  bondTokenEvents: [],
  lendPositionEvents: [lendEvent()],
  borrowPositionEvents: [borrowEvent()],
  receipt: fakeReceipt(),
  ...overrides,
});

describe('applySettlementResult', () => {
  beforeEach(() => {
    mockedApply.mockReset();
    mockedApply.mockResolvedValue({ applied: true });
  });

  it('calls applyOnChainEffect once per parsed event with correct txHash, receipt, and logIndex', async () => {
    const pool = fakePool();
    const client = fakeClient();

    await applySettlementResult(pool, client, settlementResult());

    expect(mockedApply).toHaveBeenCalledTimes(2);

    const lendCall = mockedApply.mock.calls[0]![0];
    expect(lendCall.txHash).toBe(TX_HASH);
    expect(lendCall.receipt).toBeDefined();
    expect(lendCall.logIndex).toBe(0);
    expect(lendCall.pool).toBe(pool);
    expect(lendCall.client).toBe(client);
    expect(lendCall.alreadyAppliedCheck).toBeDefined();
    expect(typeof lendCall.mutation).toBe('function');

    const borrowCall = mockedApply.mock.calls[1]![0];
    expect(borrowCall.logIndex).toBe(1);
    expect(borrowCall.txHash).toBe(TX_HASH);
  });

  it('processes multiple events sharing the same (marketId, lender) key — one helper call per logIndex', async () => {
    const pool = fakePool();
    const client = fakeClient();

    await applySettlementResult(
      pool,
      client,
      settlementResult({
        lendPositionEvents: [
          lendEvent({ logIndex: 2, principal: 1_000_000n }),
          lendEvent({ logIndex: 4, principal: 2_000_000n }),
          lendEvent({ logIndex: 6, principal: 3_000_000n }),
        ],
        borrowPositionEvents: [],
      }),
    );

    expect(mockedApply).toHaveBeenCalledTimes(3);
    const logIndexes = mockedApply.mock.calls.map((c) => c[0].logIndex);
    expect(logIndexes).toEqual([2, 4, 6]);
  });

  it('uses distinct predicates per event so each log matches its own (marketId, user)', async () => {
    const pool = fakePool();
    const client = fakeClient();

    const lenderA = '0xaaaa000000000000000000000000000000000000';
    const lenderB = '0xbbbb000000000000000000000000000000000000';

    await applySettlementResult(
      pool,
      client,
      settlementResult({
        lendPositionEvents: [
          lendEvent({ logIndex: 0, lender: lenderA }),
          lendEvent({ logIndex: 2, lender: lenderB }),
        ],
        borrowPositionEvents: [],
      }),
    );

    const predA = mockedApply.mock.calls[0]![0].expectedArgsPredicate;
    const predB = mockedApply.mock.calls[1]![0].expectedArgsPredicate;

    const shared = {
      marketId:
        '0x1111111111111111111111111111111111111111111111111111111111111111' as `0x${string}`,
      bondToken:
        '0x3333333333333333333333333333333333333333' as `0x${string}`,
      cbtAmount: 1n,
      principal: 1n,
      rate: 1n,
    };
    expect(predA({ ...shared, lender: lenderA } as never)).toBe(true);
    expect(predA({ ...shared, lender: lenderB } as never)).toBe(false);
    expect(predB({ ...shared, lender: lenderB } as never)).toBe(true);
    expect(predB({ ...shared, lender: lenderA } as never)).toBe(false);
  });

  it('is a no-op for a result with zero events', async () => {
    const pool = fakePool();
    const client = fakeClient();

    await applySettlementResult(
      pool,
      client,
      settlementResult({
        lendPositionEvents: [],
        borrowPositionEvents: [],
      }),
    );

    expect(mockedApply).not.toHaveBeenCalled();
  });

  it('continues processing remaining events when the helper reports non-fatal reasons', async () => {
    mockedApply.mockResolvedValueOnce({
      applied: false,
      reason: 'already_stamped',
    });
    mockedApply.mockResolvedValueOnce({
      applied: false,
      reason: 'args_mismatch',
    });
    mockedApply.mockResolvedValueOnce({ applied: true });

    const pool = fakePool();
    const client = fakeClient();

    await applySettlementResult(
      pool,
      client,
      settlementResult({
        lendPositionEvents: [
          lendEvent({ logIndex: 0 }),
          lendEvent({ logIndex: 1 }),
        ],
        borrowPositionEvents: [borrowEvent({ logIndex: 2 })],
      }),
    );

    expect(mockedApply).toHaveBeenCalledTimes(3);
  });

  it('propagates errors thrown by the helper (caller owns retry logic)', async () => {
    mockedApply.mockRejectedValueOnce(new Error('pg exploded'));

    const pool = fakePool();
    const client = fakeClient();

    await expect(
      applySettlementResult(pool, client, settlementResult()),
    ).rejects.toThrow('pg exploded');
  });

  it("exercises the mutation callback against a PoolClient that executes the INSERT with stamp columns", async () => {
    // When the helper invokes mutation(tx, decoded, stamp), the closure built
    // by apply-settlement.ts must run the correct INSERT/UPSERT including the
    // four applied_by_* stamp columns. Capture the mutation, invoke it with a
    // fake client, and assert on the SQL + params.
    const pool = fakePool();
    const client = fakeClient();

    await applySettlementResult(pool, client, settlementResult());

    const mutation = mockedApply.mock.calls[0]![0].mutation;
    const capturedQueries: { sql: string; params: readonly unknown[] }[] = [];
    const fakeTx = {
      query: jest.fn(async (sql: string, params: readonly unknown[]) => {
        capturedQueries.push({ sql, params });
        return { rows: [], rowCount: 0 };
      }),
    } as unknown as PoolClient;

    await mutation(
      fakeTx,
      {
        marketId:
          '0x1111111111111111111111111111111111111111111111111111111111111111',
        lender: '0x2222222222222222222222222222222222222222',
        bondToken: '0x3333333333333333333333333333333333333333',
        cbtAmount: 1n,
        principal: 1n,
        rate: 1n,
      } as never,
      {
        txHash: TX_HASH,
        blockHash: BLOCK_HASH,
        blockNumber: 100n,
        logIndex: 0,
      },
    );

    expect(capturedQueries).toHaveLength(1);
    expect(capturedQueries[0]!.sql).toMatch(/INSERT INTO lend_position/);
    expect(capturedQueries[0]!.sql).toMatch(/ON CONFLICT \(market_id, lender\)/);
    const params = capturedQueries[0]!.params;
    // stamp columns: tx_hash, log_index, block_hash, block_number
    expect(params[6]).toBeInstanceOf(Buffer);
    expect(params[7]).toBe(0);
    expect(params[8]).toBeInstanceOf(Buffer);
    expect(params[9]).toBe('100');
  });
});
