import type { Pool, PoolClient } from 'pg';
import { encodeAbiParameters, keccak256, toHex } from 'viem';
import type { Hex, PublicClient, TransactionReceipt } from 'viem';
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

jest.mock('../database/pending-collateral-flags', () => ({
  clearForEvent: jest.fn(async () => {}),
}));

import { applyOnChainEffect } from '@centuari-labs/on-chain-effects';
import { applySettlementResult } from '../database/apply-settlement';
import { withTransaction } from '../database/connection';
import { clearForEvent } from '../database/pending-collateral-flags';

const mockedWithTransaction = withTransaction as jest.MockedFunction<
  typeof withTransaction
>;
const mockedClearForEvent = clearForEvent as jest.MockedFunction<
  typeof clearForEvent
>;

const COLLATERAL_FLAG_SET_TOPIC0 = keccak256(
  toHex('CollateralFlagSet(address,address,address,bool,uint64)'),
);

const padAddressTopic = (addr: string): Hex =>
  `0x${addr.replace(/^0x/, '').padStart(64, '0').toLowerCase()}` as Hex;

const buildCollateralFlagLog = (params: {
  writer?: string;
  user: string;
  asset: string;
  used?: boolean;
  flaggedAt?: bigint;
  logIndex: number;
}) => {
  const writer = params.writer ?? '0x0000000000000000000000000000000000001234';
  const used = params.used ?? true;
  const flaggedAt = params.flaggedAt ?? 1_700_000_000n;
  const data = encodeAbiParameters(
    [
      { name: 'used', type: 'bool' },
      { name: 'flaggedAt', type: 'uint64' },
    ],
    [used, flaggedAt],
  );
  return {
    address: '0x000000000000000000000000000000000000beef' as Hex,
    topics: [
      COLLATERAL_FLAG_SET_TOPIC0,
      padAddressTopic(writer),
      padAddressTopic(params.user),
      padAddressTopic(params.asset),
    ] as readonly Hex[],
    data,
    logIndex: params.logIndex,
  };
};

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
    mockedWithTransaction.mockClear();
    mockedClearForEvent.mockClear();
  });

  it('calls applyOnChainEffect once per parsed event with correct txHash, receipt, and logIndex', async () => {
    const pool = fakePool();
    const client = fakeClient();

    await applySettlementResult(pool, client, settlementResult(), []);

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
      [],
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
      [],
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
      [],
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
      [],
    );

    expect(mockedApply).toHaveBeenCalledTimes(3);
  });

  it('propagates errors thrown by the helper (caller owns retry logic)', async () => {
    mockedApply.mockRejectedValueOnce(new Error('pg exploded'));

    const pool = fakePool();
    const client = fakeClient();

    await expect(
      applySettlementResult(pool, client, settlementResult(), []),
    ).rejects.toThrow('pg exploded');
  });

  it("exercises the mutation callback against a PoolClient that executes the INSERT with stamp columns", async () => {
    // When the helper invokes mutation(tx, decoded, stamp), the closure built
    // by apply-settlement.ts must run the correct INSERT/UPSERT including the
    // four applied_by_* stamp columns. Capture the mutation, invoke it with a
    // fake client, and assert on the SQL + params.
    const pool = fakePool();
    const client = fakeClient();

    await applySettlementResult(pool, client, settlementResult(), []);

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

  describe('CollateralFlagSet eager queue cleanup (Phase 3)', () => {
    it('does not invoke withTransaction when the receipt has no CollateralFlagSet logs', async () => {
      await applySettlementResult(fakePool(), fakeClient(), settlementResult(), []);
      expect(mockedWithTransaction).not.toHaveBeenCalled();
      expect(mockedClearForEvent).not.toHaveBeenCalled();
    });

    it('DELETEs one queue row per CollateralFlagSet event in the receipt', async () => {
      const userA = '0xaaaa000000000000000000000000000000000001';
      const userB = '0xbbbb000000000000000000000000000000000002';
      const assetX = '0xcccc000000000000000000000000000000000003';
      const assetY = '0xdddd000000000000000000000000000000000004';

      const receiptWithFlagLogs = {
        ...fakeReceipt(),
        logs: [
          buildCollateralFlagLog({ user: userA, asset: assetX, logIndex: 5 }),
          buildCollateralFlagLog({ user: userB, asset: assetY, logIndex: 7 }),
        ],
      } as unknown as TransactionReceipt;

      await applySettlementResult(
        fakePool(),
        fakeClient(),
        settlementResult({
          lendPositionEvents: [],
          borrowPositionEvents: [],
          receipt: receiptWithFlagLogs,
        }),
        [],
      );

      // Single transaction wrapping both DELETEs.
      expect(mockedWithTransaction).toHaveBeenCalledTimes(1);
      expect(mockedClearForEvent).toHaveBeenCalledTimes(2);

      // Decoded args reach clearForEvent in checksum-cased form (viem
      // normalizes); compare lowercased.
      const calls = mockedClearForEvent.mock.calls.map(
        ([, user, asset]) => ({
          user: (user as string).toLowerCase(),
          asset: (asset as string).toLowerCase(),
        }),
      );
      expect(calls).toEqual([
        { user: userA, asset: assetX },
        { user: userB, asset: assetY },
      ]);
    });

    it('also DELETEs on used=false events (defensive — Centuari.settleMatch only emits used=true today, but unflag mid-batch must not leak queue rows)', async () => {
      const user = '0xaaaa000000000000000000000000000000000001';
      const asset = '0xcccc000000000000000000000000000000000003';

      const receipt = {
        ...fakeReceipt(),
        logs: [
          buildCollateralFlagLog({ user, asset, used: false, logIndex: 3 }),
        ],
      } as unknown as TransactionReceipt;

      await applySettlementResult(
        fakePool(),
        fakeClient(),
        settlementResult({
          lendPositionEvents: [],
          borrowPositionEvents: [],
          receipt,
        }),
        [],
      );

      expect(mockedClearForEvent).toHaveBeenCalledTimes(1);
    });

    it('skips logs with mismatched topic0 (no impact on non-CollateralFlagSet logs in the same receipt)', async () => {
      const otherTopic =
        '0x1234567890123456789012345678901234567890123456789012345678901234' as Hex;
      const receipt = {
        ...fakeReceipt(),
        logs: [
          {
            address: '0x000000000000000000000000000000000000beef' as Hex,
            topics: [otherTopic] as readonly Hex[],
            data: '0x' as Hex,
            logIndex: 0,
          },
        ],
      } as unknown as TransactionReceipt;

      await applySettlementResult(
        fakePool(),
        fakeClient(),
        settlementResult({
          lendPositionEvents: [],
          borrowPositionEvents: [],
          receipt,
        }),
        [],
      );

      expect(mockedWithTransaction).not.toHaveBeenCalled();
      expect(mockedClearForEvent).not.toHaveBeenCalled();
    });

    it('skips a single malformed log without aborting the others (defensive parse)', async () => {
      const userOk = '0xaaaa000000000000000000000000000000000001';
      const assetOk = '0xcccc000000000000000000000000000000000003';

      const malformed = {
        // Right topic0 but missing the indexed user/asset topics → decode
        // throws.
        address: '0x000000000000000000000000000000000000beef' as Hex,
        topics: [COLLATERAL_FLAG_SET_TOPIC0] as readonly Hex[],
        data: '0x' as Hex,
        logIndex: 0,
      };

      const receipt = {
        ...fakeReceipt(),
        logs: [
          malformed,
          buildCollateralFlagLog({
            user: userOk,
            asset: assetOk,
            logIndex: 1,
          }),
        ],
      } as unknown as TransactionReceipt;

      await applySettlementResult(
        fakePool(),
        fakeClient(),
        settlementResult({
          lendPositionEvents: [],
          borrowPositionEvents: [],
          receipt,
        }),
        [],
      );

      // The good log still gets cleared; the malformed one is logged and
      // skipped.
      expect(mockedClearForEvent).toHaveBeenCalledTimes(1);
    });
  });
});
