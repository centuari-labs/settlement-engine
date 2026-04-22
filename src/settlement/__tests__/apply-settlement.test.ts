import type { Pool, PoolClient, QueryResult } from 'pg';
import type {
  ParsedBorrowPosition,
  ParsedLendPosition,
  SettlementResult,
} from '../smartContract';
import { applySettlementResult } from '../database/apply-settlement';

type QueryFn = jest.Mock<Promise<QueryResult<{ count: string }>>, [string, unknown[]?]>;

const mockClient = (): { client: PoolClient; query: QueryFn; release: jest.Mock } => {
  const query: QueryFn = jest.fn(async (sql: string) => {
    // Default: alreadyStamped returns no rows (i.e. not already stamped),
    // INSERT/UPDATE succeed, BEGIN/COMMIT/ROLLBACK are no-ops.
    if (/^SELECT count\(\*\)/i.test(sql.trim())) {
      return {
        rows: [{ count: '0' }],
        rowCount: 1,
        command: 'SELECT',
        oid: 0,
        fields: [],
      } as unknown as QueryResult;
    }
    return {
      rows: [],
      rowCount: 0,
      command: 'INSERT',
      oid: 0,
      fields: [],
    } as unknown as QueryResult;
  });
  const release = jest.fn();
  return {
    client: { query, release } as unknown as PoolClient,
    query,
    release,
  };
};

const mockPool = (connectImpl: () => PoolClient): Pool =>
  ({ connect: jest.fn(async () => connectImpl()) } as unknown as Pool);

const lendEvent = (overrides: Partial<ParsedLendPosition> = {}): ParsedLendPosition => ({
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

const borrowEvent = (overrides: Partial<ParsedBorrowPosition> = {}): ParsedBorrowPosition => ({
  marketId:
    '0x1111111111111111111111111111111111111111111111111111111111111111',
  borrower: '0x4444444444444444444444444444444444444444',
  principal: 1_000_000n,
  debt: 1_050_000n,
  rate: 500n,
  logIndex: 1,
  ...overrides,
});

const settlementResult = (overrides: Partial<SettlementResult> = {}): SettlementResult => ({
  transactionHash:
    '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
  blockHash:
    '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
  blockNumber: 100,
  gasUsed: 500_000,
  timestamp: Date.now(),
  settledMatchIds: ['match-1'],
  bondTokenEvents: [],
  lendPositionEvents: [lendEvent()],
  borrowPositionEvents: [borrowEvent()],
  ...overrides,
});

describe('applySettlementResult', () => {
  it('applies a lend + borrow event pair with stamp columns on both inserts', async () => {
    const lendClient = mockClient();
    const borrowClient = mockClient();
    const connects: PoolClient[] = [lendClient.client, borrowClient.client];
    const pool = mockPool(() => {
      const next = connects.shift();
      if (!next) throw new Error('unexpected extra connect');
      return next;
    });

    await applySettlementResult(pool, settlementResult());

    // Lend: BEGIN, alreadyStamped SELECT, INSERT, COMMIT
    expect(lendClient.query.mock.calls.map((c) => c[0].split(/\s+/u)[0])).toEqual([
      'BEGIN',
      'SELECT',
      'INSERT',
      'COMMIT',
    ]);
    // Borrow: same shape
    expect(borrowClient.query.mock.calls.map((c) => c[0].split(/\s+/u)[0])).toEqual([
      'BEGIN',
      'SELECT',
      'INSERT',
      'COMMIT',
    ]);

    // INSERT params must end with the stamp columns (tx_hash, log_index, block_hash, block_number)
    const lendInsert = lendClient.query.mock.calls.find(
      (c) => c[0].trim().startsWith('INSERT INTO lend_position'),
    );
    expect(lendInsert).toBeDefined();
    const lendParams = lendInsert![1] as unknown[];
    expect(lendParams[6]).toBeInstanceOf(Buffer); // tx_hash
    expect(lendParams[7]).toBe(0); // log_index
    expect(lendParams[8]).toBeInstanceOf(Buffer); // block_hash
    expect(lendParams[9]).toBe('100'); // block_number (stringified bigint)

    const borrowInsert = borrowClient.query.mock.calls.find(
      (c) => c[0].trim().startsWith('INSERT INTO borrow_position'),
    );
    expect(borrowInsert).toBeDefined();
    const borrowParams = borrowInsert![1] as unknown[];
    expect(borrowParams[5]).toBeInstanceOf(Buffer);
    expect(borrowParams[6]).toBe(1);
  });

  it('short-circuits on already-stamped row without issuing INSERT', async () => {
    // alreadyStamped returns count=1 → row already written, skip mutation.
    const client = mockClient();
    client.query.mockImplementation(async (sql: string) => {
      if (/^SELECT count\(\*\)/i.test(sql.trim())) {
        return {
          rows: [{ count: '1' }],
          rowCount: 1,
          command: 'SELECT',
          oid: 0,
          fields: [],
        } as unknown as QueryResult;
      }
      return {
        rows: [],
        rowCount: 0,
        command: 'INSERT',
        oid: 0,
        fields: [],
      } as unknown as QueryResult;
    });
    const pool = mockPool(() => client.client);

    await applySettlementResult(
      pool,
      settlementResult({ borrowPositionEvents: [] }),
    );

    // Expect BEGIN, SELECT count(*), ROLLBACK — NO INSERT
    const verbs = client.query.mock.calls.map((c) => c[0].split(/\s+/u)[0]);
    expect(verbs).toEqual(['BEGIN', 'SELECT', 'ROLLBACK']);
    expect(verbs).not.toContain('INSERT');
  });

  it('connects to pool once per event (each in its own tx)', async () => {
    const clients = [mockClient(), mockClient(), mockClient()];
    const pool = mockPool(() => {
      const next = clients.shift();
      if (!next) throw new Error('unexpected connect');
      return next.client;
    });

    await applySettlementResult(
      pool,
      settlementResult({
        lendPositionEvents: [
          lendEvent({ logIndex: 0 }),
          lendEvent({ logIndex: 2, lender: '0x5555555555555555555555555555555555555555' }),
        ],
        borrowPositionEvents: [borrowEvent({ logIndex: 1 })],
      }),
    );

    expect((pool.connect as jest.Mock).mock.calls).toHaveLength(3);
  });

  it('rolls back the pg tx when the INSERT throws', async () => {
    const client = mockClient();
    client.query.mockImplementation(async (sql: string) => {
      if (/^SELECT count\(\*\)/i.test(sql.trim())) {
        return {
          rows: [{ count: '0' }],
          rowCount: 1,
          command: 'SELECT',
          oid: 0,
          fields: [],
        } as unknown as QueryResult;
      }
      if (sql.trim().startsWith('INSERT')) {
        throw new Error('simulated pg failure');
      }
      return {
        rows: [],
        rowCount: 0,
        command: 'ROLLBACK',
        oid: 0,
        fields: [],
      } as unknown as QueryResult;
    });
    const pool = mockPool(() => client.client);

    await expect(
      applySettlementResult(
        pool,
        settlementResult({ borrowPositionEvents: [] }),
      ),
    ).rejects.toThrow('simulated pg failure');

    const verbs = client.query.mock.calls.map((c) => c[0].split(/\s+/u)[0]);
    expect(verbs).toEqual(['BEGIN', 'SELECT', 'INSERT', 'ROLLBACK']);
    expect(client.release).toHaveBeenCalled();
  });

  it('noop for a result with zero lend + zero borrow events', async () => {
    const pool = mockPool(() => {
      throw new Error('should not connect');
    });
    await expect(
      applySettlementResult(
        pool,
        settlementResult({ lendPositionEvents: [], borrowPositionEvents: [] }),
      ),
    ).resolves.toBeUndefined();
    expect((pool.connect as jest.Mock).mock.calls).toHaveLength(0);
  });
});
