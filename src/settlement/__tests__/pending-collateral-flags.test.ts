import type { Pool, PoolClient } from 'pg';
import {
  clearForEvent,
  readForBorrowers,
} from '../database/pending-collateral-flags';

const fakePool = (
  rows: { user_address: Buffer; asset: Buffer }[] = [],
): { pool: Pool; query: jest.Mock } => {
  const query = jest.fn(async () => ({ rows, rowCount: rows.length }));
  return { pool: { query } as unknown as Pool, query };
};

const fakeClient = (): { tx: PoolClient; query: jest.Mock } => {
  const query = jest.fn(async () => ({ rows: [], rowCount: 0 }));
  return { tx: { query } as unknown as PoolClient, query };
};

describe('pending-collateral-flags repo', () => {
  describe('readForBorrowers', () => {
    it('returns empty map for empty borrower list (no SQL fired)', async () => {
      const { pool, query } = fakePool();
      const result = await readForBorrowers(pool, []);
      expect(result.size).toBe(0);
      expect(query).not.toHaveBeenCalled();
    });

    it('queries with deduped lowercase BYTEA params', async () => {
      const { pool, query } = fakePool();
      await readForBorrowers(pool, [
        '0xAAAA000000000000000000000000000000000001',
        '0xaaaa000000000000000000000000000000000001', // dup of above (different case)
        '0xBBBB000000000000000000000000000000000002',
      ] as `0x${string}`[]);

      expect(query).toHaveBeenCalledTimes(1);
      const callArgs = query.mock.calls[0]!;
      // Param is a BYTEA[] — distinct lowercased addresses only.
      const params = callArgs[1] as Buffer[][];
      expect(params).toHaveLength(1);
      expect(params[0]!).toHaveLength(2);
      expect(params[0]![0]!.toString('hex')).toBe(
        'aaaa000000000000000000000000000000000001',
      );
      expect(params[0]![1]!.toString('hex')).toBe(
        'bbbb000000000000000000000000000000000002',
      );
    });

    it('groups asset rows under each lowercased borrower key', async () => {
      const userA = Buffer.from('aaaa000000000000000000000000000000000001', 'hex');
      const userB = Buffer.from('bbbb000000000000000000000000000000000002', 'hex');
      const assetX = Buffer.from('cccc000000000000000000000000000000000003', 'hex');
      const assetY = Buffer.from('dddd000000000000000000000000000000000004', 'hex');

      const { pool } = fakePool([
        { user_address: userA, asset: assetX },
        { user_address: userA, asset: assetY },
        { user_address: userB, asset: assetX },
      ]);
      const result = await readForBorrowers(pool, [
        `0x${userA.toString('hex')}` as `0x${string}`,
        `0x${userB.toString('hex')}` as `0x${string}`,
      ]);

      expect(result.size).toBe(2);
      expect(result.get(`0x${userA.toString('hex')}`)).toEqual([
        `0x${assetX.toString('hex')}`,
        `0x${assetY.toString('hex')}`,
      ]);
      expect(result.get(`0x${userB.toString('hex')}`)).toEqual([
        `0x${assetX.toString('hex')}`,
      ]);
    });

    it('returns an empty map when no rows match', async () => {
      const { pool } = fakePool([]);
      const result = await readForBorrowers(pool, [
        '0xaaaa000000000000000000000000000000000001' as `0x${string}`,
      ]);
      expect(result.size).toBe(0);
    });
  });

  describe('clearForEvent', () => {
    it('issues a single DELETE with lowercased BYTEA params', async () => {
      const { tx, query } = fakeClient();
      await clearForEvent(
        tx,
        '0xAAAA000000000000000000000000000000000001' as `0x${string}`,
        '0xCCCC000000000000000000000000000000000003' as `0x${string}`,
      );

      expect(query).toHaveBeenCalledTimes(1);
      const [sql, params] = query.mock.calls[0]!;
      expect(sql).toMatch(/DELETE FROM pending_collateral_flags/);
      expect(params).toHaveLength(2);
      expect((params as Buffer[])[0]!.toString('hex')).toBe(
        'aaaa000000000000000000000000000000000001',
      );
      expect((params as Buffer[])[1]!.toString('hex')).toBe(
        'cccc000000000000000000000000000000000003',
      );
    });

    it('is idempotent — DELETE WHERE on a missing row is a natural no-op (caller observes no error)', async () => {
      const { tx } = fakeClient();
      // The fake client returns rowCount=0 by default; clearForEvent does not
      // throw on zero rows deleted.
      await expect(
        clearForEvent(
          tx,
          '0xaaaa000000000000000000000000000000000001' as `0x${string}`,
          '0xcccc000000000000000000000000000000000003' as `0x${string}`,
        ),
      ).resolves.toBeUndefined();
    });
  });
});
