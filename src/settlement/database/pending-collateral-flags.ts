import { hexToBytea } from '@centuari-labs/on-chain-effects';
import type { Pool, PoolClient } from 'pg';
import type { Address, Hex } from 'viem';

/**
 * Read + DELETE access for the `pending_collateral_flags` table that lives
 * in the same Postgres database as the indexer-v3 shared schema. Backend-v2
 * INSERTs into this table when a user calls `POST /collateral/flag`; the
 * settlement engine reads it at settle time (queue-driven encoder) and
 * DELETEs rows on receipt success (event-driven cleanup). Indexer-v3's
 * `balance-ledger.processor.ts` is a parallel tail writer that DELETEs the
 * same rows when it observes `CollateralFlagSet` events on-chain — covers
 * direct-caller `CollateralManager.flag(asset)` events and any eager-path
 * crashes the settlement engine missed.
 */

/**
 * Read pending flag rows for a set of borrowers in a single query.
 *
 * @returns Map keyed by lowercased borrower address with the borrower's
 *   queued asset addresses (lowercased, deduped, sorted by created_at).
 *   Borrowers with no queued rows are absent from the map.
 */
export const readForBorrowers = async (
  pool: Pool,
  borrowers: readonly Address[],
): Promise<Map<string, Address[]>> => {
  const out = new Map<string, Address[]>();
  if (borrowers.length === 0) return out;

  // De-dup by lowercase before the query so the BYTEA `= ANY($1)` predicate
  // doesn't repeat the same row condition.
  const distinctLower = Array.from(
    new Set(borrowers.map((b) => b.toLowerCase())),
  );
  const params = distinctLower.map((b) => hexToBytea(b as Hex));

  const result = await pool.query<{ user_address: Buffer; asset: Buffer }>(
    `SELECT user_address, asset
       FROM pending_collateral_flags
      WHERE user_address = ANY($1::bytea[])
      ORDER BY created_at ASC`,
    [params],
  );

  for (const row of result.rows) {
    const userKey = `0x${row.user_address.toString('hex')}`;
    const asset = `0x${row.asset.toString('hex')}` as Address;
    const existing = out.get(userKey);
    if (existing) {
      existing.push(asset);
    } else {
      out.set(userKey, [asset]);
    }
  }

  return out;
};

/**
 * DELETE a single (user, asset) pending flag row. Idempotent: no row found
 * is a no-op rather than an error. Caller passes the pg client from a
 * surrounding `withTransaction` so the DELETE rides on the same tx as the
 * `applyOnChainEffect` user_balance stamping (when used eagerly) or rolls
 * back together with it on failure.
 */
export const clearForEvent = async (
  client: PoolClient,
  user: Address,
  asset: Address,
): Promise<void> => {
  await client.query(
    `DELETE FROM pending_collateral_flags
      WHERE user_address = $1 AND asset = $2`,
    [hexToBytea(user), hexToBytea(asset)],
  );
};
