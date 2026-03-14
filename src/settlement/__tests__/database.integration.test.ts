import { Pool } from 'pg';
import { persistSettlementResults } from '../database';
import type { SettlementResult } from '../smartContract';
import { insertMatches } from '../../tests/helpers/databaseTestHelpers';
import { createMatch } from '../../tests/helpers/testFixtures';

/**
 * Integration tests for PostgreSQL persistence.
 *
 * These tests require a running PostgreSQL instance accessible via DATABASE_URL.
 */
describe('persistSettlementResults Integration Tests', () => {
  let pool: Pool;
  const testData: Array<{ matchIds: string[]; txHash?: string }> = [];

  beforeAll(() => {
    if (!process.env.DATABASE_URL) {
      throw new Error(
        'DATABASE_URL must be set for database integration tests to run.',
      );
    }
    pool = new Pool({
      connectionString: process.env.DATABASE_URL,
    });
  });

  afterAll(async () => {
    await pool.end();
  });

  afterEach(async () => {
    // Clean up test data after each test
    if (testData.length === 0) {
      return;
    }

    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      for (const data of testData) {
        // Clean up settlement_items and settlement_batches
        if (data.txHash) {
          const batchesToClean = await client.query<{ id: string }>(
            'SELECT id FROM settlement_batches WHERE tx_hash = $1',
            [data.txHash],
          );
          for (const batch of batchesToClean.rows) {
            await client.query('DELETE FROM settlement_items WHERE settlement_batch_id = $1', [
              batch.id,
            ]);
            await client.query('DELETE FROM settlement_batches WHERE id = $1', [batch.id]);
          }
        }
        // Clean up matches
        if (data.matchIds.length > 0) {
          await client.query('DELETE FROM matches WHERE id = ANY($1::uuid[])', [data.matchIds]);
        }
      }
      await client.query('COMMIT');
      // Clear the test data array for next test
      testData.length = 0;
    } catch (cleanupError) {
      // Log but don't throw - we're in cleanup
      // eslint-disable-next-line no-console
      console.error('Failed to clean up test data in afterEach:', cleanupError);
      try {
        await client.query('ROLLBACK');
      } catch {
        // Ignore rollback errors
      }
    } finally {
      client.release();
    }
  });

  it('should insert settlement_batches and settlement_items with COMPLETED status', async () => {
    const client = await pool.connect();
    // Use unique identifiers to avoid conflicts with previous test runs
    const timestamp = Date.now();
    const txHash = `0x${timestamp.toString(16).padStart(64, '0')}`;
    // Generate valid UUIDs: last segment must be exactly 12 hex digits
    // Use 11 hex digits from timestamp + '1' or '2' to make exactly 12 hex digits
    const hexSuffix = timestamp.toString(16).padStart(11, '0').slice(-11);
    const settledMatchIds = [
      `550e8400-e29b-41d4-a716-${hexSuffix}1`,
      `550e8400-e29b-41d4-a716-${hexSuffix}2`,
    ];

    // Track test data for cleanup in afterEach
    testData.push({ matchIds: settledMatchIds, txHash });

    try {
      // Clean up any existing test data with these identifiers (in case of previous failed runs)
      await client.query('BEGIN');
      const existingBatches = await client.query<{ id: string }>(
        'SELECT id FROM settlement_batches WHERE tx_hash = $1',
        [txHash],
      );
      for (const batch of existingBatches.rows) {
        await client.query('DELETE FROM settlement_items WHERE settlement_batch_id = $1', [
          batch.id,
        ]);
        await client.query('DELETE FROM settlement_batches WHERE id = $1', [batch.id]);
      }
      await client.query('DELETE FROM matches WHERE id = ANY($1::uuid[])', [settledMatchIds]);
      await client.query('COMMIT');

      // Insert matches into database to satisfy foreign key constraint
      // Commit immediately so persistSettlementResults can see them
      await client.query('BEGIN');
      const matches = [
        createMatch({ matchId: settledMatchIds[0] }),
        createMatch({ matchId: settledMatchIds[1] }),
      ];
      await insertMatches(client, matches);
      await client.query('COMMIT');

      const settlementResult: SettlementResult = {
        transactionHash: txHash,
        blockNumber: 1,
        gasUsed: 1000,
        timestamp: Date.now(),
        settledMatchIds,
        bondTokenEvents: [],
        lendPositionEvents: [],
        borrowPositionEvents: [],
      };

      await persistSettlementResults({
        results: [settlementResult],
      });

      // Query the results
      const batchRows = await client.query<{
        id: string;
        tx_hash: string;
        status: string;
      }>(
        `
          SELECT id, tx_hash, status
          FROM settlement_batches
          WHERE tx_hash = $1
        `,
        [txHash],
      );

      expect(batchRows.rows.length).toBe(1);
      const batch = batchRows.rows[0];
      expect(batch.status).toBe('COMPLETED');

      const itemRows = await client.query<{
        match_id: string;
      }>(
        `
          SELECT match_id
          FROM settlement_items
          WHERE settlement_batch_id = $1
        `,
        [batch.id],
      );

      const returnedMatchIds = itemRows.rows.map((r: { match_id: string }) => r.match_id).sort();
      expect(returnedMatchIds).toEqual([...settledMatchIds].sort());
    } finally {
      // Always clean up test data, even if assertions fail
      try {
        await client.query('BEGIN');
        const batchesToClean = await client.query<{ id: string }>(
          'SELECT id FROM settlement_batches WHERE tx_hash = $1',
          [txHash],
        );
        for (const batch of batchesToClean.rows) {
          await client.query('DELETE FROM settlement_items WHERE settlement_batch_id = $1', [
            batch.id,
          ]);
          await client.query('DELETE FROM settlement_batches WHERE id = $1', [batch.id]);
        }
        await client.query('DELETE FROM matches WHERE id = ANY($1::uuid[])', [settledMatchIds]);
        await client.query('COMMIT');
      } catch (cleanupError) {
        // Log but don't throw - we're in cleanup
        // eslint-disable-next-line no-console
        console.error('Failed to clean up test data:', cleanupError);
      }
      client.release();
    }
  });
});
