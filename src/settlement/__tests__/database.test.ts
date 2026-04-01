/**
 * Unit tests for database.ts — mocks pg.Pool to test error classification,
 * retry logic, and exported functions without a real database.
 */

// Must mock pg before importing database module
const mockQuery = jest.fn();
const mockRelease = jest.fn();
const mockConnect = jest.fn().mockResolvedValue({
  query: mockQuery,
  release: mockRelease,
});
const mockPoolQuery = jest.fn();

jest.mock('pg', () => ({
  Pool: jest.fn().mockImplementation(() => ({
    connect: mockConnect,
    query: mockPoolQuery,
  })),
}));

import {
  getPool,
  persistSettlementResults,
  unlockFailedMatches,
  recordFailedMatches,
  restoreOrdersForFailedMatches,
  findUnprocessedSettlementBatches,
} from '../database';
import type { SettlementResult } from '../smartContract';
import { createMatch } from '../../tests/helpers/testFixtures';
import type { AppConfig } from '../../config';
import { logger } from '../../logger';

const createTestAppConfig = (): AppConfig => ({
  redisUrl: 'redis://localhost:6379',
  settlementMatchesStream: 'settlement:matches',
  consumerGroup: 'settlement-engine',
  consumerName: 'test-consumer',
  readBlockMs: 5000,
  readCount: 10,
  streamMaxLen: 10000,
  batchSize: 10,
  batchIntervalMs: 5000,
  pollIntervalMs: 200,
  pendingReclaimIntervalMs: 60000,
  xclaimMinIdleMs: 60000,
  failureBackoffBaseMs: 1000,
  failureBackoffMaxMs: 60000,
  settlementContractAddress: '0x1234567890123456789012345678901234567890',
  ethereumRpcUrl: 'https://rpc.example.com',
  settlementPrivateKey:
    'abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789',
  ethereumChainId: 421614,
  nonceLockTtlMs: 30000,
  txConfirmationTimeoutMs: 120000,
  nonceLockRetryDelayMs: 500,
});

describe('database', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    // Default: BEGIN, COMMIT succeed
    mockQuery.mockResolvedValue({ rows: [] });
    process.env.DATABASE_URL = 'postgresql://localhost:5432/test';
  });

  describe('getPool', () => {
    it('should throw if DATABASE_URL is not set', () => {
      // Reset module to clear singleton
      jest.resetModules();
      jest.mock('pg', () => ({
        Pool: jest.fn().mockImplementation(() => ({})),
      }));
      delete process.env.DATABASE_URL;
      const { getPool: freshGetPool } = require('../database');
      expect(() => freshGetPool()).toThrow('DATABASE_URL');
    });
  });

  describe('mapPostgresErrorToDatabaseError (via persistSettlementResults retry)', () => {
    const config = createTestAppConfig();

    const createMinimalResult = (): SettlementResult => ({
      transactionHash: '0xabc123',
      blockNumber: 100,
      gasUsed: 50000,
      timestamp: 1700000000000,
      settledMatchIds: ['550e8400-e29b-41d4-a716-446655440000'],
      bondTokenEvents: [],
      lendPositionEvents: [],
      borrowPositionEvents: [],
    });

    it('should retry on serialization_failure (40001) then succeed', async () => {
      const result = createMinimalResult();
      const match = createMatch();

      // First call: BEGIN succeeds
      // Second call: INSERT throws 40001
      // Then retry: BEGIN, INSERT succeeds, COMMIT
      let callCount = 0;
      mockQuery.mockImplementation((sql: string) => {
        callCount++;
        if (callCount === 2) {
          // First INSERT attempt — throw retryable error
          const error = new Error('serialization failure') as Error & { code: string };
          error.code = '40001';
          throw error;
        }
        if (sql?.includes?.('INSERT INTO settlement_batches')) {
          return { rows: [{ id: 'batch-1' }] };
        }
        return { rows: [] };
      });

      await persistSettlementResults({
        results: [result],
        matchPayloads: new Map([[match.matchId, match]]),
        config,
        maxRetries: 2,
        retryDelayMs: 10,
      });

      // Should have called BEGIN at least twice (first attempt + retry)
      const beginCalls = mockQuery.mock.calls.filter(
        (call) => call[0] === 'BEGIN',
      );
      expect(beginCalls.length).toBeGreaterThanOrEqual(2);
    });

    it('should fail immediately on unique_violation (23505)', async () => {
      const result = createMinimalResult();
      const match = createMatch();

      mockQuery.mockImplementation((sql: string) => {
        if (sql === 'BEGIN') return { rows: [] };
        if (sql?.includes?.('INSERT INTO settlement_batches')) {
          const error = new Error('unique violation') as Error & { code: string };
          error.code = '23505';
          throw error;
        }
        return { rows: [] };
      });

      await expect(
        persistSettlementResults({
          results: [result],
          matchPayloads: new Map([[match.matchId, match]]),
          config,
          maxRetries: 3,
          retryDelayMs: 10,
        }),
      ).rejects.toMatchObject({
        code: '23505',
        retryable: false,
      });

      // Should NOT have retried — only 1 BEGIN call
      const beginCalls = mockQuery.mock.calls.filter(
        (call) => call[0] === 'BEGIN',
      );
      expect(beginCalls.length).toBe(1);
    });

    it('should fail immediately on foreign_key_violation (23503)', async () => {
      const result = createMinimalResult();
      const match = createMatch();

      mockQuery.mockImplementation((sql: string) => {
        if (sql === 'BEGIN') return { rows: [] };
        if (sql?.includes?.('INSERT INTO settlement_batches')) {
          const error = new Error('FK violation') as Error & { code: string };
          error.code = '23503';
          throw error;
        }
        return { rows: [] };
      });

      await expect(
        persistSettlementResults({
          results: [result],
          matchPayloads: new Map([[match.matchId, match]]),
          config,
          maxRetries: 3,
          retryDelayMs: 10,
        }),
      ).rejects.toMatchObject({
        code: '23503',
        retryable: false,
      });
    });

    it('should treat unknown error codes as retryable', async () => {
      const result = createMinimalResult();
      const match = createMatch();

      mockQuery.mockImplementation((sql: string) => {
        if (sql === 'BEGIN') return { rows: [] };
        if (sql?.includes?.('INSERT INTO settlement_batches')) {
          const error = new Error('unknown pg error') as Error & { code: string };
          error.code = '99999';
          throw error;
        }
        return { rows: [] };
      });

      await expect(
        persistSettlementResults({
          results: [result],
          matchPayloads: new Map([[match.matchId, match]]),
          config,
          maxRetries: 1,
          retryDelayMs: 10,
        }),
      ).rejects.toMatchObject({
        code: '99999',
        retryable: true,
      });

      // Should have retried once before failing
      const beginCalls = mockQuery.mock.calls.filter(
        (call) => call[0] === 'BEGIN',
      );
      expect(beginCalls.length).toBe(2); // initial + 1 retry
    });

    it('should retry on deadlock (40P01)', async () => {
      const result = createMinimalResult();
      const match = createMatch();
      let attemptCount = 0;

      mockQuery.mockImplementation((sql: string) => {
        if (sql === 'BEGIN') return { rows: [] };
        if (sql?.includes?.('INSERT INTO settlement_batches')) {
          attemptCount++;
          if (attemptCount === 1) {
            const error = new Error('deadlock detected') as Error & { code: string };
            error.code = '40P01';
            throw error;
          }
          return { rows: [{ id: 'batch-1' }] };
        }
        return { rows: [] };
      });

      await persistSettlementResults({
        results: [result],
        matchPayloads: new Map([[match.matchId, match]]),
        config,
        maxRetries: 2,
        retryDelayMs: 10,
      });

      expect(attemptCount).toBe(2);
    });
  });

  describe('persistSettlementResults', () => {
    const config = createTestAppConfig();

    it('should return early for empty results', async () => {
      await persistSettlementResults({
        results: [],
        matchPayloads: new Map(),
        config,
      });
      expect(mockConnect).not.toHaveBeenCalled();
    });

    it('should insert settlement_batches and settlement_items', async () => {
      const result: SettlementResult = {
        transactionHash: '0xabc',
        blockNumber: 100,
        gasUsed: 50000,
        timestamp: 1700000000000,
        settledMatchIds: ['550e8400-e29b-41d4-a716-446655440000'],
        bondTokenEvents: [],
        lendPositionEvents: [],
        borrowPositionEvents: [],
      };
      const match = createMatch();

      mockQuery.mockImplementation((sql: string) => {
        if (sql?.includes?.('INSERT INTO settlement_batches')) {
          return { rows: [{ id: 'batch-1' }] };
        }
        return { rows: [] };
      });

      await persistSettlementResults({
        results: [result],
        matchPayloads: new Map([[match.matchId, match]]),
        config,
        maxRetries: 0,
        retryDelayMs: 10,
      });

      // Verify settlement_batches insert was called
      const batchInsertCalls = mockQuery.mock.calls.filter((call) =>
        call[0]?.includes?.('INSERT INTO settlement_batches'),
      );
      expect(batchInsertCalls.length).toBe(1);
      expect(batchInsertCalls[0][1]).toContain('0xabc'); // tx hash
    });

    it('should not rethrow Phase 2 failures', async () => {
      const result: SettlementResult = {
        transactionHash: '0xabc',
        blockNumber: 100,
        gasUsed: 50000,
        timestamp: 1700000000000,
        settledMatchIds: ['550e8400-e29b-41d4-a716-446655440000'],
        bondTokenEvents: [
          {
            marketId: '0x660e8400e29b41d4a716446655440099000000000000000000000000000000',
            bondToken: '0xaaaa',
            loanToken: '0xbbbb',
            maturity: 1735689600n,
            name: 'Bond',
            symbol: 'BT',
          },
        ],
        lendPositionEvents: [],
        borrowPositionEvents: [],
      };
      const match = createMatch();

      let phase1Done = false;
      mockQuery.mockImplementation((sql: string) => {
        if (sql?.includes?.('INSERT INTO settlement_batches')) {
          return { rows: [{ id: 'batch-1' }] };
        }
        if (sql === 'COMMIT' && !phase1Done) {
          phase1Done = true;
          return { rows: [] };
        }
        // Phase 2: SELECT asset fails (triggers error in processSettlementEvents)
        if (phase1Done && sql?.includes?.('SELECT id FROM assets')) {
          return { rows: [] }; // asset not found → throws
        }
        return { rows: [] };
      });

      // Should not throw — Phase 2 failures are caught
      await expect(
        persistSettlementResults({
          results: [result],
          matchPayloads: new Map([[match.matchId, match]]),
          config,
          maxRetries: 0,
          retryDelayMs: 10,
        }),
      ).resolves.toBeUndefined();
    });
  });

  describe('unlockFailedMatches', () => {
    it('should skip matches when account not found', async () => {
      const match = createMatch();
      const loggerSpy = jest.spyOn(logger, 'warn').mockImplementation(() => {});

      mockQuery.mockImplementation((sql: string) => {
        if (sql === 'BEGIN' || sql === 'COMMIT') return { rows: [] };
        // Return empty for account lookup
        return { rows: [] };
      });

      await unlockFailedMatches([match]);
      expect(loggerSpy).toHaveBeenCalledWith(
        expect.objectContaining({ component: 'database' }),
        expect.stringContaining('Cannot unlock match'),
      );
      loggerSpy.mockRestore();
    });

    it('should skip matches when asset not found', async () => {
      const match = createMatch();
      const loggerSpy = jest.spyOn(logger, 'warn').mockImplementation(() => {});

      mockQuery.mockImplementation((sql: string) => {
        if (sql === 'BEGIN' || sql === 'COMMIT') return { rows: [] };
        if (sql?.includes?.('SELECT id FROM accounts')) {
          return { rows: [{ id: 'account-1' }] };
        }
        // Asset lookup returns empty
        return { rows: [] };
      });

      await unlockFailedMatches([match]);
      expect(loggerSpy).toHaveBeenCalledWith(
        expect.objectContaining({ component: 'database' }),
        expect.stringContaining('asset not found'),
      );
      loggerSpy.mockRestore();
    });

    it('should execute portfolio unlock queries in deadlock-safe order', async () => {
      const match = createMatch();
      const executedQueries: string[][] = [];

      mockQuery.mockImplementation((sql: string, params?: string[]) => {
        if (sql === 'BEGIN' || sql === 'COMMIT') return { rows: [] };
        if (sql?.includes?.('SELECT id FROM accounts')) {
          // Return lender and borrower IDs — lender < borrower alphabetically
          if (params?.[0]?.includes?.('1234')) {
            return { rows: [{ id: 'aaa-lender' }] };
          }
          return { rows: [{ id: 'zzz-borrower' }] };
        }
        if (sql?.includes?.('SELECT id FROM assets')) {
          return { rows: [{ id: 'asset-1' }] };
        }
        if (sql?.includes?.('UPDATE portfolio')) {
          executedQueries.push(params ?? []);
        }
        return { rows: [] };
      });

      await unlockFailedMatches([match]);

      // Should have executed 2 portfolio updates
      expect(executedQueries.length).toBe(2);
      // First update should be for the lower account ID (aaa-lender)
      expect(executedQueries[0]).toContain('aaa-lender');
    });

    it('should rollback on error', async () => {
      const match = createMatch();

      mockQuery.mockImplementation((sql: string) => {
        if (sql === 'BEGIN') return { rows: [] };
        if (sql?.includes?.('SELECT id FROM accounts')) {
          throw new Error('connection lost');
        }
        return { rows: [] };
      });

      // Should not throw (catches internally)
      await unlockFailedMatches([match]);

      const rollbackCalls = mockQuery.mock.calls.filter(
        (call) => call[0] === 'ROLLBACK',
      );
      expect(rollbackCalls.length).toBe(1);
    });
  });

  describe('recordFailedMatches', () => {
    it('should update match status to FAILED', async () => {
      const match = createMatch();

      await recordFailedMatches([match], 'Contract error: AlreadySettled');

      const updateCalls = mockQuery.mock.calls.filter((call) =>
        call[0]?.includes?.('settlement_status'),
      );
      expect(updateCalls.length).toBe(1);
      expect(updateCalls[0][1]).toContain(match.matchId);
      expect(updateCalls[0][1]).toContain('Contract error: AlreadySettled');
    });

    it('should update multiple matches', async () => {
      const matches = [
        createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440001' }),
        createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440002' }),
      ];

      await recordFailedMatches(matches, 'Failed');

      const updateCalls = mockQuery.mock.calls.filter((call) =>
        call[0]?.includes?.('settlement_status'),
      );
      expect(updateCalls.length).toBe(2);
    });

    it('should rollback on error', async () => {
      const match = createMatch();

      mockQuery.mockImplementation((sql: string) => {
        if (sql === 'BEGIN') return { rows: [] };
        if (sql?.includes?.('UPDATE matches')) {
          throw new Error('db error');
        }
        return { rows: [] };
      });

      await recordFailedMatches([match], 'error');

      const rollbackCalls = mockQuery.mock.calls.filter(
        (call) => call[0] === 'ROLLBACK',
      );
      expect(rollbackCalls.length).toBe(1);
    });
  });

  describe('restoreOrdersForFailedMatches', () => {
    it('should restore both lend and borrow orders', async () => {
      const match = createMatch();

      await restoreOrdersForFailedMatches([match]);

      const updateCalls = mockQuery.mock.calls.filter((call) =>
        call[0]?.includes?.('UPDATE orders'),
      );
      // Should update 2 orders per match (lend + borrow)
      expect(updateCalls.length).toBe(2);
    });

    it('should pass correct order IDs', async () => {
      const match = createMatch({
        lendOrderId: '550e8400-e29b-41d4-a716-446655440001',
        borrowOrderId: '550e8400-e29b-41d4-a716-446655440002',
      });

      await restoreOrdersForFailedMatches([match]);

      const updateCalls = mockQuery.mock.calls.filter((call) =>
        call[0]?.includes?.('UPDATE orders'),
      );
      expect(updateCalls[0][1][0]).toBe('550e8400-e29b-41d4-a716-446655440001');
      expect(updateCalls[1][1][0]).toBe('550e8400-e29b-41d4-a716-446655440002');
    });

    it('should rollback on error', async () => {
      const match = createMatch();

      mockQuery.mockImplementation((sql: string) => {
        if (sql === 'BEGIN') return { rows: [] };
        if (sql?.includes?.('UPDATE orders')) {
          throw new Error('db error');
        }
        return { rows: [] };
      });

      await restoreOrdersForFailedMatches([match]);

      const rollbackCalls = mockQuery.mock.calls.filter(
        (call) => call[0] === 'ROLLBACK',
      );
      expect(rollbackCalls.length).toBe(1);
    });
  });

  describe('findUnprocessedSettlementBatches', () => {
    it('should return unprocessed batches', async () => {
      mockPoolQuery.mockResolvedValue({
        rows: [
          {
            id: 'batch-1',
            raw_events: {
              bondTokenEvents: [],
              lendPositionEvents: [],
              borrowPositionEvents: [],
            },
          },
        ],
      });

      const result = await findUnprocessedSettlementBatches(10);
      expect(result).toHaveLength(1);
      expect(result[0].id).toBe('batch-1');
    });

    it('should parse string raw_events as JSON', async () => {
      const events = {
        bondTokenEvents: [],
        lendPositionEvents: [],
        borrowPositionEvents: [],
      };
      mockPoolQuery.mockResolvedValue({
        rows: [{ id: 'batch-1', raw_events: JSON.stringify(events) }],
      });

      const result = await findUnprocessedSettlementBatches();
      expect(result[0].rawEvents).toEqual(events);
    });

    it('should return empty array when no batches found', async () => {
      mockPoolQuery.mockResolvedValue({ rows: [] });
      const result = await findUnprocessedSettlementBatches();
      expect(result).toHaveLength(0);
    });

    it('should respect the limit parameter', async () => {
      mockPoolQuery.mockResolvedValue({ rows: [] });
      await findUnprocessedSettlementBatches(5);
      expect(mockPoolQuery.mock.calls[0][1]).toEqual([5]);
    });
  });

  describe('executeWithRetry (via persistSettlementResults)', () => {
    const config = createTestAppConfig();

    const createMinimalResult = (): SettlementResult => ({
      transactionHash: '0xabc123',
      blockNumber: 100,
      gasUsed: 50000,
      timestamp: 1700000000000,
      settledMatchIds: ['550e8400-e29b-41d4-a716-446655440000'],
      bondTokenEvents: [],
      lendPositionEvents: [],
      borrowPositionEvents: [],
    });

    it('should succeed on first try without retrying', async () => {
      const result = createMinimalResult();
      const match = createMatch();

      mockQuery.mockImplementation((sql: string) => {
        if (sql?.includes?.('INSERT INTO settlement_batches')) {
          return { rows: [{ id: 'batch-1' }] };
        }
        return { rows: [] };
      });

      await persistSettlementResults({
        results: [result],
        matchPayloads: new Map([[match.matchId, match]]),
        config,
        maxRetries: 3,
        retryDelayMs: 10,
      });

      // Phase 1 uses 1 BEGIN, Phase 2 also uses 1 BEGIN.
      // With no retries, Phase 1 should only BEGIN once — check that
      // the first BEGIN is followed by a COMMIT without a second BEGIN in between.
      const beginCalls = mockQuery.mock.calls.filter(
        (call) => call[0] === 'BEGIN',
      );
      // 1 BEGIN for Phase 1 + 1 BEGIN for Phase 2 = 2 total
      // If retries happened, we'd see 3+ BEGINs
      expect(beginCalls.length).toBe(2);
    });

    it('should retry on retryable error then succeed', async () => {
      const result = createMinimalResult();
      const match = createMatch();
      let insertAttempt = 0;

      mockQuery.mockImplementation((sql: string) => {
        if (sql?.includes?.('INSERT INTO settlement_batches')) {
          insertAttempt++;
          if (insertAttempt === 1) {
            const error = new Error('serialization failure') as Error & { code: string };
            error.code = '40001';
            throw error;
          }
          return { rows: [{ id: 'batch-1' }] };
        }
        return { rows: [] };
      });

      await persistSettlementResults({
        results: [result],
        matchPayloads: new Map([[match.matchId, match]]),
        config,
        maxRetries: 3,
        retryDelayMs: 10,
      });

      const beginCalls = mockQuery.mock.calls.filter(
        (call) => call[0] === 'BEGIN',
      );
      expect(beginCalls.length).toBeGreaterThanOrEqual(2);
    });

    it('should exhaust max retries and throw', async () => {
      const result = createMinimalResult();
      const match = createMatch();

      mockQuery.mockImplementation((sql: string) => {
        if (sql === 'BEGIN' || sql === 'ROLLBACK') return { rows: [] };
        if (sql?.includes?.('INSERT INTO settlement_batches')) {
          const error = new Error('serialization failure') as Error & { code: string };
          error.code = '40001';
          throw error;
        }
        return { rows: [] };
      });

      await expect(
        persistSettlementResults({
          results: [result],
          matchPayloads: new Map([[match.matchId, match]]),
          config,
          maxRetries: 2,
          retryDelayMs: 10,
        }),
      ).rejects.toMatchObject({ code: '40001' });

      const beginCalls = mockQuery.mock.calls.filter(
        (call) => call[0] === 'BEGIN',
      );
      expect(beginCalls.length).toBe(3); // initial + 2 retries
    });

    it('should fail immediately on non-retryable error without retrying', async () => {
      const result = createMinimalResult();
      const match = createMatch();

      mockQuery.mockImplementation((sql: string) => {
        if (sql === 'BEGIN' || sql === 'ROLLBACK') return { rows: [] };
        if (sql?.includes?.('INSERT INTO settlement_batches')) {
          const error = new Error('unique violation') as Error & { code: string };
          error.code = '23505';
          throw error;
        }
        return { rows: [] };
      });

      await expect(
        persistSettlementResults({
          results: [result],
          matchPayloads: new Map([[match.matchId, match]]),
          config,
          maxRetries: 3,
          retryDelayMs: 10,
        }),
      ).rejects.toMatchObject({ code: '23505', retryable: false });

      const beginCalls = mockQuery.mock.calls.filter(
        (call) => call[0] === 'BEGIN',
      );
      expect(beginCalls.length).toBe(1);
    });

    it('should double backoff delay between retries', async () => {
      const result = createMinimalResult();
      const match = createMatch();

      // Track timestamps of each BEGIN call to verify exponential backoff
      const beginTimestamps: number[] = [];
      const startTime = Date.now();

      mockQuery.mockImplementation((sql: string) => {
        if (sql === 'BEGIN') {
          beginTimestamps.push(Date.now() - startTime);
          return { rows: [] };
        }
        if (sql === 'ROLLBACK') return { rows: [] };
        if (sql?.includes?.('INSERT INTO settlement_batches')) {
          const error = new Error('serialization failure') as Error & { code: string };
          error.code = '40001';
          throw error;
        }
        return { rows: [] };
      });

      await expect(
        persistSettlementResults({
          results: [result],
          matchPayloads: new Map([[match.matchId, match]]),
          config,
          maxRetries: 2,
          retryDelayMs: 50,
        }),
      ).rejects.toMatchObject({ code: '40001' });

      // 3 BEGINs: initial + 2 retries
      expect(beginTimestamps.length).toBe(3);

      // First retry delay should be ~50ms, second ~100ms (doubled)
      const firstRetryDelay = beginTimestamps[1] - beginTimestamps[0];
      const secondRetryDelay = beginTimestamps[2] - beginTimestamps[1];

      // Second delay should be roughly double the first (with some tolerance)
      // Allow ±30ms tolerance for timer imprecision
      expect(firstRetryDelay).toBeGreaterThanOrEqual(40);
      expect(firstRetryDelay).toBeLessThan(100);
      expect(secondRetryDelay).toBeGreaterThanOrEqual(80);
      expect(secondRetryDelay).toBeLessThan(200);
      // The ratio should be approximately 2x
      expect(secondRetryDelay / firstRetryDelay).toBeGreaterThanOrEqual(1.5);
    });
  });

  describe('upsertMatch (via persistSettlementResults Phase 1)', () => {
    const config = createTestAppConfig();

    const createMinimalResult = (): SettlementResult => ({
      transactionHash: '0xabc123',
      blockNumber: 100,
      gasUsed: 50000,
      timestamp: 1700000000000,
      settledMatchIds: ['550e8400-e29b-41d4-a716-446655440000'],
      bondTokenEvents: [],
      lendPositionEvents: [],
      borrowPositionEvents: [],
    });

    it('should insert match when all lookups succeed', async () => {
      const result = createMinimalResult();
      const match = createMatch();

      mockQuery.mockImplementation((sql: string, params?: string[]) => {
        if (sql?.includes?.('INSERT INTO settlement_batches')) {
          return { rows: [{ id: 'batch-1' }] };
        }
        if (sql?.includes?.('SELECT id FROM assets')) {
          return { rows: [{ id: 'asset-1' }] };
        }
        if (sql?.includes?.('SELECT id FROM accounts')) {
          if (params?.[0]?.toLowerCase()?.includes?.('1234')) {
            return { rows: [{ id: 'lender-1' }] };
          }
          return { rows: [{ id: 'borrower-1' }] };
        }
        return { rows: [] };
      });

      await persistSettlementResults({
        results: [result],
        matchPayloads: new Map([[match.matchId, match]]),
        config,
        maxRetries: 0,
        retryDelayMs: 10,
      });

      const matchInsertCalls = mockQuery.mock.calls.filter((call) =>
        call[0]?.includes?.('INSERT INTO matches'),
      );
      expect(matchInsertCalls.length).toBe(1);
      expect(matchInsertCalls[0][1]).toContain(match.matchId);
    });

    it('should skip match upsert when asset not found', async () => {
      const result = createMinimalResult();
      const match = createMatch();
      const loggerSpy = jest.spyOn(logger, 'warn').mockImplementation(() => {});

      mockQuery.mockImplementation((sql: string) => {
        if (sql?.includes?.('INSERT INTO settlement_batches')) {
          return { rows: [{ id: 'batch-1' }] };
        }
        if (sql?.includes?.('SELECT id FROM assets')) {
          return { rows: [] };
        }
        return { rows: [] };
      });

      await persistSettlementResults({
        results: [result],
        matchPayloads: new Map([[match.matchId, match]]),
        config,
        maxRetries: 0,
        retryDelayMs: 10,
      });

      expect(loggerSpy).toHaveBeenCalledWith(
        expect.objectContaining({ component: 'database' }),
        expect.stringContaining('Asset not found'),
      );

      const matchInsertCalls = mockQuery.mock.calls.filter((call) =>
        call[0]?.includes?.('INSERT INTO matches'),
      );
      expect(matchInsertCalls.length).toBe(0);

      loggerSpy.mockRestore();
    });

    it('should skip match upsert when lender account not found', async () => {
      const result = createMinimalResult();
      const match = createMatch();
      const loggerSpy = jest.spyOn(logger, 'warn').mockImplementation(() => {});

      mockQuery.mockImplementation((sql: string, params?: string[]) => {
        if (sql?.includes?.('INSERT INTO settlement_batches')) {
          return { rows: [{ id: 'batch-1' }] };
        }
        if (sql?.includes?.('SELECT id FROM assets')) {
          return { rows: [{ id: 'asset-1' }] };
        }
        if (sql?.includes?.('SELECT id FROM accounts')) {
          // Lender wallet lookup returns empty
          if (params?.[0]?.toLowerCase()?.includes?.('1234')) {
            return { rows: [] };
          }
          return { rows: [{ id: 'borrower-1' }] };
        }
        return { rows: [] };
      });

      await persistSettlementResults({
        results: [result],
        matchPayloads: new Map([[match.matchId, match]]),
        config,
        maxRetries: 0,
        retryDelayMs: 10,
      });

      expect(loggerSpy).toHaveBeenCalledWith(
        expect.objectContaining({ component: 'database' }),
        expect.stringContaining('Lender account not found'),
      );

      loggerSpy.mockRestore();
    });
  });

  describe('processSettlementEvents (via persistSettlementResults Phase 2)', () => {
    const config = createTestAppConfig();

    it('should process events in FK order: bond token -> lend position -> borrow position', async () => {
      const match = createMatch();
      const marketIdHex = '0x660e8400e29b41d4a716446655440099000000000000000000000000000000';

      const result: SettlementResult = {
        transactionHash: '0xabc123',
        blockNumber: 100,
        gasUsed: 50000,
        timestamp: 1700000000000,
        settledMatchIds: [match.matchId],
        bondTokenEvents: [
          {
            marketId: marketIdHex,
            bondToken: '0xaaaa',
            loanToken: '0xbbbb',
            maturity: 1735689600n,
            name: 'Bond',
            symbol: 'BT',
          },
        ],
        lendPositionEvents: [
          {
            marketId: marketIdHex,
            lender: match.lenderWallet,
            bondToken: '0xaaaa',
            cbtAmount: 1000000n,
            principal: 1000000n,
            rate: 5000n,
          },
        ],
        borrowPositionEvents: [
          {
            marketId: marketIdHex,
            borrower: match.borrowerWallet,
            principal: 1000000n,
            debt: 1050000n,
            rate: 5000n,
          },
        ],
      };

      let phase1Done = false;
      const queryOrder: string[] = [];

      mockQuery.mockImplementation((sql: string, params?: unknown[]) => {
        // Phase 1 queries
        if (sql?.includes?.('INSERT INTO settlement_batches')) {
          return { rows: [{ id: 'batch-1' }] };
        }
        if (sql === 'COMMIT' && !phase1Done) {
          phase1Done = true;
          return { rows: [] };
        }

        // Phase 2 queries — track order
        if (phase1Done) {
          if (sql?.includes?.('INSERT INTO markets')) {
            queryOrder.push('markets');
          }
          if (sql?.includes?.('INSERT INTO cbt_assets')) {
            queryOrder.push('cbt_assets');
          }
          if (sql?.includes?.('INSERT INTO lend_positions')) {
            queryOrder.push('lend_positions');
          }
          if (sql?.includes?.('INSERT INTO borrow_positions')) {
            queryOrder.push('borrow_positions');
          }

          // Provide lookups for Phase 2
          if (sql?.includes?.('SELECT id FROM assets')) {
            return { rows: [{ id: 'asset-1' }] };
          }
          if (sql?.includes?.('SELECT id FROM accounts')) {
            return { rows: [{ id: 'account-1' }] };
          }
          if (sql?.includes?.('SELECT asset_id FROM markets')) {
            return { rows: [{ asset_id: 'asset-1' }] };
          }
          if (sql?.includes?.('SELECT id FROM cbt_assets')) {
            return { rows: [{ id: 'cbt-asset-1' }] };
          }
          // settlement_items JOIN matches for updatePortfolioForBatch
          if (sql?.includes?.('FROM settlement_items si')) {
            return { rows: [] };
          }
        }

        return { rows: [] };
      });

      await persistSettlementResults({
        results: [result],
        matchPayloads: new Map([[match.matchId, match]]),
        config,
        maxRetries: 0,
        retryDelayMs: 10,
      });

      // Verify FK ordering: bond token tables before lend, lend before borrow
      const marketsIdx = queryOrder.indexOf('markets');
      const cbtIdx = queryOrder.indexOf('cbt_assets');
      const lendIdx = queryOrder.indexOf('lend_positions');
      const borrowIdx = queryOrder.indexOf('borrow_positions');

      expect(marketsIdx).toBeGreaterThanOrEqual(0);
      expect(cbtIdx).toBeGreaterThan(marketsIdx);
      expect(lendIdx).toBeGreaterThan(cbtIdx);
      expect(borrowIdx).toBeGreaterThan(lendIdx);
    });

    it('should handle empty event arrays gracefully', async () => {
      const match = createMatch();

      const result: SettlementResult = {
        transactionHash: '0xabc123',
        blockNumber: 100,
        gasUsed: 50000,
        timestamp: 1700000000000,
        settledMatchIds: [match.matchId],
        bondTokenEvents: [],
        lendPositionEvents: [],
        borrowPositionEvents: [],
      };

      let phase1Done = false;

      mockQuery.mockImplementation((sql: string) => {
        if (sql?.includes?.('INSERT INTO settlement_batches')) {
          return { rows: [{ id: 'batch-1' }] };
        }
        if (sql === 'COMMIT' && !phase1Done) {
          phase1Done = true;
          return { rows: [] };
        }
        // Phase 2: settlement_items JOIN matches returns empty (no matches to update)
        if (phase1Done && sql?.includes?.('FROM settlement_items si')) {
          return { rows: [] };
        }
        return { rows: [] };
      });

      await expect(
        persistSettlementResults({
          results: [result],
          matchPayloads: new Map([[match.matchId, match]]),
          config,
          maxRetries: 0,
          retryDelayMs: 10,
        }),
      ).resolves.toBeUndefined();

      // No position-related INSERT queries should have been called
      const positionInserts = mockQuery.mock.calls.filter(
        (call) =>
          call[0]?.includes?.('INSERT INTO markets') ||
          call[0]?.includes?.('INSERT INTO cbt_assets') ||
          call[0]?.includes?.('INSERT INTO lend_positions') ||
          call[0]?.includes?.('INSERT INTO borrow_positions'),
      );
      expect(positionInserts.length).toBe(0);
    });

    it('should log warning when Phase 2 fails for missing asset', async () => {
      const match = createMatch();
      const marketIdHex = '0x660e8400e29b41d4a716446655440099000000000000000000000000000000';

      const result: SettlementResult = {
        transactionHash: '0xabc123',
        blockNumber: 100,
        gasUsed: 50000,
        timestamp: 1700000000000,
        settledMatchIds: [match.matchId],
        bondTokenEvents: [
          {
            marketId: marketIdHex,
            bondToken: '0xaaaa',
            loanToken: '0xbbbb',
            maturity: 1735689600n,
            name: 'Bond',
            symbol: 'BT',
          },
        ],
        lendPositionEvents: [],
        borrowPositionEvents: [],
      };

      let phase1Done = false;
      const loggerSpy = jest.spyOn(logger, 'error').mockImplementation(() => {});

      mockQuery.mockImplementation((sql: string) => {
        if (sql?.includes?.('INSERT INTO settlement_batches')) {
          return { rows: [{ id: 'batch-1' }] };
        }
        if (sql === 'COMMIT' && !phase1Done) {
          phase1Done = true;
          return { rows: [] };
        }
        // Phase 2: asset lookup returns empty — will cause persistBondTokenCreated to throw
        if (phase1Done && sql?.includes?.('SELECT id FROM assets')) {
          return { rows: [] };
        }
        return { rows: [] };
      });

      // Should NOT throw — Phase 2 failures are caught
      await expect(
        persistSettlementResults({
          results: [result],
          matchPayloads: new Map([[match.matchId, match]]),
          config,
          maxRetries: 0,
          retryDelayMs: 10,
        }),
      ).resolves.toBeUndefined();

      expect(loggerSpy).toHaveBeenCalledWith(
        expect.objectContaining({ component: 'database' }),
        expect.stringContaining('Phase 2 failed'),
      );

      loggerSpy.mockRestore();
    });
  });

  describe('updatePortfolioForBatch (via persistSettlementResults Phase 2)', () => {
    const config = createTestAppConfig();

    const createResultWithEvents = (matchId: string): SettlementResult => ({
      transactionHash: '0xabc123',
      blockNumber: 100,
      gasUsed: 50000,
      timestamp: 1700000000000,
      settledMatchIds: [matchId],
      bondTokenEvents: [],
      lendPositionEvents: [],
      borrowPositionEvents: [],
    });

    /**
     * Helper to set up Phase 1 + Phase 2 mocks with settlement_items JOIN matches
     * returning the given matchRow data.
     */
    const setupPhase2Mock = (matchRow: Record<string, unknown>) => {
      let phase1Done = false;
      let phase2JoinReturned = false;

      mockQuery.mockImplementation((sql: string) => {
        // Phase 1
        if (sql?.includes?.('INSERT INTO settlement_batches')) {
          return { rows: [{ id: 'batch-1' }] };
        }
        if (sql === 'COMMIT' && !phase1Done) {
          phase1Done = true;
          return { rows: [] };
        }

        // Phase 2
        if (phase1Done) {
          // settlement_items JOIN matches query
          if (sql?.includes?.('FROM settlement_items si') && !phase2JoinReturned) {
            phase2JoinReturned = true;
            return { rows: [matchRow] };
          }
        }

        return { rows: [] };
      });
    };

    it('should update portfolio for lender and borrower', async () => {
      const match = createMatch();
      const result = createResultWithEvents(match.matchId);

      setupPhase2Mock({
        match_amount: '1000000',
        lender_settlement_fee: '100',
        borrower_settlement_fee: '200',
        lender_account_id: 'aaa-lender',
        borrower_account_id: 'zzz-borrower',
        asset_id: 'asset-1',
        is_borrower_taker: true,
        maker_fee: '50',
        taker_fee: '75',
      });

      await persistSettlementResults({
        results: [result],
        matchPayloads: new Map([[match.matchId, match]]),
        config,
        maxRetries: 0,
        retryDelayMs: 10,
      });

      const updatePortfolioCalls = mockQuery.mock.calls.filter((call) =>
        call[0]?.includes?.('UPDATE portfolio'),
      );
      const insertPortfolioCalls = mockQuery.mock.calls.filter((call) =>
        call[0]?.includes?.('INSERT INTO portfolio'),
      );

      // 2 UPDATE portfolio (lender deduct + borrower unlock) + 1 INSERT/upsert for borrower
      expect(updatePortfolioCalls.length).toBe(2);
      expect(insertPortfolioCalls.length).toBe(1);
    });

    it('should order account ID updates for deadlock prevention', async () => {
      const match = createMatch();
      const result = createResultWithEvents(match.matchId);

      // lender_account_id > borrower_account_id alphabetically
      setupPhase2Mock({
        match_amount: '1000000',
        lender_settlement_fee: '100',
        borrower_settlement_fee: '200',
        lender_account_id: 'zzz-lender',
        borrower_account_id: 'aaa-borrower',
        asset_id: 'asset-1',
        is_borrower_taker: true,
        maker_fee: '50',
        taker_fee: '75',
      });

      await persistSettlementResults({
        results: [result],
        matchPayloads: new Map([[match.matchId, match]]),
        config,
        maxRetries: 0,
        retryDelayMs: 10,
      });

      const updatePortfolioCalls = mockQuery.mock.calls.filter((call) =>
        call[0]?.includes?.('UPDATE portfolio'),
      );

      // First UPDATE should be for borrower (aaa-) since it's alphabetically lower
      expect(updatePortfolioCalls.length).toBe(2);
      expect(updatePortfolioCalls[0][1]).toContain('aaa-borrower');
      expect(updatePortfolioCalls[1][1]).toContain('zzz-lender');
    });

    it('should mark matches as SETTLED after portfolio updates', async () => {
      const match = createMatch();
      const result = createResultWithEvents(match.matchId);

      setupPhase2Mock({
        match_amount: '1000000',
        lender_settlement_fee: '100',
        borrower_settlement_fee: '200',
        lender_account_id: 'aaa-lender',
        borrower_account_id: 'zzz-borrower',
        asset_id: 'asset-1',
        is_borrower_taker: true,
        maker_fee: '50',
        taker_fee: '75',
      });

      await persistSettlementResults({
        results: [result],
        matchPayloads: new Map([[match.matchId, match]]),
        config,
        maxRetries: 0,
        retryDelayMs: 10,
      });

      const settledUpdateCalls = mockQuery.mock.calls.filter(
        (call) =>
          call[0]?.includes?.('UPDATE matches') &&
          call[0]?.includes?.("settlement_status = 'SETTLED'"),
      );
      expect(settledUpdateCalls.length).toBe(1);

      // Ensure it happens after UPDATE portfolio calls
      const allCalls = mockQuery.mock.calls.map((call) => call[0]);
      const lastPortfolioUpdateIdx = allCalls.reduce(
        (maxIdx: number, sql: string, idx: number) =>
          sql?.includes?.('UPDATE portfolio') ? idx : maxIdx,
        -1,
      );
      const settledIdx = allCalls.findIndex(
        (sql: string) =>
          sql?.includes?.('UPDATE matches') &&
          sql?.includes?.("settlement_status = 'SETTLED'"),
      );
      expect(settledIdx).toBeGreaterThan(lastPortfolioUpdateIdx);
    });
  });
});
