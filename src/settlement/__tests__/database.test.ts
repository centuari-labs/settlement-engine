/**
 * Unit tests for database.ts.
 *
 * Tests the exported persistSettlementResults function and internal logic
 * (error mapping, retry, transaction handling) by mocking the pg module.
 */

import type { SettlementResult } from '../smartContract';

// Mock pg module before importing database
jest.mock('pg', () => {
  const mockClient = {
    query: jest.fn(),
    release: jest.fn(),
  };
  const mockPool = {
    connect: jest.fn().mockResolvedValue(mockClient),
  };
  return {
    Pool: jest.fn(() => mockPool),
    __mockPool: mockPool,
    __mockClient: mockClient,
  };
});

// eslint-disable-next-line @typescript-eslint/no-require-imports
const pgMocks = require('pg') as {
  __mockPool: { connect: jest.Mock };
  __mockClient: { query: jest.Mock; release: jest.Mock };
};

// We need the actual database module (not auto-mocked)
const actualDb = jest.requireActual('../database') as typeof import('../database');

describe('persistSettlementResults', () => {
  const mockClient = pgMocks.__mockClient;

  const createResult = (overrides?: Partial<SettlementResult>): SettlementResult => ({
    transactionHash: '0xabc123',
    blockNumber: 100,
    gasUsed: 50000,
    timestamp: Date.now(),
    settledMatchIds: ['550e8400-e29b-41d4-a716-446655440000'],
    ...overrides,
  });

  beforeEach(() => {
    jest.clearAllMocks();
    // Set DATABASE_URL so getPool() doesn't throw
    process.env.DATABASE_URL = 'postgresql://localhost:5432/test';

    // Default: successful transaction
    mockClient.query.mockImplementation((sql: string) => {
      if (sql.includes('INSERT INTO settlement_batches')) {
        return { rows: [{ id: 'batch-1' }] };
      }
      return { rows: [] };
    });
  });

  it('should return immediately for empty results array', async () => {
    await actualDb.persistSettlementResults({ results: [] });
    expect(mockClient.query).not.toHaveBeenCalled();
  });

  it('should persist a single result within a transaction', async () => {
    const result = createResult();

    await actualDb.persistSettlementResults({ results: [result] });

    // Expect BEGIN, batch INSERT, item INSERT, COMMIT
    const calls = mockClient.query.mock.calls.map(
      (c: [string, ...unknown[]]) => c[0],
    );
    expect(calls[0]).toBe('BEGIN');
    expect(calls[1]).toContain('INSERT INTO settlement_batches');
    expect(calls[2]).toContain('INSERT INTO settlement_items');
    expect(calls[3]).toBe('COMMIT');
    expect(mockClient.release).toHaveBeenCalled();
  });

  it('should insert one item per settled match ID', async () => {
    const result = createResult({
      settledMatchIds: ['id-1', 'id-2', 'id-3'],
    });

    await actualDb.persistSettlementResults({ results: [result] });

    // BEGIN + 1 batch INSERT + 3 item INSERTs + COMMIT = 6 calls
    expect(mockClient.query).toHaveBeenCalledTimes(6);
  });

  it('should persist multiple results in a single transaction', async () => {
    const results = [
      createResult({ transactionHash: '0x111' }),
      createResult({ transactionHash: '0x222' }),
    ];

    await actualDb.persistSettlementResults({ results });

    const calls = mockClient.query.mock.calls.map(
      (c: [string, ...unknown[]]) => c[0],
    );
    expect(calls[0]).toBe('BEGIN');
    // 2 batch inserts + 2 item inserts
    expect(
      calls.filter((c: string) => c.includes('INSERT INTO settlement_batches')).length,
    ).toBe(2);
    expect(calls[calls.length - 1]).toBe('COMMIT');
  });

  it('should ROLLBACK on batch insert failure', async () => {
    mockClient.query.mockImplementation((sql: string) => {
      if (sql.includes('INSERT INTO settlement_batches')) {
        return { rows: [] }; // No rows returned = failure
      }
      return { rows: [] };
    });

    await expect(
      actualDb.persistSettlementResults({
        results: [createResult()],
        maxRetries: 0,
      }),
    ).rejects.toBeDefined();

    const calls = mockClient.query.mock.calls.map(
      (c: [string, ...unknown[]]) => c[0],
    );
    expect(calls).toContain('ROLLBACK');
    expect(mockClient.release).toHaveBeenCalled();
  });

  it('should throw non-retryable error for unique_violation (23505)', async () => {
    mockClient.query.mockImplementation((sql: string) => {
      if (sql === 'BEGIN') return { rows: [] };
      const err = new Error('duplicate key value') as Error & { code: string };
      err.code = '23505';
      throw err;
    });

    try {
      await actualDb.persistSettlementResults({
        results: [createResult()],
        maxRetries: 3,
      });
      fail('Expected error');
    } catch (error) {
      const err = error as { retryable: boolean; code: string };
      expect(err.retryable).toBe(false);
      expect(err.code).toBe('23505');
    }
  });

  it('should throw non-retryable error for foreign_key_violation (23503)', async () => {
    mockClient.query.mockImplementation((sql: string) => {
      if (sql === 'BEGIN') return { rows: [] };
      const err = new Error('foreign key violation') as Error & { code: string };
      err.code = '23503';
      throw err;
    });

    try {
      await actualDb.persistSettlementResults({
        results: [createResult()],
        maxRetries: 3,
      });
      fail('Expected error');
    } catch (error) {
      const err = error as { retryable: boolean; code: string };
      expect(err.retryable).toBe(false);
      expect(err.code).toBe('23503');
    }
  });

  it('should throw non-retryable error for not_null_violation (23502)', async () => {
    mockClient.query.mockImplementation((sql: string) => {
      if (sql === 'BEGIN') return { rows: [] };
      const err = new Error('not null violation') as Error & { code: string };
      err.code = '23502';
      throw err;
    });

    try {
      await actualDb.persistSettlementResults({
        results: [createResult()],
        maxRetries: 0,
      });
      fail('Expected error');
    } catch (error) {
      const err = error as { retryable: boolean };
      expect(err.retryable).toBe(false);
    }
  });

  it('should throw non-retryable error for check_violation (23514)', async () => {
    mockClient.query.mockImplementation((sql: string) => {
      if (sql === 'BEGIN') return { rows: [] };
      const err = new Error('check violation') as Error & { code: string };
      err.code = '23514';
      throw err;
    });

    try {
      await actualDb.persistSettlementResults({
        results: [createResult()],
        maxRetries: 0,
      });
      fail('Expected error');
    } catch (error) {
      const err = error as { retryable: boolean };
      expect(err.retryable).toBe(false);
    }
  });

  it('should throw non-retryable error for invalid_text_representation (22P02)', async () => {
    mockClient.query.mockImplementation((sql: string) => {
      if (sql === 'BEGIN') return { rows: [] };
      const err = new Error('invalid text') as Error & { code: string };
      err.code = '22P02';
      throw err;
    });

    try {
      await actualDb.persistSettlementResults({
        results: [createResult()],
        maxRetries: 0,
      });
      fail('Expected error');
    } catch (error) {
      const err = error as { retryable: boolean };
      expect(err.retryable).toBe(false);
    }
  });

  it('should retry on serialization_failure (40001) and then succeed', async () => {
    let attempt = 0;
    mockClient.query.mockImplementation((sql: string) => {
      if (sql === 'BEGIN') return { rows: [] };
      if (sql === 'COMMIT') return { rows: [] };
      if (sql === 'ROLLBACK') return { rows: [] };
      if (sql.includes('INSERT INTO settlement_batches')) {
        attempt++;
        if (attempt === 1) {
          const err = new Error('serialization failure') as Error & { code: string };
          err.code = '40001';
          throw err;
        }
        return { rows: [{ id: 'batch-1' }] };
      }
      return { rows: [] };
    });

    await actualDb.persistSettlementResults({
      results: [createResult()],
      maxRetries: 3,
      retryDelayMs: 1, // fast retries for tests
    });

    // Should have been called multiple times (retry)
    expect(attempt).toBe(2);
  });

  it('should retry on deadlock_detected (40P01)', async () => {
    let attempt = 0;
    mockClient.query.mockImplementation((sql: string) => {
      if (sql === 'BEGIN') return { rows: [] };
      if (sql === 'COMMIT') return { rows: [] };
      if (sql === 'ROLLBACK') return { rows: [] };
      if (sql.includes('INSERT INTO settlement_batches')) {
        attempt++;
        if (attempt <= 2) {
          const err = new Error('deadlock') as Error & { code: string };
          err.code = '40P01';
          throw err;
        }
        return { rows: [{ id: 'batch-1' }] };
      }
      return { rows: [] };
    });

    await actualDb.persistSettlementResults({
      results: [createResult()],
      maxRetries: 3,
      retryDelayMs: 1,
    });

    expect(attempt).toBe(3);
  });

  it('should retry on connection_failure (08006)', async () => {
    let attempt = 0;
    mockClient.query.mockImplementation((sql: string) => {
      if (sql === 'BEGIN') return { rows: [] };
      if (sql === 'COMMIT') return { rows: [] };
      if (sql === 'ROLLBACK') return { rows: [] };
      if (sql.includes('INSERT INTO settlement_batches')) {
        attempt++;
        if (attempt === 1) {
          const err = new Error('connection failure') as Error & { code: string };
          err.code = '08006';
          throw err;
        }
        return { rows: [{ id: 'batch-1' }] };
      }
      return { rows: [] };
    });

    await actualDb.persistSettlementResults({
      results: [createResult()],
      maxRetries: 3,
      retryDelayMs: 1,
    });

    expect(attempt).toBe(2);
  });

  it('should exhaust retries and throw for persistent retryable errors', async () => {
    mockClient.query.mockImplementation((sql: string) => {
      if (sql === 'BEGIN') return { rows: [] };
      if (sql === 'ROLLBACK') return { rows: [] };
      const err = new Error('deadlock') as Error & { code: string };
      err.code = '40P01';
      throw err;
    });

    try {
      await actualDb.persistSettlementResults({
        results: [createResult()],
        maxRetries: 2,
        retryDelayMs: 1,
      });
      fail('Expected error');
    } catch (error) {
      const err = error as { retryable: boolean; code: string };
      expect(err.retryable).toBe(true);
      expect(err.code).toBe('40P01');
    }
  });

  it('should treat unknown error codes as retryable', async () => {
    mockClient.query.mockImplementation((sql: string) => {
      if (sql === 'BEGIN') return { rows: [] };
      if (sql === 'ROLLBACK') return { rows: [] };
      const err = new Error('unknown pg error') as Error & { code: string };
      err.code = '99999';
      throw err;
    });

    try {
      await actualDb.persistSettlementResults({
        results: [createResult()],
        maxRetries: 0,
        retryDelayMs: 1,
      });
      fail('Expected error');
    } catch (error) {
      const err = error as { retryable: boolean };
      // Unknown codes are retryable by default
      expect(err.retryable).toBe(true);
    }
  });

  it('should treat errors without code as non-retryable', async () => {
    mockClient.query.mockImplementation((sql: string) => {
      if (sql === 'BEGIN') return { rows: [] };
      if (sql === 'ROLLBACK') return { rows: [] };
      throw new Error('generic error with no code');
    });

    try {
      await actualDb.persistSettlementResults({
        results: [createResult()],
        maxRetries: 3,
        retryDelayMs: 1,
      });
      fail('Expected error');
    } catch (error) {
      const err = error as { retryable: boolean; code?: string };
      // No code → retryable is false (the code path: code is undefined, so neither set is checked)
      expect(err.retryable).toBe(false);
    }
  });
});

describe('settlementBatchStatusSchema', () => {
  it('should accept COMPLETED', () => {
    expect(actualDb.settlementBatchStatusSchema.parse('COMPLETED')).toBe('COMPLETED');
  });

  it('should accept FAILED', () => {
    expect(actualDb.settlementBatchStatusSchema.parse('FAILED')).toBe('FAILED');
  });

  it('should reject invalid status', () => {
    expect(() => actualDb.settlementBatchStatusSchema.parse('PENDING')).toThrow();
  });

  it('should reject empty string', () => {
    expect(() => actualDb.settlementBatchStatusSchema.parse('')).toThrow();
  });
});
