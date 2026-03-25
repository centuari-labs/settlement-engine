import { settlementBatchStatusSchema } from '../database';
import type { SettlementResult } from '../smartContract';

/**
 * Unit tests for database module.
 */
describe('settlementBatchStatusSchema', () => {
  it('should accept COMPLETED status', () => {
    const result = settlementBatchStatusSchema.safeParse('COMPLETED');
    expect(result.success).toBe(true);
  });

  it('should accept FAILED status', () => {
    const result = settlementBatchStatusSchema.safeParse('FAILED');
    expect(result.success).toBe(true);
  });

  it('should reject unknown status', () => {
    const result = settlementBatchStatusSchema.safeParse('PENDING');
    expect(result.success).toBe(false);
  });

  it('should reject empty string', () => {
    const result = settlementBatchStatusSchema.safeParse('');
    expect(result.success).toBe(false);
  });

  it('should reject lowercase', () => {
    const result = settlementBatchStatusSchema.safeParse('completed');
    expect(result.success).toBe(false);
  });
});

describe('persistSettlementResults', () => {
  let originalDatabaseUrl: string | undefined;

  const createResult = (overrides?: Partial<SettlementResult>): SettlementResult => ({
    transactionHash: '0xabc123',
    blockNumber: 12345,
    gasUsed: 50000,
    timestamp: Date.now(),
    settledMatchIds: ['match-1'],
    ...overrides,
  });

  beforeEach(() => {
    originalDatabaseUrl = process.env.DATABASE_URL;
    jest.resetModules();
  });

  afterEach(() => {
    if (originalDatabaseUrl !== undefined) {
      process.env.DATABASE_URL = originalDatabaseUrl;
    } else {
      delete process.env.DATABASE_URL;
    }
  });

  it('should return early for empty results array', async () => {
    // No pg mock needed - empty results returns before accessing pool
    jest.mock('../smartContract');
    const { persistSettlementResults } = require('../database');

    await persistSettlementResults({ results: [] });
    // If it didn't throw, it returned early (no DB access)
  });

  it('should persist settlement results with transaction', async () => {
    const mockQuery = jest.fn()
      .mockResolvedValueOnce(undefined) // BEGIN
      .mockResolvedValueOnce({ rows: [{ id: 'batch-1' }] }) // INSERT settlement_batches
      .mockResolvedValueOnce(undefined) // INSERT settlement_items (match1)
      .mockResolvedValueOnce(undefined) // INSERT settlement_items (match2)
      .mockResolvedValueOnce(undefined); // COMMIT
    const mockRelease = jest.fn();
    const mockConnect = jest.fn().mockResolvedValue({
      query: mockQuery,
      release: mockRelease,
    });

    jest.mock('pg', () => ({
      Pool: jest.fn().mockImplementation(() => ({ connect: mockConnect })),
    }));
    jest.mock('../smartContract');
    process.env.DATABASE_URL = 'postgresql://test:test@localhost:5432/testdb';

    const { persistSettlementResults } = require('../database');

    const result = createResult({ settledMatchIds: ['match-1', 'match-2'] });
    await persistSettlementResults({ results: [result] });

    expect(mockConnect).toHaveBeenCalled();
    expect(mockQuery).toHaveBeenCalledWith('BEGIN');
    expect(mockQuery).toHaveBeenCalledWith(
      expect.stringContaining('INSERT INTO settlement_batches'),
      ['0xabc123', 'COMPLETED'],
    );
    expect(mockQuery).toHaveBeenCalledWith(
      expect.stringContaining('INSERT INTO settlement_items'),
      ['batch-1', 'match-1'],
    );
    expect(mockQuery).toHaveBeenCalledWith(
      expect.stringContaining('INSERT INTO settlement_items'),
      ['batch-1', 'match-2'],
    );
    expect(mockQuery).toHaveBeenCalledWith('COMMIT');
    expect(mockRelease).toHaveBeenCalled();
  });

  it('should rollback on unique_violation error (non-retryable)', async () => {
    const mockQuery = jest.fn()
      .mockResolvedValueOnce(undefined) // BEGIN
      .mockRejectedValueOnce({ message: 'unique_violation', code: '23505' }) // INSERT fails
      .mockResolvedValueOnce(undefined); // ROLLBACK
    const mockRelease = jest.fn();
    const mockConnect = jest.fn().mockResolvedValue({
      query: mockQuery,
      release: mockRelease,
    });

    jest.mock('pg', () => ({
      Pool: jest.fn().mockImplementation(() => ({ connect: mockConnect })),
    }));
    jest.mock('../smartContract');
    process.env.DATABASE_URL = 'postgresql://test:test@localhost:5432/testdb';

    const { persistSettlementResults } = require('../database');

    await expect(
      persistSettlementResults({ results: [createResult()] }),
    ).rejects.toEqual(
      expect.objectContaining({
        message: 'unique_violation',
        code: '23505',
        retryable: false,
      }),
    );

    expect(mockQuery).toHaveBeenCalledWith('ROLLBACK');
    expect(mockRelease).toHaveBeenCalled();
  });

  it('should classify serialization_failure as retryable', async () => {
    const mockQuery = jest.fn()
      .mockResolvedValueOnce(undefined) // BEGIN
      .mockRejectedValueOnce({ message: 'serialization failure', code: '40001' })
      .mockResolvedValueOnce(undefined); // ROLLBACK
    const mockRelease = jest.fn();
    const mockConnect = jest.fn().mockResolvedValue({
      query: mockQuery,
      release: mockRelease,
    });

    jest.mock('pg', () => ({
      Pool: jest.fn().mockImplementation(() => ({ connect: mockConnect })),
    }));
    jest.mock('../smartContract');
    process.env.DATABASE_URL = 'postgresql://test:test@localhost:5432/testdb';

    const { persistSettlementResults } = require('../database');

    await expect(
      persistSettlementResults({ results: [createResult()], maxRetries: 0 }),
    ).rejects.toEqual(
      expect.objectContaining({
        code: '40001',
        retryable: true,
      }),
    );
  });

  it('should classify foreign_key_violation as non-retryable', async () => {
    const mockQuery = jest.fn()
      .mockResolvedValueOnce(undefined) // BEGIN
      .mockRejectedValueOnce({ message: 'foreign key violation', code: '23503' })
      .mockResolvedValueOnce(undefined); // ROLLBACK
    const mockRelease = jest.fn();
    const mockConnect = jest.fn().mockResolvedValue({
      query: mockQuery,
      release: mockRelease,
    });

    jest.mock('pg', () => ({
      Pool: jest.fn().mockImplementation(() => ({ connect: mockConnect })),
    }));
    jest.mock('../smartContract');
    process.env.DATABASE_URL = 'postgresql://test:test@localhost:5432/testdb';

    const { persistSettlementResults } = require('../database');

    await expect(
      persistSettlementResults({ results: [createResult()] }),
    ).rejects.toEqual(
      expect.objectContaining({
        code: '23503',
        retryable: false,
      }),
    );
  });

  it('should handle batch insert returning empty rows', async () => {
    const mockQuery = jest.fn()
      .mockResolvedValueOnce(undefined) // BEGIN
      .mockResolvedValueOnce({ rows: [] }) // INSERT returns no rows
      .mockResolvedValueOnce(undefined); // ROLLBACK
    const mockRelease = jest.fn();
    const mockConnect = jest.fn().mockResolvedValue({
      query: mockQuery,
      release: mockRelease,
    });

    jest.mock('pg', () => ({
      Pool: jest.fn().mockImplementation(() => ({ connect: mockConnect })),
    }));
    jest.mock('../smartContract');
    process.env.DATABASE_URL = 'postgresql://test:test@localhost:5432/testdb';

    const { persistSettlementResults } = require('../database');

    await expect(
      persistSettlementResults({ results: [createResult()], maxRetries: 0 }),
    ).rejects.toEqual(
      expect.objectContaining({
        message: 'Failed to insert settlement batch',
      }),
    );
  });

  it('should classify deadlock as retryable', async () => {
    const mockQuery = jest.fn()
      .mockResolvedValueOnce(undefined) // BEGIN
      .mockRejectedValueOnce({ message: 'deadlock detected', code: '40P01' })
      .mockResolvedValueOnce(undefined); // ROLLBACK
    const mockRelease = jest.fn();
    const mockConnect = jest.fn().mockResolvedValue({
      query: mockQuery,
      release: mockRelease,
    });

    jest.mock('pg', () => ({
      Pool: jest.fn().mockImplementation(() => ({ connect: mockConnect })),
    }));
    jest.mock('../smartContract');
    process.env.DATABASE_URL = 'postgresql://test:test@localhost:5432/testdb';

    const { persistSettlementResults } = require('../database');

    await expect(
      persistSettlementResults({ results: [createResult()], maxRetries: 0 }),
    ).rejects.toEqual(
      expect.objectContaining({
        code: '40P01',
        retryable: true,
      }),
    );
  });

  it('should classify not_null_violation as non-retryable', async () => {
    const mockQuery = jest.fn()
      .mockResolvedValueOnce(undefined) // BEGIN
      .mockRejectedValueOnce({ message: 'not null violation', code: '23502' })
      .mockResolvedValueOnce(undefined); // ROLLBACK
    const mockRelease = jest.fn();
    const mockConnect = jest.fn().mockResolvedValue({
      query: mockQuery,
      release: mockRelease,
    });

    jest.mock('pg', () => ({
      Pool: jest.fn().mockImplementation(() => ({ connect: mockConnect })),
    }));
    jest.mock('../smartContract');
    process.env.DATABASE_URL = 'postgresql://test:test@localhost:5432/testdb';

    const { persistSettlementResults } = require('../database');

    await expect(
      persistSettlementResults({ results: [createResult()] }),
    ).rejects.toEqual(
      expect.objectContaining({
        code: '23502',
        retryable: false,
      }),
    );
  });

  it('should classify unknown error code as retryable', async () => {
    const mockQuery = jest.fn()
      .mockResolvedValueOnce(undefined) // BEGIN
      .mockRejectedValueOnce({ message: 'unknown error', code: '99999' })
      .mockResolvedValueOnce(undefined); // ROLLBACK
    const mockRelease = jest.fn();
    const mockConnect = jest.fn().mockResolvedValue({
      query: mockQuery,
      release: mockRelease,
    });

    jest.mock('pg', () => ({
      Pool: jest.fn().mockImplementation(() => ({ connect: mockConnect })),
    }));
    jest.mock('../smartContract');
    process.env.DATABASE_URL = 'postgresql://test:test@localhost:5432/testdb';

    const { persistSettlementResults } = require('../database');

    await expect(
      persistSettlementResults({ results: [createResult()], maxRetries: 0 }),
    ).rejects.toEqual(
      expect.objectContaining({
        code: '99999',
        retryable: true,
      }),
    );
  });

  it('should handle error without code', async () => {
    const mockQuery = jest.fn()
      .mockResolvedValueOnce(undefined) // BEGIN
      .mockRejectedValueOnce({ message: 'generic error' })
      .mockResolvedValueOnce(undefined); // ROLLBACK
    const mockRelease = jest.fn();
    const mockConnect = jest.fn().mockResolvedValue({
      query: mockQuery,
      release: mockRelease,
    });

    jest.mock('pg', () => ({
      Pool: jest.fn().mockImplementation(() => ({ connect: mockConnect })),
    }));
    jest.mock('../smartContract');
    process.env.DATABASE_URL = 'postgresql://test:test@localhost:5432/testdb';

    const { persistSettlementResults } = require('../database');

    await expect(
      persistSettlementResults({ results: [createResult()], maxRetries: 0 }),
    ).rejects.toEqual(
      expect.objectContaining({
        message: 'generic error',
        retryable: false,
      }),
    );
  });
});
