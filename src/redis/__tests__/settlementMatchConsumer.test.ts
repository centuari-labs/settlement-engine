import {
  ensureConsumerGroup,
  readMatches,
  processPendingEntriesOnStartup,
} from '../settlementMatchConsumer';
import { createMatch } from '../../tests/helpers/testFixtures';

/**
 * Unit tests for settlement match consumer.
 * All Redis interactions are mocked — no live Redis required.
 */

describe('ensureConsumerGroup', () => {
  it('should create a consumer group successfully', async () => {
    const mockRedis = {
      xgroup: jest.fn().mockResolvedValue('OK'),
    } as any;

    const consoleSpy = jest.spyOn(console, 'log').mockImplementation();

    await ensureConsumerGroup(mockRedis, 'test-stream', 'test-group');

    expect(mockRedis.xgroup).toHaveBeenCalledWith(
      'CREATE',
      'test-stream',
      'test-group',
      '0',
      'MKSTREAM',
    );
    expect(consoleSpy).toHaveBeenCalledWith(
      'Created consumer group "test-group" on stream "test-stream"',
    );

    consoleSpy.mockRestore();
  });

  it('should handle BUSYGROUP error gracefully when group already exists', async () => {
    const mockRedis = {
      xgroup: jest.fn().mockRejectedValue(new Error('BUSYGROUP Consumer Group name already exists')),
    } as any;

    await expect(
      ensureConsumerGroup(mockRedis, 'test-stream', 'test-group'),
    ).resolves.not.toThrow();
  });

  it('should propagate other errors', async () => {
    const mockRedis = {
      xgroup: jest.fn().mockRejectedValue(new Error('ERR some other error')),
    } as any;

    await expect(
      ensureConsumerGroup(mockRedis, 'test-stream', 'test-group'),
    ).rejects.toThrow('ERR some other error');
  });

  it('should handle non-Error thrown values', async () => {
    const mockRedis = {
      xgroup: jest.fn().mockRejectedValue('string error'),
    } as any;

    await expect(
      ensureConsumerGroup(mockRedis, 'test-stream', 'test-group'),
    ).rejects.toBe('string error');
  });
});

describe('readMatches', () => {
  let onInvalid: jest.Mock;

  beforeEach(() => {
    onInvalid = jest.fn().mockResolvedValue(undefined);
  });

  it('should read valid matches with JSON data field', async () => {
    const match = createMatch();
    const mockRedis = {
      xreadgroup: jest.fn().mockResolvedValue([
        ['test-stream', [['entry-1', ['data', JSON.stringify(match)]]]],
      ]),
      xack: jest.fn().mockResolvedValue(1),
    } as any;

    const matches = await readMatches({
      redis: mockRedis,
      stream: 'test-stream',
      consumerGroup: 'test-group',
      consumerName: 'test-consumer',
      readCount: 10,
    });

    expect(matches).toHaveLength(1);
    expect(matches[0]).toEqual({
      id: 'entry-1',
      stream: 'test-stream',
      payload: match,
    });
  });

  it('should read valid matches with individual fields', async () => {
    const match = createMatch();
    const fields = [
      'matchId', match.matchId,
      'lendOrderId', match.lendOrderId,
      'borrowOrderId', match.borrowOrderId,
      'lenderWallet', match.lenderWallet,
      'borrowerWallet', match.borrowerWallet,
      'matchedAmount', match.matchedAmount,
      'rate', String(match.rate),
      'loanToken', match.loanToken,
      'maturity', String(match.maturity),
      'timestamp', String(match.timestamp),
      'borrowerIsTaker', String(match.borrowerIsTaker),
      'makerFeeAmount', match.makerFeeAmount,
      'takerFeeAmount', match.takerFeeAmount,
      'lenderSettlementFee', match.lenderSettlementFee,
      'borrowerSettlementFee', match.borrowerSettlementFee,
    ];

    const mockRedis = {
      xreadgroup: jest.fn().mockResolvedValue([
        ['test-stream', [['entry-1', fields]]],
      ]),
      xack: jest.fn().mockResolvedValue(1),
    } as any;

    const matches = await readMatches({
      redis: mockRedis,
      stream: 'test-stream',
      consumerGroup: 'test-group',
      consumerName: 'test-consumer',
      readCount: 10,
    });

    expect(matches).toHaveLength(1);
    expect(matches[0]?.payload).toEqual(match);
  });

  it('should handle invalid JSON in data field', async () => {
    const mockRedis = {
      xreadgroup: jest.fn().mockResolvedValue([
        ['test-stream', [['entry-1', ['data', 'invalid json {']]]],
      ]),
      xack: jest.fn().mockResolvedValue(1),
    } as any;

    const matches = await readMatches({
      redis: mockRedis,
      stream: 'test-stream',
      consumerGroup: 'test-group',
      consumerName: 'test-consumer',
      readCount: 10,
      onInvalid,
    });

    expect(matches).toHaveLength(0);
    expect(onInvalid).toHaveBeenCalledTimes(1);
    expect(onInvalid).toHaveBeenCalledWith(
      expect.objectContaining({
        id: 'entry-1',
        stream: 'test-stream',
        error: expect.any(Error),
      }),
    );
    // Invalid entries should be ACKed immediately
    expect(mockRedis.xack).toHaveBeenCalledWith('test-stream', 'test-group', 'entry-1');
  });

  it('should handle invalid schema matches', async () => {
    const invalidMatch = {
      matchId: 'invalid', // Not a UUID
      lendOrderId: '550e8400-e29b-41d4-a716-446655440001',
    };

    const mockRedis = {
      xreadgroup: jest.fn().mockResolvedValue([
        ['test-stream', [['entry-1', ['data', JSON.stringify(invalidMatch)]]]],
      ]),
      xack: jest.fn().mockResolvedValue(1),
    } as any;

    const matches = await readMatches({
      redis: mockRedis,
      stream: 'test-stream',
      consumerGroup: 'test-group',
      consumerName: 'test-consumer',
      readCount: 10,
      onInvalid,
    });

    expect(matches).toHaveLength(0);
    expect(onInvalid).toHaveBeenCalledTimes(1);
    expect(mockRedis.xack).toHaveBeenCalledWith('test-stream', 'test-group', 'entry-1');
  });

  it('should log to console.error when onInvalid handler is not provided', async () => {
    const consoleSpy = jest.spyOn(console, 'error').mockImplementation();

    const mockRedis = {
      xreadgroup: jest.fn().mockResolvedValue([
        ['test-stream', [['entry-1', ['data', JSON.stringify({ invalid: 'data' })]]]],
      ]),
      xack: jest.fn().mockResolvedValue(1),
    } as any;

    const matches = await readMatches({
      redis: mockRedis,
      stream: 'test-stream',
      consumerGroup: 'test-group',
      consumerName: 'test-consumer',
      readCount: 10,
    });

    expect(matches).toHaveLength(0);
    expect(consoleSpy).toHaveBeenCalledWith(
      '[settlement-consumer] Invalid match entry',
      expect.any(String),
    );

    consoleSpy.mockRestore();
  });

  it('should return empty array when stream is empty (null result)', async () => {
    const mockRedis = {
      xreadgroup: jest.fn().mockResolvedValue(null),
    } as any;

    const matches = await readMatches({
      redis: mockRedis,
      stream: 'test-stream',
      consumerGroup: 'test-group',
      consumerName: 'test-consumer',
      readCount: 10,
    });

    expect(matches).toHaveLength(0);
  });

  it('should return empty array when stream result is empty array', async () => {
    const mockRedis = {
      xreadgroup: jest.fn().mockResolvedValue([]),
    } as any;

    const matches = await readMatches({
      redis: mockRedis,
      stream: 'test-stream',
      consumerGroup: 'test-group',
      consumerName: 'test-consumer',
      readCount: 10,
    });

    expect(matches).toHaveLength(0);
  });

  it('should read multiple matches', async () => {
    const match1 = createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440001' });
    const match2 = createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440002' });

    const mockRedis = {
      xreadgroup: jest.fn().mockResolvedValue([
        [
          'test-stream',
          [
            ['entry-1', ['data', JSON.stringify(match1)]],
            ['entry-2', ['data', JSON.stringify(match2)]],
          ],
        ],
      ]),
      xack: jest.fn().mockResolvedValue(1),
    } as any;

    const matches = await readMatches({
      redis: mockRedis,
      stream: 'test-stream',
      consumerGroup: 'test-group',
      consumerName: 'test-consumer',
      readCount: 10,
    });

    expect(matches).toHaveLength(2);
    expect(matches[0]?.id).toBe('entry-1');
    expect(matches[1]?.id).toBe('entry-2');
  });

  it('should handle errors gracefully and return empty array', async () => {
    const consoleSpy = jest.spyOn(console, 'error').mockImplementation();

    const mockRedis = {
      xreadgroup: jest.fn().mockRejectedValue(new Error('Connection lost')),
    } as any;

    const matches = await readMatches({
      redis: mockRedis,
      stream: 'test-stream',
      consumerGroup: 'test-group',
      consumerName: 'test-consumer',
      readCount: 10,
    });

    expect(matches).toHaveLength(0);
    expect(consoleSpy).toHaveBeenCalledWith(
      '[settlement-consumer] Error reading matches',
      expect.any(Error),
    );

    consoleSpy.mockRestore();
  });

  it('should pass correct arguments to xreadgroup', async () => {
    const mockRedis = {
      xreadgroup: jest.fn().mockResolvedValue(null),
    } as any;

    await readMatches({
      redis: mockRedis,
      stream: 'my-stream',
      consumerGroup: 'my-group',
      consumerName: 'my-consumer',
      readCount: 42,
    });

    expect(mockRedis.xreadgroup).toHaveBeenCalledWith(
      'GROUP',
      'my-group',
      'my-consumer',
      'COUNT',
      42,
      'STREAMS',
      'my-stream',
      '>',
    );
  });

  it('should mix valid and invalid entries, keeping valid ones', async () => {
    const validMatch = createMatch({ matchId: '550e8400-e29b-41d4-a716-446655440001' });

    const mockRedis = {
      xreadgroup: jest.fn().mockResolvedValue([
        [
          'test-stream',
          [
            ['entry-1', ['data', JSON.stringify(validMatch)]],
            ['entry-2', ['data', JSON.stringify({ invalid: true })]],
            ['entry-3', ['data', 'not json']],
          ],
        ],
      ]),
      xack: jest.fn().mockResolvedValue(1),
    } as any;

    const matches = await readMatches({
      redis: mockRedis,
      stream: 'test-stream',
      consumerGroup: 'test-group',
      consumerName: 'test-consumer',
      readCount: 10,
      onInvalid,
    });

    expect(matches).toHaveLength(1);
    expect(matches[0]?.payload.matchId).toBe('550e8400-e29b-41d4-a716-446655440001');
    // Invalid entries ACKed
    expect(mockRedis.xack).toHaveBeenCalledWith('test-stream', 'test-group', 'entry-2');
    expect(mockRedis.xack).toHaveBeenCalledWith('test-stream', 'test-group', 'entry-3');
    // Valid entry NOT ACKed (ACK happens after batch processing)
    expect(mockRedis.xack).not.toHaveBeenCalledWith('test-stream', 'test-group', 'entry-1');
  });
});

describe('processPendingEntriesOnStartup', () => {
  it('should process pending entries from current consumer', async () => {
    const match = createMatch();

    const mockRedis = {
      xreadgroup: jest.fn()
        // First call: pending entries for current consumer (using '0')
        .mockResolvedValueOnce([
          ['test-stream', [['entry-1', ['data', JSON.stringify(match)]]]],
        ])
        // Second call: no more pending entries
        .mockResolvedValueOnce([['test-stream', []]])
        // Third call is for XPENDING (not xreadgroup)
        ,
      xack: jest.fn().mockResolvedValue(1),
      xpending: jest.fn().mockResolvedValue([0, null, null, null]),
    } as any;

    const matches = await processPendingEntriesOnStartup({
      redis: mockRedis,
      stream: 'test-stream',
      consumerGroup: 'test-group',
      consumerName: 'test-consumer',
      readCount: 10,
    });

    expect(matches).toHaveLength(1);
    expect(matches[0]?.payload).toEqual(match);
  });

  it('should return empty array when no pending entries', async () => {
    const mockRedis = {
      xreadgroup: jest.fn().mockResolvedValue(null),
      xpending: jest.fn().mockResolvedValue([0, null, null, null]),
    } as any;

    const matches = await processPendingEntriesOnStartup({
      redis: mockRedis,
      stream: 'test-stream',
      consumerGroup: 'test-group',
      consumerName: 'test-consumer',
      readCount: 10,
    });

    expect(matches).toHaveLength(0);
  });

  it('should handle errors during pending entry processing', async () => {
    const consoleSpy = jest.spyOn(console, 'error').mockImplementation();

    const mockRedis = {
      xreadgroup: jest.fn().mockRejectedValue(new Error('Redis error')),
      xpending: jest.fn().mockResolvedValue([0, null, null, null]),
    } as any;

    const matches = await processPendingEntriesOnStartup({
      redis: mockRedis,
      stream: 'test-stream',
      consumerGroup: 'test-group',
      consumerName: 'test-consumer',
      readCount: 10,
    });

    expect(matches).toHaveLength(0);
    consoleSpy.mockRestore();
  });

  it('should claim stale entries from other consumers', async () => {
    const match = createMatch();

    const mockRedis = {
      // First: no pending for current consumer
      xreadgroup: jest.fn().mockResolvedValue(null),
      // XPENDING summary: 1 pending entry
      xpending: jest.fn()
        .mockResolvedValueOnce([1, 'entry-1', 'entry-1', [['other-consumer', '1']]])
        // Detailed pending: one entry from other-consumer
        .mockResolvedValueOnce([['entry-1', 'other-consumer', 5000, 1]])
        // After claim, no more pending
        .mockResolvedValueOnce([0, null, null, null]),
      xclaim: jest.fn().mockResolvedValue([
        ['entry-1', ['data', JSON.stringify(match)]],
      ]),
      xack: jest.fn().mockResolvedValue(1),
    } as any;

    const matches = await processPendingEntriesOnStartup({
      redis: mockRedis,
      stream: 'test-stream',
      consumerGroup: 'test-group',
      consumerName: 'test-consumer',
      readCount: 10,
    });

    expect(matches).toHaveLength(1);
    expect(mockRedis.xclaim).toHaveBeenCalledWith(
      'test-stream',
      'test-group',
      'test-consumer',
      0,
      'entry-1',
    );
  });

  it('should ACK and skip invalid entries during pending processing', async () => {
    const mockRedis = {
      xreadgroup: jest.fn().mockResolvedValueOnce([
        ['test-stream', [['entry-1', ['data', 'not valid json']]]],
      ]).mockResolvedValueOnce([['test-stream', []]]),
      xack: jest.fn().mockResolvedValue(1),
      xpending: jest.fn().mockResolvedValue([0, null, null, null]),
    } as any;

    const onInvalid = jest.fn().mockResolvedValue(undefined);

    const matches = await processPendingEntriesOnStartup({
      redis: mockRedis,
      stream: 'test-stream',
      consumerGroup: 'test-group',
      consumerName: 'test-consumer',
      readCount: 10,
      onInvalid,
    });

    expect(matches).toHaveLength(0);
    expect(mockRedis.xack).toHaveBeenCalledWith('test-stream', 'test-group', 'entry-1');
  });
});
