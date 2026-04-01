import { bytes32ToUuid, positionUuidFor, cbtAssetUuidFor } from '../helpers';

const UUID_REGEX =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

describe('bytes32ToUuid', () => {
  it('should convert a 0x-prefixed bytes32 hex to UUID format', () => {
    const hex =
      '0x550e8400e29b41d4a716446655440000aaaabbbbccccddddeeeeffffaaaabbbb';
    const result = bytes32ToUuid(hex);
    expect(result).toBe('550e8400-e29b-41d4-a716-446655440000');
    expect(result).toMatch(UUID_REGEX);
  });

  it('should convert a bytes32 hex without 0x prefix to UUID format', () => {
    const hex =
      '660e8400e29b41d4a716446655440099aaaabbbbccccddddeeeeffffaaaabbbb';
    const result = bytes32ToUuid(hex);
    expect(result).toBe('660e8400-e29b-41d4-a716-446655440099');
  });

  it('should throw for hex string shorter than 32 characters', () => {
    expect(() => bytes32ToUuid('0xabcdef')).toThrow(
      'bytes32ToUuid: insufficient hex length',
    );
  });

  it('should throw for empty hex string', () => {
    expect(() => bytes32ToUuid('')).toThrow(
      'bytes32ToUuid: insufficient hex length',
    );
  });

  it('should handle exactly 32 hex characters (no 0x prefix)', () => {
    const hex = '550e8400e29b41d4a716446655440000';
    const result = bytes32ToUuid(hex);
    expect(result).toBe('550e8400-e29b-41d4-a716-446655440000');
  });

  it('should truncate longer hex strings to first 32 characters', () => {
    const hex =
      '0x550e8400e29b41d4a716446655440000ffffffffffffffffffffffffffffffff';
    const result = bytes32ToUuid(hex);
    // Only first 32 hex chars after 0x are used
    expect(result).toBe('550e8400-e29b-41d4-a716-446655440000');
  });
});

describe('positionUuidFor', () => {
  it('should return deterministic output for the same inputs', () => {
    const marketId = '660e8400-e29b-41d4-a716-446655440099';
    const wallet = '0x1234567890123456789012345678901234567890';
    const result1 = positionUuidFor(marketId, wallet);
    const result2 = positionUuidFor(marketId, wallet);
    expect(result1).toBe(result2);
  });

  it('should return different output for different inputs', () => {
    const marketId = '660e8400-e29b-41d4-a716-446655440099';
    const wallet1 = '0x1234567890123456789012345678901234567890';
    const wallet2 = '0x0987654321098765432109876543210987654321';
    const result1 = positionUuidFor(marketId, wallet1);
    const result2 = positionUuidFor(marketId, wallet2);
    expect(result1).not.toBe(result2);
  });

  it('should return a valid UUID format', () => {
    const result = positionUuidFor('some-market', '0xwallet');
    expect(result).toMatch(UUID_REGEX);
  });

  it('should produce different results when marketId differs', () => {
    const wallet = '0x1234567890123456789012345678901234567890';
    const result1 = positionUuidFor('market-a', wallet);
    const result2 = positionUuidFor('market-b', wallet);
    expect(result1).not.toBe(result2);
  });
});

describe('cbtAssetUuidFor', () => {
  it('should return deterministic output for the same inputs', () => {
    const marketId = '660e8400-e29b-41d4-a716-446655440099';
    const bondToken = '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';
    const result1 = cbtAssetUuidFor(marketId, bondToken);
    const result2 = cbtAssetUuidFor(marketId, bondToken);
    expect(result1).toBe(result2);
  });

  it('should return different output for different bond token addresses', () => {
    const marketId = '660e8400-e29b-41d4-a716-446655440099';
    const bond1 = '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';
    const bond2 = '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb';
    const result1 = cbtAssetUuidFor(marketId, bond1);
    const result2 = cbtAssetUuidFor(marketId, bond2);
    expect(result1).not.toBe(result2);
  });

  it('should return a valid UUID format', () => {
    const result = cbtAssetUuidFor('some-market', '0xbond');
    expect(result).toMatch(UUID_REGEX);
  });
});
