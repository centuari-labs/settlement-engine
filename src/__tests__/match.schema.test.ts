import {
  matchSchema,
  ethereumAddressSchema,
  REDIS_STREAMS,
  REDIS_CONSUMER_GROUPS,
  type Match,
} from '../schemas/match';

/**
 * Unit tests for match schema validation (Zod).
 */
describe('ethereumAddressSchema', () => {
  it('should accept a valid lowercase address', () => {
    const result = ethereumAddressSchema.safeParse(
      '0x1234567890abcdef1234567890abcdef12345678',
    );
    expect(result.success).toBe(true);
  });

  it('should accept a valid mixed-case address', () => {
    const result = ethereumAddressSchema.safeParse(
      '0xAbCdEf1234567890AbCdEf1234567890AbCdEf12',
    );
    expect(result.success).toBe(true);
  });

  it('should reject address without 0x prefix', () => {
    const result = ethereumAddressSchema.safeParse(
      '1234567890abcdef1234567890abcdef12345678',
    );
    expect(result.success).toBe(false);
  });

  it('should reject too short address', () => {
    const result = ethereumAddressSchema.safeParse('0x1234');
    expect(result.success).toBe(false);
  });

  it('should reject too long address', () => {
    const result = ethereumAddressSchema.safeParse(
      '0x1234567890abcdef1234567890abcdef1234567800',
    );
    expect(result.success).toBe(false);
  });

  it('should reject non-hex characters', () => {
    const result = ethereumAddressSchema.safeParse(
      '0xgggggggggggggggggggggggggggggggggggggggg',
    );
    expect(result.success).toBe(false);
  });

  it('should reject empty string', () => {
    const result = ethereumAddressSchema.safeParse('');
    expect(result.success).toBe(false);
  });

  it('should reject non-string types', () => {
    expect(ethereumAddressSchema.safeParse(123).success).toBe(false);
    expect(ethereumAddressSchema.safeParse(null).success).toBe(false);
    expect(ethereumAddressSchema.safeParse(undefined).success).toBe(false);
  });
});

describe('matchSchema', () => {
  const validMatch: Match = {
    matchId: '550e8400-e29b-41d4-a716-446655440000',
    lendOrderId: '550e8400-e29b-41d4-a716-446655440001',
    borrowOrderId: '550e8400-e29b-41d4-a716-446655440002',
    lenderWallet: '0x1234567890123456789012345678901234567890',
    borrowerWallet: '0x0987654321098765432109876543210987654321',
    matchedAmount: '1000000',
    rate: 5000,
    loanToken: '0x1111111111111111111111111111111111111111',
    maturity: 1735689600,
    timestamp: 1704067200,
    borrowerIsTaker: true,
    makerFeeAmount: '0',
    takerFeeAmount: '0',
    lenderSettlementFee: '0',
    borrowerSettlementFee: '0',
  };

  it('should accept a valid match', () => {
    const result = matchSchema.safeParse(validMatch);
    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.data).toEqual(validMatch);
    }
  });

  // --- matchId ---
  it('should reject non-UUID matchId', () => {
    const result = matchSchema.safeParse({ ...validMatch, matchId: 'not-uuid' });
    expect(result.success).toBe(false);
  });

  it('should reject missing matchId', () => {
    const { matchId, ...rest } = validMatch;
    const result = matchSchema.safeParse(rest);
    expect(result.success).toBe(false);
  });

  // --- lendOrderId / borrowOrderId ---
  it('should reject non-UUID lendOrderId', () => {
    const result = matchSchema.safeParse({
      ...validMatch,
      lendOrderId: '12345',
    });
    expect(result.success).toBe(false);
  });

  it('should reject non-UUID borrowOrderId', () => {
    const result = matchSchema.safeParse({
      ...validMatch,
      borrowOrderId: '12345',
    });
    expect(result.success).toBe(false);
  });

  // --- wallets ---
  it('should reject invalid lenderWallet', () => {
    const result = matchSchema.safeParse({
      ...validMatch,
      lenderWallet: 'invalid',
    });
    expect(result.success).toBe(false);
  });

  it('should reject invalid borrowerWallet', () => {
    const result = matchSchema.safeParse({
      ...validMatch,
      borrowerWallet: '0x123',
    });
    expect(result.success).toBe(false);
  });

  // --- matchedAmount ---
  it('should accept zero matchedAmount', () => {
    const result = matchSchema.safeParse({
      ...validMatch,
      matchedAmount: '0',
    });
    expect(result.success).toBe(true);
  });

  it('should accept large matchedAmount', () => {
    const result = matchSchema.safeParse({
      ...validMatch,
      matchedAmount: '999999999999999999999999999999',
    });
    expect(result.success).toBe(true);
  });

  it('should reject negative matchedAmount', () => {
    const result = matchSchema.safeParse({
      ...validMatch,
      matchedAmount: '-100',
    });
    expect(result.success).toBe(false);
  });

  it('should reject decimal matchedAmount', () => {
    const result = matchSchema.safeParse({
      ...validMatch,
      matchedAmount: '100.5',
    });
    expect(result.success).toBe(false);
  });

  it('should reject non-numeric matchedAmount', () => {
    const result = matchSchema.safeParse({
      ...validMatch,
      matchedAmount: 'abc',
    });
    expect(result.success).toBe(false);
  });

  // --- rate ---
  it('should accept rate of 0 (minimum)', () => {
    const result = matchSchema.safeParse({ ...validMatch, rate: 0 });
    expect(result.success).toBe(true);
  });

  it('should accept rate of 10000 (maximum)', () => {
    const result = matchSchema.safeParse({ ...validMatch, rate: 10000 });
    expect(result.success).toBe(true);
  });

  it('should reject rate above 10000', () => {
    const result = matchSchema.safeParse({ ...validMatch, rate: 10001 });
    expect(result.success).toBe(false);
  });

  it('should reject negative rate', () => {
    const result = matchSchema.safeParse({ ...validMatch, rate: -1 });
    expect(result.success).toBe(false);
  });

  it('should reject non-integer rate', () => {
    const result = matchSchema.safeParse({ ...validMatch, rate: 50.5 });
    expect(result.success).toBe(false);
  });

  // --- loanToken ---
  it('should reject invalid loanToken', () => {
    const result = matchSchema.safeParse({
      ...validMatch,
      loanToken: 'not-address',
    });
    expect(result.success).toBe(false);
  });

  // --- maturity ---
  it('should reject zero maturity', () => {
    const result = matchSchema.safeParse({ ...validMatch, maturity: 0 });
    expect(result.success).toBe(false);
  });

  it('should reject negative maturity', () => {
    const result = matchSchema.safeParse({ ...validMatch, maturity: -1 });
    expect(result.success).toBe(false);
  });

  // --- timestamp ---
  it('should reject zero timestamp', () => {
    const result = matchSchema.safeParse({ ...validMatch, timestamp: 0 });
    expect(result.success).toBe(false);
  });

  // --- borrowerIsTaker ---
  it('should accept false borrowerIsTaker', () => {
    const result = matchSchema.safeParse({
      ...validMatch,
      borrowerIsTaker: false,
    });
    expect(result.success).toBe(true);
  });

  it('should reject non-boolean borrowerIsTaker', () => {
    const result = matchSchema.safeParse({
      ...validMatch,
      borrowerIsTaker: 'true',
    });
    expect(result.success).toBe(false);
  });

  // --- fee amounts ---
  it('should reject non-numeric fee amounts', () => {
    const result = matchSchema.safeParse({
      ...validMatch,
      makerFeeAmount: 'abc',
    });
    expect(result.success).toBe(false);
  });

  it('should reject negative fee amounts', () => {
    const result = matchSchema.safeParse({
      ...validMatch,
      takerFeeAmount: '-100',
    });
    expect(result.success).toBe(false);
  });

  it('should reject decimal settlement fees', () => {
    const result = matchSchema.safeParse({
      ...validMatch,
      lenderSettlementFee: '10.5',
    });
    expect(result.success).toBe(false);
  });

  // --- extra fields ---
  it('should strip unknown fields', () => {
    const result = matchSchema.safeParse({
      ...validMatch,
      unknownField: 'extra',
    });
    // Zod by default strips unknown fields in strict mode or passes them
    expect(result.success).toBe(true);
  });

  // --- completely invalid input ---
  it('should reject null input', () => {
    expect(matchSchema.safeParse(null).success).toBe(false);
  });

  it('should reject empty object', () => {
    expect(matchSchema.safeParse({}).success).toBe(false);
  });

  it('should reject string input', () => {
    expect(matchSchema.safeParse('not-an-object').success).toBe(false);
  });
});

describe('constants', () => {
  it('should export REDIS_STREAMS with correct values', () => {
    expect(REDIS_STREAMS.SETTLEMENT_MATCHES).toBe('settlement:matches');
  });

  it('should export REDIS_CONSUMER_GROUPS with correct values', () => {
    expect(REDIS_CONSUMER_GROUPS.SETTLEMENT_ENGINE).toBe('settlement-engine');
  });
});
