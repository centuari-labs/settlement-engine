import { matchSchema, ethereumAddressSchema, REDIS_STREAMS, REDIS_CONSUMER_GROUPS, type Match } from '../match';
import { createMatch } from '../../tests/helpers/testFixtures';

/**
 * Unit tests for match schema validation.
 */
describe('ethereumAddressSchema', () => {
  it('should accept valid Ethereum address', () => {
    const result = ethereumAddressSchema.safeParse('0x1234567890123456789012345678901234567890');
    expect(result.success).toBe(true);
  });

  it('should accept lowercase hex address', () => {
    const result = ethereumAddressSchema.safeParse('0xabcdef0123456789abcdef0123456789abcdef01');
    expect(result.success).toBe(true);
  });

  it('should accept uppercase hex address', () => {
    const result = ethereumAddressSchema.safeParse('0xABCDEF0123456789ABCDEF0123456789ABCDEF01');
    expect(result.success).toBe(true);
  });

  it('should accept mixed case hex address', () => {
    const result = ethereumAddressSchema.safeParse('0xAbCdEf0123456789aBcDeF0123456789AbCdEf01');
    expect(result.success).toBe(true);
  });

  it('should reject address without 0x prefix', () => {
    const result = ethereumAddressSchema.safeParse('1234567890123456789012345678901234567890');
    expect(result.success).toBe(false);
  });

  it('should reject address that is too short', () => {
    const result = ethereumAddressSchema.safeParse('0x12345678901234567890');
    expect(result.success).toBe(false);
  });

  it('should reject address that is too long', () => {
    const result = ethereumAddressSchema.safeParse('0x123456789012345678901234567890123456789012');
    expect(result.success).toBe(false);
  });

  it('should reject address with non-hex characters', () => {
    const result = ethereumAddressSchema.safeParse('0xGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGG');
    expect(result.success).toBe(false);
  });

  it('should reject empty string', () => {
    const result = ethereumAddressSchema.safeParse('');
    expect(result.success).toBe(false);
  });

  it('should reject non-string value', () => {
    const result = ethereumAddressSchema.safeParse(123);
    expect(result.success).toBe(false);
  });

  it('should reject null', () => {
    const result = ethereumAddressSchema.safeParse(null);
    expect(result.success).toBe(false);
  });
});

describe('matchSchema', () => {
  it('should accept valid match object', () => {
    const match = createMatch();
    const result = matchSchema.safeParse(match);
    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.data).toEqual(match);
    }
  });

  describe('matchId validation', () => {
    it('should reject non-UUID matchId', () => {
      const result = matchSchema.safeParse({
        ...createMatch(),
        matchId: 'not-a-uuid',
      });
      expect(result.success).toBe(false);
    });

    it('should reject empty matchId', () => {
      const result = matchSchema.safeParse({
        ...createMatch(),
        matchId: '',
      });
      expect(result.success).toBe(false);
    });
  });

  describe('lendOrderId validation', () => {
    it('should reject non-UUID lendOrderId', () => {
      const result = matchSchema.safeParse({
        ...createMatch(),
        lendOrderId: 'invalid',
      });
      expect(result.success).toBe(false);
    });
  });

  describe('borrowOrderId validation', () => {
    it('should reject non-UUID borrowOrderId', () => {
      const result = matchSchema.safeParse({
        ...createMatch(),
        borrowOrderId: 'invalid',
      });
      expect(result.success).toBe(false);
    });
  });

  describe('wallet address validation', () => {
    it('should reject invalid lenderWallet', () => {
      const result = matchSchema.safeParse({
        ...createMatch(),
        lenderWallet: 'not-an-address',
      });
      expect(result.success).toBe(false);
    });

    it('should reject invalid borrowerWallet', () => {
      const result = matchSchema.safeParse({
        ...createMatch(),
        borrowerWallet: '0xshort',
      });
      expect(result.success).toBe(false);
    });
  });

  describe('matchedAmount validation', () => {
    it('should accept valid amount string', () => {
      const result = matchSchema.safeParse({
        ...createMatch(),
        matchedAmount: '1000000000000000000',
      });
      expect(result.success).toBe(true);
    });

    it('should accept zero amount', () => {
      const result = matchSchema.safeParse({
        ...createMatch(),
        matchedAmount: '0',
      });
      expect(result.success).toBe(true);
    });

    it('should reject negative amount', () => {
      const result = matchSchema.safeParse({
        ...createMatch(),
        matchedAmount: '-100',
      });
      expect(result.success).toBe(false);
    });

    it('should reject non-numeric string', () => {
      const result = matchSchema.safeParse({
        ...createMatch(),
        matchedAmount: 'abc',
      });
      expect(result.success).toBe(false);
    });

    it('should reject decimal amount', () => {
      const result = matchSchema.safeParse({
        ...createMatch(),
        matchedAmount: '100.5',
      });
      expect(result.success).toBe(false);
    });
  });

  describe('rate validation', () => {
    it('should accept 0 rate', () => {
      const result = matchSchema.safeParse({
        ...createMatch(),
        rate: 0,
      });
      expect(result.success).toBe(true);
    });

    it('should accept 10000 rate (100% in basis points)', () => {
      const result = matchSchema.safeParse({
        ...createMatch(),
        rate: 10000,
      });
      expect(result.success).toBe(true);
    });

    it('should reject rate above 10000', () => {
      const result = matchSchema.safeParse({
        ...createMatch(),
        rate: 10001,
      });
      expect(result.success).toBe(false);
    });

    it('should reject negative rate', () => {
      const result = matchSchema.safeParse({
        ...createMatch(),
        rate: -1,
      });
      expect(result.success).toBe(false);
    });

    it('should reject non-integer rate', () => {
      const result = matchSchema.safeParse({
        ...createMatch(),
        rate: 50.5,
      });
      expect(result.success).toBe(false);
    });
  });

  describe('loanToken validation', () => {
    it('should reject invalid loanToken address', () => {
      const result = matchSchema.safeParse({
        ...createMatch(),
        loanToken: 'not-an-address',
      });
      expect(result.success).toBe(false);
    });
  });

  describe('maturity validation', () => {
    it('should accept positive maturity', () => {
      const result = matchSchema.safeParse({
        ...createMatch(),
        maturity: 1735689600,
      });
      expect(result.success).toBe(true);
    });

    it('should reject zero maturity', () => {
      const result = matchSchema.safeParse({
        ...createMatch(),
        maturity: 0,
      });
      expect(result.success).toBe(false);
    });

    it('should reject negative maturity', () => {
      const result = matchSchema.safeParse({
        ...createMatch(),
        maturity: -100,
      });
      expect(result.success).toBe(false);
    });
  });

  describe('timestamp validation', () => {
    it('should accept positive timestamp', () => {
      const result = matchSchema.safeParse({
        ...createMatch(),
        timestamp: 1704067200,
      });
      expect(result.success).toBe(true);
    });

    it('should reject zero timestamp', () => {
      const result = matchSchema.safeParse({
        ...createMatch(),
        timestamp: 0,
      });
      expect(result.success).toBe(false);
    });
  });

  describe('borrowerIsTaker validation', () => {
    it('should accept true', () => {
      const result = matchSchema.safeParse({
        ...createMatch(),
        borrowerIsTaker: true,
      });
      expect(result.success).toBe(true);
    });

    it('should accept false', () => {
      const result = matchSchema.safeParse({
        ...createMatch(),
        borrowerIsTaker: false,
      });
      expect(result.success).toBe(true);
    });

    it('should reject non-boolean', () => {
      const result = matchSchema.safeParse({
        ...createMatch(),
        borrowerIsTaker: 'true',
      });
      expect(result.success).toBe(false);
    });
  });

  describe('fee amount validation', () => {
    it('should accept zero fee amounts', () => {
      const result = matchSchema.safeParse({
        ...createMatch(),
        makerFeeAmount: '0',
        takerFeeAmount: '0',
        lenderSettlementFee: '0',
        borrowerSettlementFee: '0',
      });
      expect(result.success).toBe(true);
    });

    it('should accept positive fee amounts', () => {
      const result = matchSchema.safeParse({
        ...createMatch(),
        makerFeeAmount: '1000',
        takerFeeAmount: '2000',
        lenderSettlementFee: '500',
        borrowerSettlementFee: '500',
      });
      expect(result.success).toBe(true);
    });

    it('should reject non-numeric makerFeeAmount', () => {
      const result = matchSchema.safeParse({
        ...createMatch(),
        makerFeeAmount: 'abc',
      });
      expect(result.success).toBe(false);
    });

    it('should reject negative takerFeeAmount', () => {
      const result = matchSchema.safeParse({
        ...createMatch(),
        takerFeeAmount: '-100',
      });
      expect(result.success).toBe(false);
    });
  });

  describe('missing fields', () => {
    it('should reject when matchId is missing', () => {
      const { matchId, ...rest } = createMatch();
      const result = matchSchema.safeParse(rest);
      expect(result.success).toBe(false);
    });

    it('should reject when lenderWallet is missing', () => {
      const { lenderWallet, ...rest } = createMatch();
      const result = matchSchema.safeParse(rest);
      expect(result.success).toBe(false);
    });

    it('should reject empty object', () => {
      const result = matchSchema.safeParse({});
      expect(result.success).toBe(false);
    });

    it('should reject null', () => {
      const result = matchSchema.safeParse(null);
      expect(result.success).toBe(false);
    });

    it('should reject undefined', () => {
      const result = matchSchema.safeParse(undefined);
      expect(result.success).toBe(false);
    });
  });
});

describe('REDIS_STREAMS', () => {
  it('should have SETTLEMENT_MATCHES key', () => {
    expect(REDIS_STREAMS.SETTLEMENT_MATCHES).toBe('settlement:matches');
  });
});

describe('REDIS_CONSUMER_GROUPS', () => {
  it('should have SETTLEMENT_ENGINE key', () => {
    expect(REDIS_CONSUMER_GROUPS.SETTLEMENT_ENGINE).toBe('settlement-engine');
  });
});
