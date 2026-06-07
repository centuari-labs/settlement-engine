import { matchSchema } from '../match';
import { createMatch } from '../../tests/helpers/testFixtures';

const UINT256_MAX = 2n ** 256n - 1n;

// A plain valid match object. createMatch() validates internally, so we capture
// one valid baseline and override fields directly to exercise invalid inputs
// (which the fixture's own matchSchema.parse would otherwise reject up front).
const validMatch = { ...createMatch() };
const withField = (field: string, value: string): Record<string, unknown> => ({
  ...validMatch,
  [field]: value,
});

describe('matchSchema fee fields', () => {
  const feeFields = [
    'makerFeeAmount',
    'takerFeeAmount',
    'lenderSettlementFeeAmount',
    'borrowerSettlementFeeAmount',
  ] as const;

  // A zero fee is legitimate: BigInt.toString() yields "0" when a fee is 0 bps
  // or floors to zero on a small fill. The matching engine emits it verbatim, so
  // the settlement engine MUST accept it — rejecting it dead-letters the match and
  // strands the user_balance.in_orders lock taken at match time.
  it.each(feeFields)('accepts "0" for %s', (field) => {
    const result = matchSchema.safeParse(withField(field, '0'));
    expect(result.success).toBe(true);
  });

  it('accepts "0" for all fee fields simultaneously', () => {
    const result = matchSchema.safeParse({
      ...validMatch,
      makerFeeAmount: '0',
      takerFeeAmount: '0',
      lenderSettlementFeeAmount: '0',
      borrowerSettlementFeeAmount: '0',
    });
    expect(result.success).toBe(true);
  });

  it.each(feeFields)('rejects leading-zero form "007" for %s', (field) => {
    const result = matchSchema.safeParse(withField(field, '007'));
    expect(result.success).toBe(false);
  });

  it.each(feeFields)('rejects uint256 overflow for %s', (field) => {
    const result = matchSchema.safeParse(withField(field, (UINT256_MAX + 1n).toString()));
    expect(result.success).toBe(false);
  });

  it.each(feeFields)('rejects non-numeric for %s', (field) => {
    const result = matchSchema.safeParse(withField(field, 'abc'));
    expect(result.success).toBe(false);
  });
});

describe('matchSchema matchedAmount stays strictly positive', () => {
  it('rejects "0" for matchedAmount (a zero-amount match is never valid)', () => {
    const result = matchSchema.safeParse(withField('matchedAmount', '0'));
    expect(result.success).toBe(false);
  });

  it('accepts a normal positive matchedAmount', () => {
    const result = matchSchema.safeParse(withField('matchedAmount', '1000000'));
    expect(result.success).toBe(true);
  });
});
