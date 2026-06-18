import { parseMatchEntry } from '../settlementMatchConsumer';
import { createMatch } from '../../tests/helpers/testFixtures';

const matchToFields = (match: ReturnType<typeof createMatch>): string[] => [
  'matchId',
  match.matchId,
  'marketId',
  match.marketId,
  'lendOrderId',
  match.lendOrderId,
  'borrowOrderId',
  match.borrowOrderId,
  'lenderWallet',
  match.lenderWallet,
  'borrowerWallet',
  match.borrowerWallet,
  'matchedAmount',
  match.matchedAmount,
  'rate',
  String(match.rate),
  'loanToken',
  match.loanToken,
  'maturity',
  String(match.maturity),
  'timestamp',
  String(match.timestamp),
  'borrowerIsTaker',
  String(match.borrowerIsTaker),
  'makerFeeAmount',
  String(match.makerFeeAmount),
  'takerFeeAmount',
  String(match.takerFeeAmount),
  'lenderSettlementFeeAmount',
  String(match.lenderSettlementFeeAmount),
  'borrowerSettlementFeeAmount',
  String(match.borrowerSettlementFeeAmount),
];

describe('parseMatchEntry', () => {
  it('should parse JSON data payloads', () => {
    const match = createMatch();

    const parsed = parseMatchEntry(['1-0', ['data', JSON.stringify(match)]]);

    expect(parsed).toEqual({ id: '1-0', value: match });
  });

  it('coerces string-typed numeric/boolean fields in a JSON data payload (matching-engine wire format)', () => {
    const match = createMatch();
    // The matching engine serialises rate/maturity/timestamp/borrowerIsTaker as
    // STRINGS inside the JSON payload. Without coercion on the JSON path these
    // fail z.number()/z.boolean() and the match is dead-lettered (regression).
    const wire = {
      ...match,
      rate: String(match.rate),
      maturity: String(match.maturity),
      timestamp: String(match.timestamp),
      borrowerIsTaker: String(match.borrowerIsTaker),
    };

    const parsed = parseMatchEntry(['1-0', ['data', JSON.stringify(wire)]]);

    expect(parsed).toEqual({ id: '1-0', value: match });
  });

  it('should ignore dangerous field names in flat field payloads', () => {
    const match = createMatch();

    const parsed = parseMatchEntry([
      '1-0',
      ['__proto__', '{"polluted":true}', ...matchToFields(match)],
    ]);

    expect(parsed).toEqual({ id: '1-0', value: match });
    expect(({} as Record<string, unknown>).polluted).toBeUndefined();
  });
});
