import type { Match } from '../../schemas/match';
import type { MatchWithMeta } from '../../redis/settlementMatchConsumer';
import { matchSchema } from '../../schemas/match';

/**
 * Creates a valid match object with default values that can be overridden.
 *
 * @param overrides - Optional fields to override in the match object.
 * @returns A valid Match object.
 */
export const createMatch = (overrides?: Partial<Match>): Match => {
  const defaults: Match = {
    matchId: '550e8400-e29b-41d4-a716-446655440000',
    // bytes32 on-chain market key (C4): the matching engine identifies markets by
    // the bytes32 the contract uses as its storage key, not a UUID.
    marketId:
      '0x660e8400e29b41d4a716446655440099000000000000000000000000000000aa',
    lendOrderId: '550e8400-e29b-41d4-a716-446655440001',
    borrowOrderId: '550e8400-e29b-41d4-a716-446655440002',
    lenderWallet: '0x1234567890123456789012345678901234567890',
    borrowerWallet: '0x0987654321098765432109876543210987654321',
    matchedAmount: '1000000',
    rate: 5000,
    loanToken: '0x1111111111111111111111111111111111111111',
    // Far-future maturity (2099-01-01) — schema now requires a future
    // unix-seconds maturity (M4), so a fixed future constant keeps fixtures
    // deterministic without ever drifting into the past.
    maturity: 4070908800,
    timestamp: 1704067200,
    borrowerIsTaker: true,
    // Fee/amount digit fields must now be positive integers (no '0') per the
    // tightened schema (M3). Use a representative non-zero default.
    makerFeeAmount: '1000',
    takerFeeAmount: '1000',
    lenderSettlementFeeAmount: '1000',
    borrowerSettlementFeeAmount: '1000',
  };

  const match = { ...defaults, ...overrides };
  // Validate the match to ensure it's correct
  return matchSchema.parse(match);
};

/**
 * Creates a MatchWithMeta object with default values.
 * Each call generates a unique stream entry id unless overridden.
 *
 * @param matchOverrides - Optional fields to override in the match payload.
 * @param metaOverrides - Optional fields to override in the meta information.
 * @returns A MatchWithMeta object.
 */
export const createMatchWithMeta = (
  matchOverrides?: Partial<Match>,
  metaOverrides?: Partial<Omit<MatchWithMeta, 'payload'>>,
): MatchWithMeta => {
  const match = createMatch(matchOverrides);
  const uniqueId = `${Date.now()}-${Math.random().toString(36).slice(2, 9)}`;
  const defaults: Omit<MatchWithMeta, 'payload'> = {
    id: uniqueId,
    stream: 'settlement:matches',
  };

  return {
    ...defaults,
    ...metaOverrides,
    payload: match,
  };
};

/**
 * Creates a Redis stream entry format from a match.
 * This simulates how a match would be stored in a Redis stream.
 *
 * @param match - The match object to convert.
 * @param entryId - The Redis stream entry ID (default: '12345-0').
 * @param format - Whether to format as JSON in 'data' field or as individual fields.
 * @returns A Redis stream entry in the format [id, [field1, value1, field2, value2, ...]].
 */
export const createRedisStreamEntry = (
  match: Match,
  entryId = '12345-0',
  format: 'json' | 'fields' = 'json',
): [string, string[]] => {
  if (format === 'json') {
    return [entryId, ['data', JSON.stringify(match)]];
  }

  // Format as individual fields
  const fields: string[] = [];
  for (const [key, value] of Object.entries(match)) {
    fields.push(key, typeof value === 'string' ? value : String(value));
  }

  return [entryId, fields];
};

/**
 * Creates multiple match objects in a batch.
 *
 * @param count - Number of matches to create.
 * @param baseOverrides - Base overrides to apply to all matches.
 * @returns An array of Match objects.
 */
export const createMatchBatch = (
  count: number,
  baseOverrides?: Partial<Match>,
): Match[] => {
  return Array.from({ length: count }, (_, index) =>
    createMatch({
      ...baseOverrides,
      // Generate valid UUID by replacing last 12 hex digits with zero-padded index
      // UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx (36 chars total)
      // Base: 550e8400-e29b-41d4-a716-446655440000
      // Replace last segment (12 hex digits) with padded index (max 4 digits = 9999)
      matchId: `550e8400-e29b-41d4-a716-${String(index).padStart(12, '0')}`,
    }),
  );
};

