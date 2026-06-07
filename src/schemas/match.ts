import { z } from 'zod';

/**
 * Zod schema for validating Ethereum addresses.
 * This is a minimal checksum-agnostic validator; you can replace it with
 * a shared schema from elsewhere in your codebase later if needed.
 */
export const ethereumAddressSchema = z
  .string()
  .regex(/^0x[a-fA-F0-9]{40}$/, 'Must be a valid Ethereum address');

/**
 * Maximum value an on-chain uint256 amount/fee field can hold. Any digit
 * string above this would overflow the contract's uint256 args at settlement.
 */
const UINT256_MAX = 2n ** 256n - 1n;

/**
 * Throw-safe uint256 upper-bound check. `BigInt("abc")` throws, and a throw
 * inside a `.refine` propagates out of `safeParse` instead of yielding a clean
 * validation failure — which would crash the consumer rather than dead-letter a
 * malformed entry. Guarded so any non-numeric input simply fails validation.
 */
const withinUint256 = (v: string): boolean => {
  try {
    return BigInt(v) <= UINT256_MAX;
  } catch {
    return false;
  }
};

/**
 * Validator for a strictly-positive integer amount field carried as a decimal
 * string (BigInt money math — never `number`). Hardening (M3):
 *   - regex `^[1-9]\d*$` rejects `"0"` and any leading-zero form (`"007"`),
 *     which the previous `^\d+$` accepted.
 *   - bound-check rejects any value that would overflow uint256 on-chain.
 * Use for `matchedAmount`, where zero is never a valid match.
 */
const positiveUint256String = (label: string) =>
  z
    .string()
    .regex(/^[1-9]\d*$/, `${label} must be a positive integer string (no zero/leading zeros)`)
    .refine(withinUint256, { message: `${label} must not exceed uint256 max` });

/**
 * Validator for a NON-negative integer fee field carried as a decimal string.
 * Unlike `matchedAmount`, a zero fee is legitimate: `BigInt.toString()` yields
 * `"0"` for a 0-bps fee or one that floors to zero on a small fill, and the
 * matching engine emits it verbatim. Rejecting `"0"` here would dead-letter the
 * match and permanently strand the `user_balance.in_orders` lock taken at match
 * time (the C2 sweeper only recovers DB-`PENDING` rows, not dead-lettered ones).
 *   - regex `^(0|[1-9]\d*)$` accepts `"0"` but still rejects leading-zero forms.
 *   - bound-check rejects uint256 overflow.
 */
const nonNegativeUint256String = (label: string) =>
  z
    .string()
    .regex(/^(0|[1-9]\d*)$/, `${label} must be a non-negative integer string (no leading zeros)`)
    .refine(withinUint256, { message: `${label} must not exceed uint256 max` });

/**
 * Schema representing a single match produced by the Matching Engine.
 */
export const matchSchema = z.object({
  matchId: z.string().uuid('Match ID must be a valid UUID'),
  // marketId is the on-chain bytes32 market key (C4 UUID→BYTEA migration): the
  // matching engine + backend identify markets by the bytes32 the contract uses
  // as the storage key (NOT a UUID). settleMatch/repay/withdraw all key on it
  // verbatim, so it must be carried through unchanged — see encodeMatchData.
  marketId: z
    .string()
    .regex(/^0x[a-fA-F0-9]{64}$/, 'Market ID must be a 32-byte hex string (0x + 64 hex)'),
  lendOrderId: z.string().uuid('Lend order ID must be a valid UUID'),
  borrowOrderId: z.string().uuid('Borrow order ID must be a valid UUID'),
  lenderWallet: ethereumAddressSchema,
  borrowerWallet: ethereumAddressSchema,
  matchedAmount: positiveUint256String('Matched amount'),
  rate: z
    .number()
    .int('Rate must be an integer')
    .min(0, 'Rate must be non-negative')
    .max(10000, 'Rate must not exceed 10000 basis points (100%)'),
  loanToken: ethereumAddressSchema,
  // `maturity` stays numeric (unix seconds). Defense-in-depth (M4): reject a
  // maturity that is not strictly in the future at ingest — a non-future
  // maturity is never a valid settleable market and indicates a malformed or
  // stale match.
  maturity: z
    .number()
    .int()
    .positive('Maturity must be a positive integer')
    .refine((m) => m > Math.floor(Date.now() / 1000), {
      message: 'Maturity must be a future unix-seconds timestamp',
    }),
  timestamp: z.number().int().positive('Timestamp must be a positive integer'),
  borrowerIsTaker: z.boolean(),
  makerFeeAmount: nonNegativeUint256String('Maker fee amount'),
  takerFeeAmount: nonNegativeUint256String('Taker fee amount'),
  lenderSettlementFeeAmount: nonNegativeUint256String('Lender settlement fee amount'),
  borrowerSettlementFeeAmount: nonNegativeUint256String('Borrower settlement fee amount'),
});

/**
 * Redis stream keys used by the settlement engine.
 */
export const REDIS_STREAMS = {
  /**
   * Stream for settlement matches to be consumed by Settlement Engine.
   */
  SETTLEMENT_MATCHES: 'settlement:matches',
  /**
   * Dead-letter stream for match entries that fail schema validation.
   * Invalid entries are XADDed here (with the raw payload + error) before
   * being ACKed off the live stream, so they are inspectable instead of
   * silently lost (L5).
   */
  SETTLEMENT_MATCHES_DEAD: 'settlement:matches:dead',
} as const;

/**
 * MAXLEN bound (approximate, `~`) for the dead-letter stream so it cannot
 * grow without limit.
 */
export const SETTLEMENT_MATCHES_DEAD_MAXLEN = 10000;

/**
 * Redis consumer group configuration.
 */
export const REDIS_CONSUMER_GROUPS = {
  /**
   * Consumer group name for Settlement Engine.
   */
  SETTLEMENT_ENGINE: 'settlement-engine',
} as const;

export type Match = z.infer<typeof matchSchema>;


