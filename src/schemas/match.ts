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
 * Validator for a positive integer amount/fee field carried as a decimal
 * string (BigInt money math — never `number`). Hardening (M3):
 *   - regex `^[1-9]\d*$` rejects `"0"` and any leading-zero form (`"007"`),
 *     which the previous `^\d+$` accepted.
 *   - `.refine` rejects any value that would overflow uint256 on-chain.
 */
const positiveUint256String = (label: string) =>
  z
    .string()
    .regex(/^[1-9]\d*$/, `${label} must be a positive integer string (no zero/leading zeros)`)
    .refine((v) => BigInt(v) <= UINT256_MAX, {
      message: `${label} must not exceed uint256 max`,
    });

/**
 * Schema representing a single match produced by the Matching Engine.
 */
export const matchSchema = z.object({
  matchId: z.string().uuid('Match ID must be a valid UUID'),
  marketId: z.string().uuid('Market ID must be a valid UUID'),
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
  makerFeeAmount: positiveUint256String('Maker fee amount'),
  takerFeeAmount: positiveUint256String('Taker fee amount'),
  lenderSettlementFeeAmount: positiveUint256String('Lender settlement fee amount'),
  borrowerSettlementFeeAmount: positiveUint256String('Borrower settlement fee amount'),
});

/**
 * Redis stream keys used by the settlement engine.
 */
export const REDIS_STREAMS = {
  /**
   * Stream for settlement matches to be consumed by Settlement Engine.
   */
  SETTLEMENT_MATCHES: 'settlement:matches',
} as const;

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


