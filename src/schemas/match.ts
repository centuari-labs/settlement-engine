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
 * Schema representing a single match produced by the Matching Engine.
 */
export const matchSchema = z.object({
  matchId: z.string().uuid('Match ID must be a valid UUID'),
  lendOrderId: z.string().uuid('Lend order ID must be a valid UUID'), //@note : change into order market id in future
  borrowOrderId: z.string().uuid('Borrow order ID must be a valid UUID'), //@note : change into order market id in future
  lenderWallet: ethereumAddressSchema, //@note : change into account id in future
  borrowerWallet: ethereumAddressSchema, //@note : change into account id in future
  matchedAmount: z
    .string()
    .regex(/^\d+$/, 'Matched amount must be a positive integer string'),
  rate: z
    .number()
    .int('Rate must be an integer')
    .min(0, 'Rate must be non-negative')
    .max(10000, 'Rate must not exceed 10000 basis points (100%)'),
  loanToken: ethereumAddressSchema, //@note : change into account id in future
  maturity: z.number().int().positive('Maturity must be a positive integer'),
  timestamp: z.number().int().positive('Timestamp must be a positive integer'),
  borrowerIsTaker: z.boolean(),
  makerFeeAmount: z
    .string()
    .regex(/^\d+$/, 'Fee amount must be a positive integer string'),
  takerFeeAmount: z
    .string()
    .regex(/^\d+$/, 'Fee amount must be a positive integer string'),
  lenderSettlementFee: z
    .string()
    .regex(/^\d+$/, 'Settlement fee amount must be a positive integer string'),
  borrowerSettlementFee: z
    .string()
    .regex(/^\d+$/, 'Settlement fee amount must be a positive integer string'),
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


