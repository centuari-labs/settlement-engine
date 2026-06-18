import pino from 'pino';

/**
 * Structured logger for the settlement engine.
 *
 * Security hardening (M2): Viem errors carry the RPC `url` (which embeds the
 * provider API key) and request calldata. Logging `{ err: error }` with a raw
 * Viem/Error object would leak the key and tx args into logs. Two guards:
 *
 *  1. `serializers.err` — Pino's standard error serializer reduces any object
 *     passed on the `err` key to `{ type, message, stack }`, so nested fields
 *     like `url`, `metaMessages`, or request bodies never get serialized.
 *  2. `redact` — defence-in-depth: censor any `url` / `rpcUrl` field at common
 *     paths regardless of which log key it arrives under, so a stray RPC URL
 *     in a structured payload is scrubbed everywhere at once.
 */
export const logger = pino({
  name: 'settlement-engine',
  serializers: {
    err: pino.stdSerializers.err,
    error: pino.stdSerializers.err,
  },
  redact: {
    paths: [
      'url',
      'rpcUrl',
      'rpcUrls',
      'err.url',
      'error.url',
      '*.url',
      '*.rpcUrl',
    ],
    censor: '[REDACTED]',
  },
});
