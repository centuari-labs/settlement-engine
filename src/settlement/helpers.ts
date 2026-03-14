import crypto from 'crypto';

/**
 * Converts a bytes32 hex string (e.g. from blockchain events) to a valid UUID format.
 * Takes the first 32 hex chars and formats as xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx.
 */
export function bytes32ToUuid(hex: string): string {
  const clean = hex.startsWith('0x') ? hex.slice(2) : hex;
  if (clean.length < 32) {
    throw new Error(`bytes32ToUuid: insufficient hex length: ${hex}`);
  }
  const h = clean.slice(0, 32);
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
}

function hashToUuidBase(base: string): string {
  const hash = crypto.createHash('sha1').update(base).digest('hex').slice(0, 32);
  return `${hash.slice(0, 8)}-${hash.slice(8, 12)}-${hash.slice(12, 16)}-${hash.slice(16, 20)}-${hash.slice(20)}`;
}

export function positionUuidFor(marketId: string, walletLower: string): string {
  return hashToUuidBase(`${marketId}-${walletLower}`);
}

export function cbtAssetUuidFor(
  marketId: string,
  bondTokenAddress: string,
): string {
  return hashToUuidBase(`${marketId}-${bondTokenAddress}`);
}
